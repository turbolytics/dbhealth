// dbhealth watches databases and reports whether they are serving, how close
// they are to their limits, and whether their tables are current.
//
//	dbhealth validate -c dbhealth.yml
//	dbhealth run -c dbhealth.yml
//	dbhealth version
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.uber.org/zap"

	"github.com/turbolytics/dbhealth/internal/collector"
	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/postgres"
	"github.com/turbolytics/dbhealth/internal/report"
)

const usage = `usage:
  dbhealth validate -c <file>   check the file and say how many databases it names
  dbhealth run      -c <file>   watch them and report
  dbhealth run                  watch DBHEALTH_DSN and report with DBHEALTH_KEY
  dbhealth version              print the version and exit
`

// finalTimeout bounds the last bundle on shutdown.
const finalTimeout = 10 * time.Second

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
	if args[0] == "version" {
		fmt.Println("dbhealth " + report.Version)
		return 0
	}
	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	path := fs.String("c", "", "the config file")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	// No file is the environment: one database from DBHEALTH_DSN.
	load := config.FromEnv
	if *path != "" {
		load = func() (*config.File, error) { return config.Load(*path) }
	}
	f, err := load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "dbhealth: %v\n", err)
		return 1
	}
	switch args[0] {
	case "validate":
		fmt.Printf("ok: %d databases\n", len(f.Databases))
		return 0
	case "run":
		return serve(f, *path)
	default:
		fmt.Fprint(os.Stderr, usage)
		return 2
	}
}

// serve runs the reporter until SIGTERM or SIGINT, then sends a final
// bundle per database saying so.
func serve(f *config.File, path string) int {
	log, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintln(os.Stderr, "dbhealth:", err)
		return 1
	}
	defer log.Sync() //nolint:errcheck

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	timeout := time.Duration(f.Probe.TimeoutSeconds) * time.Second
	var instances []report.Instance
	for _, db := range f.Databases {
		// Open does not connect; an endpoint that is down opens fine and
		// the probe says refused.
		client, err := postgres.Open(ctx, db.DSN, timeout)
		if err != nil {
			log.Error("database did not open", zap.String("instance", db.Name), zap.Error(err))
			return 1
		}
		defer client.Close()
		c := collector.New(db, f.Tables, f.Probe, client, time.Now)
		instances = append(instances, report.Instance{Name: db.Name, Cluster: db.Cluster, Collect: c.Collect})
		log.Info("watching", zap.String("instance", db.Name), zap.String("target", c.Target()))
	}

	r, err := report.New(report.Config{
		To: f.Report.To, Credential: f.Report.Credential, StatsD: f.Report.StatsD,
		Interval:   time.Duration(f.Probe.IntervalSeconds) * time.Second,
		ConfigHash: configHash(path), Instances: instances, Log: log,
	})
	if err != nil {
		log.Error("reporter did not start", zap.Error(err))
		return 1
	}
	defer r.Close()
	log.Info("dbhealth started", zap.String("report_to", f.Report.To), zap.String("statsd", f.Report.StatsD),
		zap.Int("interval_seconds", f.Probe.IntervalSeconds), zap.String("version", report.Version))

	r.Run(ctx)

	// The run context is done; the final bundle gets its own budget.
	fctx, cancel := context.WithTimeout(context.Background(), finalTimeout)
	defer cancel()
	r.Final(fctx, wire.Exit{Reason: "signal", Code: 0})
	log.Info("dbhealth stopped")
	return 0
}

// configHash is sha256 of the file as written, so control can tell one
// config from the next. The DSN is read from the environment, not the
// file, so the hash does not change with it; a DSN written into the file
// hashes like any other byte, and the hash does not reveal it.
func configHash(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}
