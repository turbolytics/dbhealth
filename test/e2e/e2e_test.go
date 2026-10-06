// End to end: the built binary, a real Postgres, a real control. Skipped
// unless DBHEALTH_E2E=1. It needs:
//
//	DBHEALTH_E2E_DSN         the Postgres to watch, with public.usage_per_minute (the usage-metering stack: make up there)
//	DBHEALTH_E2E_CONTROL     control's origin, http://127.0.0.1:8090 (sql-flow-control, serve)
//	DBHEALTH_E2E_CONTROL_DB  control's database, to read what it stored
//	DBHEALTH_E2E_CREDENTIAL  an sfc_ credential control knows (sqlflow-control credential create)
//	DBHEALTH_E2E_CONTAINER   the Postgres container, stopped and started to prove the probe (optional)
//
// Every bundle the test reads back is one control accepted and verified.
package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

const interval = 5 * time.Second

type env struct {
	dsn, control, controlDB, credential, container string
}

func setup(t *testing.T) (env, string) {
	t.Helper()
	if os.Getenv("DBHEALTH_E2E") != "1" {
		t.Skip("DBHEALTH_E2E=1 runs this against a real Postgres and control")
	}
	e := env{
		dsn: os.Getenv("DBHEALTH_E2E_DSN"), control: os.Getenv("DBHEALTH_E2E_CONTROL"),
		controlDB: os.Getenv("DBHEALTH_E2E_CONTROL_DB"), credential: os.Getenv("DBHEALTH_E2E_CREDENTIAL"),
		container: os.Getenv("DBHEALTH_E2E_CONTAINER"),
	}
	for name, v := range map[string]string{"DSN": e.dsn, "CONTROL": e.control, "CONTROL_DB": e.controlDB, "CREDENTIAL": e.credential} {
		if v == "" {
			t.Fatalf("DBHEALTH_E2E_%s is not set", name)
		}
	}
	bin := filepath.Join(t.TempDir(), "dbhealth")
	build := exec.Command("go", "build", "-o", bin, "../../cmd/dbhealth")
	build.Stderr = os.Stderr
	assert.NoError(t, build.Run())
	return e, bin
}

// run starts the binary on a config and returns a stop that sends SIGTERM
// and returns the log.
func run(t *testing.T, e env, bin, name, tables string) (stop func() string) {
	t.Helper()
	cfg := filepath.Join(t.TempDir(), "dbhealth.yml")
	body := fmt.Sprintf(`databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_E2E_DSN }}"
    name: %s
probe:
  interval_seconds: %d
  timeout_seconds: 3
tables:
%s
  freshness_interval_seconds: %d
  rows_exact_interval_seconds: %d
report:
  to: %s
  credential: "{{ DBHEALTH_E2E_CREDENTIAL }}"
`, name, int(interval.Seconds()), tables, int(interval.Seconds()), int(interval.Seconds()), e.control)
	assert.NoError(t, os.WriteFile(cfg, []byte(body), 0o600))
	cmd := exec.Command(bin, "run", "-c", cfg)
	cmd.Env = append(os.Environ(), "DBHEALTH_E2E_DSN="+e.dsn, "DBHEALTH_E2E_CREDENTIAL="+e.credential)
	var log strings.Builder
	cmd.Stdout, cmd.Stderr = &log, &log
	assert.NoError(t, cmd.Start())
	return func() string {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
		return log.String()
	}
}

// stored is every bundle control stored for the instance, oldest first.
func stored(t *testing.T, e env, name string) []wire.Bundle {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, e.controlDB)
	assert.NoError(t, err)
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT convert_from(r.body, 'UTF8') FROM raw_reports r
		JOIN instances i ON i.id = r.instance WHERE i.instance_id = $1 AND r.verifiable ORDER BY r.sent_at`, "dbhealth-"+name)
	assert.NoError(t, err)
	defer rows.Close()
	var out []wire.Bundle
	for rows.Next() {
		var raw string
		assert.NoError(t, rows.Scan(&raw))
		var b wire.Bundle
		assert.NoError(t, json.Unmarshal([]byte(raw), &b))
		out = append(out, b)
	}
	return out
}

// await polls control until want is true of the stored bundles, within
// two intervals and a margin.
func await(t *testing.T, e env, name string, what string, want func([]wire.Bundle) bool) []wire.Bundle {
	t.Helper()
	deadline := time.Now().Add(2*interval + 10*time.Second)
	for time.Now().Before(deadline) {
		if b := stored(t, e, name); want(b) {
			return b
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("within two intervals, control never stored %s", what)
	return nil
}

func latest(b []wire.Bundle) *wire.Database {
	if len(b) == 0 {
		return nil
	}
	return b[len(b)-1].Database
}

func watched(ctx context.Context, t *testing.T, dsn string) (tables []string, count int64, newest time.Time) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	assert.NoError(t, err)
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE 'sqlflow\_%' ORDER BY 1`)
	assert.NoError(t, err)
	for rows.Next() {
		var n string
		assert.NoError(t, rows.Scan(&n))
		tables = append(tables, "public."+n)
	}
	rows.Close()
	assert.NoError(t, conn.QueryRow(ctx, `SELECT count(*), max(minute) FROM public.usage_per_minute`).Scan(&count, &newest))
	return tables, count, newest
}

func TestE2E_DiscoveredTablesMatchTheCatalog(t *testing.T) {
	e, bin := setup(t)
	name := fmt.Sprintf("e2e-discover-%d", time.Now().Unix())
	stop := run(t, e, bin, name, "  discover:\n    schemas: [public]\n    exclude: [\"sqlflow_*\"]\n    freshness_columns: [minute]\n  rows: estimate")
	defer stop()

	want, count, newest := watched(context.Background(), t, e.dsn)
	bundles := await(t, e, name, "a bundle with tables", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && len(d.Tables) > 0
	})
	d := latest(bundles)
	var got []string
	var upm *wire.DatabaseTable
	for i := range d.Tables {
		got = append(got, d.Tables[i].Name)
		if d.Tables[i].Name == "public.usage_per_minute" {
			upm = &d.Tables[i]
		}
	}
	assert.DeepEqual(t, want, got)
	assert.NotNil(t, upm)
	assert.False(t, upm.RowsExact)
	assert.NotNil(t, upm.Rows)
	assert.That(t, float64(*upm.Rows) > 0.9*float64(count) && float64(*upm.Rows) < 1.1*float64(count))
	assert.Equal(t, "minute", upm.FreshnessColumn)
	assert.NotNil(t, upm.NewestAt)
	assert.True(t, upm.NewestAt.Equal(newest))
	assert.True(t, d.Probe.OK)
	assert.That(t, d.Target != "")
	assert.That(t, d.Collection.Queries > 0)
}

func TestE2E_AnExactCountIsExact(t *testing.T) {
	e, bin := setup(t)
	name := fmt.Sprintf("e2e-exact-%d", time.Now().Unix())
	stop := run(t, e, bin, name, "  static:\n    - name: public.usage_per_minute\n      freshness_column: minute\n      rows: exact")
	defer stop()
	_, count, _ := watched(context.Background(), t, e.dsn)
	bundles := await(t, e, name, "an exact count", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && len(d.Tables) == 1 && d.Tables[0].RowsExact
	})
	assert.Equal(t, count, *latest(bundles).Tables[0].Rows)
}

func TestE2E_AStoppedPostgresIsAProbeAlone(t *testing.T) {
	e, bin := setup(t)
	if e.container == "" {
		t.Skip("DBHEALTH_E2E_CONTAINER names the Postgres container to stop")
	}
	name := fmt.Sprintf("e2e-stop-%d", time.Now().Unix())
	stop := run(t, e, bin, name, "  discover:\n    schemas: [public]\n  rows: estimate")
	defer stop()
	await(t, e, name, "a serving probe", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && d.Probe.OK && d.Resources != nil
	})

	assert.NoError(t, exec.Command("docker", "stop", e.container).Run())
	defer func() { _ = exec.Command("docker", "start", e.container).Run() }()
	bundles := await(t, e, name, "a failed probe", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && !d.Probe.OK
	})
	d := latest(bundles)
	assert.Nil(t, d.Resources)
	assert.Nil(t, d.Tables)
	assert.Nil(t, d.Replication)
	assert.That(t, d.Probe.Error == "refused" || d.Probe.Error == "timeout")
	assert.NotNil(t, d.Probe.LastOKAt) // carried from before the stop

	assert.NoError(t, exec.Command("docker", "start", e.container).Run())
	bundles = await(t, e, name, "a probe that serves again", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && d.Probe.OK && d.Probe.ConsecutiveFailures == 0
	})
	assert.NotNil(t, latest(bundles).Resources)
}

func TestE2E_TheDSNIsNowhere(t *testing.T) {
	e, bin := setup(t)
	name := fmt.Sprintf("e2e-dsn-%d", time.Now().Unix())
	stop := run(t, e, bin, name, "  discover:\n    schemas: [public]\n  rows: estimate")
	await(t, e, name, "a bundle", func(b []wire.Bundle) bool { return len(b) > 0 })
	log := stop()
	// The password, and whether it is also the user or database name,
	// which the redacted target carries on purpose (the metering stack's
	// is metering:metering@.../metering). Then only the credential form,
	// :password@, is proof of a leak.
	password, bare := "", false
	if i := strings.Index(e.dsn, "://"); i >= 0 {
		if j := strings.Index(e.dsn[i+3:], "@"); j >= 0 {
			if k := strings.Index(e.dsn[i+3:i+3+j], ":"); k >= 0 {
				user := e.dsn[i+3 : i+3+k]
				password = e.dsn[i+3+k+1 : i+3+j]
				bare = password != user && !strings.Contains(e.dsn[i+3+j:], password)
			}
		}
	}
	leaks := func(s string) bool {
		if strings.Contains(s, e.dsn) || strings.Contains(s, e.credential) {
			return true
		}
		if password == "" {
			return false
		}
		if bare {
			return strings.Contains(s, password)
		}
		return strings.Contains(s, ":"+password+"@")
	}
	assert.False(t, leaks(log))
	for _, b := range stored(t, e, name) {
		raw, _ := json.Marshal(b)
		assert.False(t, leaks(string(raw)))
	}
}

// Under the metering stack's workers, the load facts move: transactions
// and rows read per second above zero from the second interval, the cache
// ratio in (0, 1], a session active (ours at least).
func TestE2E_LoadUnderTheWorkers(t *testing.T) {
	e, bin := setup(t)
	name := fmt.Sprintf("e2e-load-%d", time.Now().Unix())
	stop := run(t, e, bin, name, "  discover:\n    schemas: [public]\n    exclude: [\"sqlflow_*\"]\n  rows: estimate")
	defer stop()
	bundles := await(t, e, name, "a bundle with rates", func(b []wire.Bundle) bool {
		d := latest(b)
		return d != nil && d.Load != nil && d.Load.TransactionsPerSecond != nil
	})
	l := latest(bundles).Load
	assert.That(t, *l.TransactionsPerSecond > 0)
	assert.NotNil(t, l.RowsReadPerSecond)
	assert.That(t, *l.RowsReadPerSecond > 0)
	assert.NotNil(t, l.CacheHitRatio)
	assert.That(t, *l.CacheHitRatio > 0 && *l.CacheHitRatio <= 1)
	assert.NotNil(t, l.SessionsActiveNow)
	assert.That(t, *l.SessionsActiveNow >= 1)
	assert.Nil(t, l.QueriesPerSecond) // Postgres, no statement store read
	// The first bundle had the sample and no rates.
	first := bundles[0].Database.Load
	assert.NotNil(t, first)
	assert.NotNil(t, first.SessionsActiveNow)
	assert.Nil(t, first.TransactionsPerSecond)
	// Nothing in any bundle is query text.
	for _, b := range bundles {
		assert.Equal(t, 0, len(b.Database.Queries))
	}
}
