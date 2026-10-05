// Package report sends what the collectors found: one signed TurboStats
// bundle per database per interval to control, and the same facts as
// StatsD gauges when asked. The body never carries a DSN; the collector's
// section has only the redacted target, and nothing here adds to it.
package report

import (
	"bytes"
	"cmp"
	"context"
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.uber.org/zap"
)

// Version is the build's version, set by the Makefile with -ldflags.
var Version = "dev"

// Commit is the build's commit, set the same way.
var Commit = ""

// Endpoint is control's path for bundles.
const Endpoint = "/v1/turbostats"

// defaultPostTimeout bounds one POST, after the collection: the two never
// share a budget, so a slow table query cannot cost the bundle.
const defaultPostTimeout = 10 * time.Second

// jitterFraction spreads a fleet's posts, so a thousand instances started
// together do not report in the same second of every minute.
const jitterFraction = 0.1

// Instance is one database the reporter sends for.
type Instance struct {
	// Name is the endpoint's name; the id is dbhealth-<name>.
	Name string
	// Cluster is what control groups endpoints by: instance.name and
	// database.cluster, which control requires to be equal. Empty is the
	// endpoint's own name.
	Cluster string
	// Collect is the collector's interval. Its context ends with the
	// interval: a database that answers nothing cannot hold the next one.
	Collect func(context.Context) wire.Database
}

// Config is everything the reporter needs. Now and Client are for tests.
type Config struct {
	// To is control's origin or its full endpoint; empty sends nothing
	// to control.
	To         string
	Credential string
	// StatsD is host:port for UDP gauges; empty sends none.
	StatsD     string
	Interval   time.Duration
	Version    string
	ConfigHash string
	Instances  []Instance
	Log        *zap.Logger
	Now        func() time.Time
	Client     *http.Client
}

// Reporter posts for every instance, once per interval.
type Reporter struct {
	url         string
	logURL      string
	key         ed25519.PrivateKey
	statsd      *statsd
	interval    time.Duration
	postTimeout time.Duration
	version     string
	hash        string
	instances   []Instance
	log         *zap.Logger
	now         func() time.Time
	client      *http.Client
	startedAt   time.Time
	rand        *rand.Rand

	mu      sync.Mutex
	failing map[string]bool
}

// New checks the config and prepares the reporter. The credential error
// never repeats the credential.
func New(c Config) (*Reporter, error) {
	r := &Reporter{
		interval: c.Interval, version: c.Version, hash: c.ConfigHash, instances: c.Instances,
		log: c.Log, now: c.Now, client: c.Client, failing: map[string]bool{},
		rand: rand.New(rand.NewSource(seed())), postTimeout: defaultPostTimeout,
	}
	if r.version == "" {
		r.version = Version
	}
	if r.log == nil {
		r.log = zap.NewNop()
	}
	if r.now == nil {
		r.now = time.Now
	}
	if r.client == nil {
		r.client = &http.Client{Timeout: defaultPostTimeout}
	}
	r.startedAt = r.now().UTC()
	if c.To != "" {
		u, err := url.Parse(c.To)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, errors.New("report.to: not a URL")
		}
		if u.Path == "" || u.Path == "/" {
			u.Path = Endpoint
		}
		r.url = u.String()
		// What the log says: never a user, a password or a query, which
		// an operator may have put in the URL.
		u.User, u.RawQuery, u.Fragment = nil, "", ""
		r.logURL = u.String()
		key, err := wire.ParseCredential(c.Credential)
		if err != nil {
			return nil, errors.New("report.credential: not a credential; it starts with " + wire.CredentialPrefix)
		}
		r.key = key
	}
	if c.StatsD != "" {
		s, err := newStatsD(c.StatsD)
		if err != nil {
			return nil, fmt.Errorf("report.statsd: %w", err)
		}
		r.statsd = s
	}
	return r, nil
}

// seed draws the jitter seed from the OS, as the engine does: a device
// with no battery-backed clock boots to the same time every time.
func seed() int64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return time.Now().UnixNano()
	}
	return int64(binary.LittleEndian.Uint64(b[:]))
}

// Run posts once now and then every interval, jittered, until ctx ends.
func (r *Reporter) Run(ctx context.Context) {
	r.Once(ctx)
	for {
		timer := time.NewTimer(r.nextInterval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			r.Once(ctx)
		}
	}
}

func (r *Reporter) nextInterval() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	spread := float64(r.interval) * jitterFraction
	return r.interval + time.Duration((r.rand.Float64()*2-1)*spread)
}

// Once is one interval: every instance collects and posts, each on its
// own goroutine, and Once returns when all have.
func (r *Reporter) Once(ctx context.Context) {
	r.each(ctx, nil)
}

// Final sends the last bundle for every instance, carrying how the process
// ended. A receiver tells a clean stop from a crash by its presence.
func (r *Reporter) Final(ctx context.Context, exit wire.Exit) {
	r.each(ctx, &exit)
}

func (r *Reporter) each(ctx context.Context, exit *wire.Exit) {
	var wg sync.WaitGroup
	for _, inst := range r.instances {
		wg.Add(1)
		go func(inst Instance) {
			defer wg.Done()
			r.post(ctx, inst, exit)
		}(inst)
	}
	wg.Wait()
}

// Close releases the StatsD socket.
func (r *Reporter) Close() {
	if r.statsd != nil {
		r.statsd.close()
	}
}

// post collects one instance's section, wraps it in a bundle, and sends
// it. It returns nothing: a caller cannot act on a failure.
func (r *Reporter) post(ctx context.Context, inst Instance, exit *wire.Exit) {
	// Collection gets the interval; the POST gets its own budget after.
	cctx, cancelCollect := context.WithTimeout(ctx, r.interval)
	d := inst.Collect(cctx)
	cancelCollect()
	if r.statsd != nil {
		r.statsd.send(inst.Name, &d)
	}
	if r.url == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, r.postTimeout)
	defer cancel()
	b := r.bundle(inst, &d, exit)
	body, err := json.Marshal(b)
	if err != nil {
		r.fail(inst.Name, "encoding the bundle", err)
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, bytes.NewReader(body))
	if err != nil {
		r.fail(inst.Name, "building the request", err)
		return
	}
	req.Header.Set("Content-Type", wire.MediaType)
	if err := wire.SignRequest(req, r.key, body, r.now()); err != nil {
		r.fail(inst.Name, "signing the request", err)
		return
	}
	resp, err := r.client.Do(req)
	if err != nil {
		r.fail(inst.Name, "posting the bundle", err)
		return
	}
	// Drained and closed whatever the status, or the connection is not
	// reused and a process reporting for weeks leaks sockets.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		r.fail(inst.Name, "posting the bundle", fmt.Errorf("receiver answered %s", resp.Status))
		return
	}
	r.succeed(inst.Name)
}

// bundle is the document for one instance: the shared envelope and the
// database section. Pipeline and Serve stay nil, and the kind says so:
// control refuses a database section without it.
func (r *Reporter) bundle(inst Instance, d *wire.Database, exit *wire.Exit) wire.Bundle {
	now := r.now().UTC()
	uptime := int64(now.Sub(r.startedAt).Seconds())
	// The reporter sets both, so they cannot disagree.
	cluster := cmp.Or(inst.Cluster, inst.Name)
	d.Cluster = cluster
	b := wire.Bundle{
		V:               wire.Version,
		SentAt:          now,
		IntervalSeconds: int(r.interval.Seconds()),
		LastActivityAt:  d.Probe.LastOKAt,
		Instance: wire.Instance{
			ID:         "dbhealth-" + inst.Name,
			Name:       cluster,
			Kind:       wire.KindDatabase,
			Version:    r.version,
			Commit:     Commit,
			Arch:       runtime.GOOS + "/" + runtime.GOARCH,
			ConfigHash: r.hash,
			Runtime:    "dbhealth",
		},
		// RSS is the kernel's number, which the Go runtime cannot give;
		// the contract reads an absent one as not measured, which is true.
		Process: wire.Process{
			StartedAt:     r.startedAt,
			UptimeSeconds: &uptime,
			Goroutines:    runtime.NumGoroutine(),
		},
		Database: d,
		Exit:     exit,
	}
	return b
}

// fail logs a failed post: one warning when a run of failures begins and
// debug for the rest. An unregistered instance posts forever, and a line
// per attempt would bury every other line in the log. The error is the
// HTTP client's or the receiver's status; neither names a DSN.
func (r *Reporter) fail(name, what string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.failing[name] {
		r.failing[name] = true
		r.log.Warn("turbostats reporting is failing", zap.String("instance", name), zap.String("step", what),
			zap.String("report_to", r.logURL), zap.Error(redact(err, r.url, r.logURL)))
		return
	}
	r.log.Debug("turbostats post failed", zap.String("instance", name), zap.String("step", what), zap.Error(err))
}

// succeed notes a post that worked, and says so only if the last one did
// not.
func (r *Reporter) succeed(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failing[name] {
		r.failing[name] = false
		r.log.Info("turbostats reporting recovered", zap.String("instance", name), zap.String("report_to", r.logURL))
	}
}

// redact replaces the full URL in an HTTP client's error with the one the
// log may carry.
func redact(err error, full, shown string) error {
	if full == shown {
		return err
	}
	return errors.New(strings.ReplaceAll(err.Error(), full, shown))
}
