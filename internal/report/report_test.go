package report

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// control is an httptest receiver that verifies every request's signature
// against the test credential's public key and keeps the bodies.
type control struct {
	srv    *httptest.Server
	pub    ed25519.PublicKey
	mu     sync.Mutex
	bodies []wire.Bundle
	raw    [][]byte
	bad    []string // signature or header failures
	status []int    // answers to give, in order; 200 after the list runs out
}

func newControl(t *testing.T, priv ed25519.PrivateKey) *control {
	t.Helper()
	c := &control{pub: priv.Public().(ed25519.PublicKey)}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		defer c.mu.Unlock()
		if r.URL.Path != "/v1/turbostats" {
			c.bad = append(c.bad, "path "+r.URL.Path)
		}
		_, ts, sig, err := wire.ParseHeaders(r.Header)
		if err != nil {
			c.bad = append(c.bad, err.Error())
		} else if !wire.Verify(c.pub, r.Method, r.URL.Path, ts, body, sig) {
			c.bad = append(c.bad, "signature does not verify")
		}
		var b wire.Bundle
		if err := json.Unmarshal(body, &b); err != nil {
			c.bad = append(c.bad, "body: "+err.Error())
		}
		c.bodies = append(c.bodies, b)
		c.raw = append(c.raw, body)
		status := http.StatusOK
		if len(c.status) > 0 {
			status, c.status = c.status[0], c.status[1:]
		}
		w.Header().Set("Content-Type", wire.MediaType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"v":1,"commands":[]}`))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *control) posts() []wire.Bundle {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]wire.Bundle(nil), c.bodies...)
}

const dsn = "postgres://u:hunter2@pg.internal:5432/billing"

func credential(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	cred, err := wire.FormatCredential(seed)
	assert.NoError(t, err)
	priv, err := wire.ParseCredential(cred)
	assert.NoError(t, err)
	return cred, priv
}

// database is a Collect that answers a fixed section with the probe OK at
// the given time.
func database(okAt time.Time) func(context.Context) wire.Database {
	return func(context.Context) wire.Database {
		used, n := int64(1), int64(7832)
		_ = used
		return wire.Database{
			Kind: "postgres", Target: "pg.internal:5432/billing", Cluster: "billing",
			Probe:      wire.DatabaseProbe{OK: true, LatencyMs: 3, LastOKAt: &okAt},
			Resources:  &wire.DatabaseResources{Connections: &wire.DatabaseConnections{Used: 18, Max: 100}, TableCount: &tableCount, PartitionCount: &partitionCount},
			Tables:     []wire.DatabaseTable{{Name: "public.usage_per_minute", Rows: &n, RowsExact: true, SizeBytes: 4096, CheckedAt: okAt}},
			Collection: wire.DatabaseCollection{Queries: 9, DurationMs: 12, Errors: []wire.DatabaseError{}},
		}
	}
}

func newReporter(t *testing.T, c *control, cred string, now func() time.Time, instances ...Instance) (*Reporter, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.DebugLevel)
	r, err := New(Config{
		To: c.srv.URL, Credential: cred, Interval: 60 * time.Second,
		Version: "v0.1.0-test", ConfigHash: "sha256:abc",
		Log: zap.New(core), Now: now, Instances: instances,
	})
	assert.NoError(t, err)
	return r, logs
}

var okAt = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func fixedNow() time.Time { return okAt.Add(5 * time.Second) }

// Control's own database, the day these counts were added.
var tableCount, partitionCount = 172, 148

func TestReport_BundleShape(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "billing-primary", Cluster: "billing", Collect: database(okAt)})
	r.Once(context.Background())
	posts := c.posts()
	assert.Equal(t, 1, len(posts))
	b := posts[0]
	assert.Equal(t, wire.Version, b.V)
	// Control files the report by its kind, and groups endpoints by
	// instance.name, which must equal database.cluster.
	assert.Equal(t, wire.KindDatabase, b.Instance.Kind)
	assert.Equal(t, "billing", b.Instance.Name)
	assert.Equal(t, "billing", b.Database.Cluster)
	assert.Equal(t, 60, b.IntervalSeconds)
	assert.True(t, b.SentAt.Equal(fixedNow()))
	assert.Equal(t, "dbhealth-billing-primary", b.Instance.ID)
	assert.Equal(t, "v0.1.0-test", b.Instance.Version)
	assert.Equal(t, "sha256:abc", b.Instance.ConfigHash)
	assert.Equal(t, "dbhealth", b.Instance.Runtime)
	assert.False(t, b.Process.StartedAt.IsZero())
	assert.NotNil(t, b.Database)
	assert.Equal(t, "postgres", b.Database.Kind)
	assert.Nil(t, b.Pipeline)
	assert.Nil(t, b.Serve)
	assert.Nil(t, b.Exit)
	assert.Equal(t, 0, len(c.bad))
}

func TestReport_LastActivityIsTheLastOKProbe(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Once(context.Background())
	b := c.posts()[0]
	assert.NotNil(t, b.LastActivityAt)
	assert.True(t, b.LastActivityAt.Equal(okAt))
}

func TestReport_SignatureVerifies(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Once(context.Background())
	assert.Equal(t, 1, len(c.posts()))
	if len(c.bad) > 0 {
		t.Fatalf("the receiver refused the request: %v", c.bad)
	}
}

func TestReport_ToWithoutAPathGetsTheEndpoint(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	core, _ := observer.New(zap.DebugLevel)
	for _, to := range []string{c.srv.URL, c.srv.URL + "/", c.srv.URL + "/v1/turbostats"} {
		r, err := New(Config{To: to, Credential: cred, Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
			Instances: []Instance{{Name: "x", Collect: database(okAt)}}})
		assert.NoError(t, err)
		r.Once(context.Background())
	}
	assert.Equal(t, 3, len(c.posts()))
	assert.Equal(t, 0, len(c.bad))
}

func TestReport_ARefusedPostIsLoggedOnceAndRetriedNextInterval(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	c.status = []int{500, 500, 200}
	r, logs := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Once(context.Background())
	r.Once(context.Background())
	r.Once(context.Background())
	assert.Equal(t, 3, len(c.posts()))
	warns := logs.FilterLevelExact(zap.WarnLevel).All()
	assert.Equal(t, 1, len(warns))
	assert.That(t, strings.Contains(warns[0].Message, "failing"))
	infos := logs.FilterLevelExact(zap.InfoLevel).FilterMessageSnippet("recovered").All()
	assert.Equal(t, 1, len(infos))
}

func TestReport_OneBundlePerDatabasePerInterval(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow,
		Instance{Name: "a", Collect: database(okAt)},
		Instance{Name: "b", Collect: database(okAt)})
	for i := 0; i < 3; i++ {
		r.Once(context.Background())
	}
	posts := c.posts()
	assert.Equal(t, 6, len(posts))
	per := map[string]int{}
	for _, b := range posts {
		per[b.Instance.ID]++
	}
	assert.Equal(t, 3, per["dbhealth-a"])
	assert.Equal(t, 3, per["dbhealth-b"])
}

func TestReport_FinalCarriesTheExit(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Final(context.Background(), wire.Exit{Reason: "signal", Code: 0})
	b := c.posts()[0]
	assert.NotNil(t, b.Exit)
	assert.Equal(t, "signal", b.Exit.Reason)
}

func TestReport_DSNNeverInBody(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, logs := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Once(context.Background())
	c.mu.Lock()
	raw := string(c.raw[0])
	c.mu.Unlock()
	assert.False(t, strings.Contains(raw, "hunter2"))
	assert.False(t, strings.Contains(raw, dsn))
	for _, e := range logs.All() {
		assert.False(t, strings.Contains(e.Message, "hunter2"))
		for _, f := range e.Context {
			assert.False(t, strings.Contains(f.String, "hunter2"))
		}
	}
}

func TestReport_CredentialIsChecked(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	_, err := New(Config{To: "http://x", Credential: "not-a-credential", Interval: time.Minute, Log: zap.New(core)})
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "credential"))
	assert.False(t, strings.Contains(err.Error(), "not-a-credential"))
}

// statsd is a UDP listener collecting datagrams.
func statsdListener(t *testing.T) (string, func() []string) {
	t.Helper()
	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	assert.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	var mu sync.Mutex
	var lines []string
	go func() {
		buf := make([]byte, 65535)
		for {
			n, _, err := conn.ReadFrom(buf)
			if err != nil {
				return
			}
			mu.Lock()
			for _, l := range strings.Split(strings.TrimSpace(string(buf[:n])), "\n") {
				lines = append(lines, l)
			}
			mu.Unlock()
		}
	}()
	return conn.LocalAddr().String(), func() []string {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			n := len(lines)
			mu.Unlock()
			if n > 0 {
				time.Sleep(50 * time.Millisecond)
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), lines...)
	}
}

func TestStatsD_GaugesMirrorTheBundle(t *testing.T) {
	addr, got := statsdListener(t)
	core, _ := observer.New(zap.DebugLevel)
	r, err := New(Config{StatsD: addr, Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "billing-primary", Collect: database(okAt)}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	lines := got()
	has := func(prefix string) bool {
		for _, l := range lines {
			if strings.HasPrefix(l, prefix) {
				return true
			}
		}
		return false
	}
	for _, want := range []string{
		"dbhealth.probe.ok:1|g|#db:billing-primary",
		"dbhealth.probe.latency_ms:3|g|#db:billing-primary",
		"dbhealth.connections.used:18|g|#db:billing-primary",
		"dbhealth.connections.max:100|g|#db:billing-primary",
		"dbhealth.tables:172|g|#db:billing-primary",
		"dbhealth.partitions:148|g|#db:billing-primary",
		"dbhealth.table.rows:7832|g|#db:billing-primary,table:public.usage_per_minute",
		"dbhealth.table.size_bytes:4096|g|#db:billing-primary,table:public.usage_per_minute",
		"dbhealth.collection.queries:9|g|#db:billing-primary",
	} {
		if !has(want) {
			t.Errorf("no gauge %q in:\n%s", want, strings.Join(lines, "\n"))
		}
	}
	// Absent fields send nothing.
	for _, l := range lines {
		assert.False(t, strings.HasPrefix(l, "dbhealth.size_bytes"))
		assert.False(t, strings.HasPrefix(l, "dbhealth.replication"))
		assert.False(t, strings.HasPrefix(l, "dbhealth.table.newest_at"))
	}
}

// A UDP datagram over the path MTU is fragmented or dropped, and StatsD
// servers cap what they read. Fifty tables of gauges go out in several
// datagrams, each under 1400 bytes and each ending on a line.
func TestStatsD_DatagramsStayUnderTheMTU(t *testing.T) {
	d := database(okAt)(context.Background())
	for i := 0; i < 49; i++ {
		n := int64(i)
		d.Tables = append(d.Tables, wire.DatabaseTable{Name: fmt.Sprintf("public.table_with_a_long_name_%02d", i), Rows: &n, SizeBytes: 1 << 30, CheckedAt: okAt})
	}
	grams := datagrams("billing-primary", &d)
	assert.That(t, len(grams) > 1)
	total := 0
	for _, g := range grams {
		assert.That(t, len(g) <= maxDatagram)
		assert.True(t, strings.HasSuffix(g, "\n"))
		for _, line := range strings.Split(strings.TrimSpace(g), "\n") {
			assert.True(t, strings.HasPrefix(line, "dbhealth."))
			assert.That(t, strings.Contains(line, "|g|#db:billing-primary"))
			total++
		}
	}
	// probe 3 + connections 3 + tables and partitions 2 + 50 tables × (rows, rows_exact, size, schema_changes) + collection 3
	assert.Equal(t, 3+3+2+50*4+3, total)
}

// Findings from the review of #5.

// A slow collection must not eat the POST's budget: the bundle still goes
// out, late, rather than not at all.
func TestReport_ASlowCollectStillPosts(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	slow := func(ctx context.Context) wire.Database {
		// Longer than the POST timeout a test is given, shorter than the
		// collect budget.
		time.Sleep(300 * time.Millisecond)
		return database(okAt)(ctx)
	}
	core, logs := observer.New(zap.DebugLevel)
	r, err := New(Config{To: c.srv.URL, Credential: cred, Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: slow}}})
	assert.NoError(t, err)
	r.postTimeout = 200 * time.Millisecond
	r.Once(context.Background())
	assert.Equal(t, 1, len(c.posts()))
	assert.Equal(t, 0, len(logs.FilterLevelExact(zap.WarnLevel).All()))
}

// Collection is bounded by the interval: a database that answers nothing
// cannot hold the next interval back.
func TestReport_CollectIsBoundedByTheInterval(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	var deadline time.Duration
	collect := func(ctx context.Context) wire.Database {
		d, _ := ctx.Deadline()
		deadline = time.Until(d)
		return database(okAt)(ctx)
	}
	core, _ := observer.New(zap.DebugLevel)
	r, err := New(Config{To: c.srv.URL, Credential: cred, Interval: 30 * time.Second, Log: zap.New(core),
		Instances: []Instance{{Name: "x", Collect: collect}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	assert.That(t, deadline > 25*time.Second && deadline <= 30*time.Second)
}

func TestReport_RSSIsNotInventedFromTheGoRuntime(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "x", Collect: database(okAt)})
	r.Once(context.Background())
	assert.Equal(t, int64(0), c.posts()[0].Process.RSSBytes)
}

func TestReport_ReportToIsLoggedWithoutASecret(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	c.status = []int{500}
	core, logs := observer.New(zap.DebugLevel)
	to := strings.Replace(c.srv.URL, "http://", "http://user:hunter2@", 1) + "/v1/turbostats?token=hunter2"
	r, err := New(Config{To: to, Credential: cred, Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: database(okAt)}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	warns := logs.FilterLevelExact(zap.WarnLevel).All()
	assert.Equal(t, 1, len(warns))
	for _, f := range warns[0].Context {
		assert.False(t, strings.Contains(f.String, "hunter2"))
		if f.Interface != nil {
			assert.False(t, strings.Contains(fmt.Sprint(f.Interface), "hunter2"))
		}
	}
}

func TestStatsD_GaugesArePlainAndNeverNegative(t *testing.T) {
	d := database(okAt)(context.Background())
	tiny, negative := 1.2e-05, -0.3
	d.Replication = &wire.DatabaseReplication{Role: "replica", LagSeconds: &tiny,
		Replicas: []wire.DatabaseReplica{{Name: "r1", LagSeconds: &negative}}}
	lines := strings.Split(strings.TrimSpace(strings.Join(datagrams("db", &d), "")), "\n")
	var lag, replica string
	for _, l := range lines {
		if strings.HasPrefix(l, "dbhealth.replication.lag_seconds:") {
			lag = l
		}
		if strings.HasPrefix(l, "dbhealth.replication.replica.lag_seconds:") {
			replica = l
		}
	}
	assert.Equal(t, "dbhealth.replication.lag_seconds:0.000012|g|#db:db", lag)
	assert.Equal(t, "dbhealth.replication.replica.lag_seconds:0|g|#db:db,replica:r1", replica)
}

// An endpoint with no cluster is its own: its name is the cluster, in
// instance.name and in database.cluster, whatever the collector sent.
func TestReport_NoClusterIsItsOwn(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "orders-pg", Collect: database(okAt)})
	r.Once(context.Background())
	b := c.posts()[0]
	assert.Equal(t, "dbhealth-orders-pg", b.Instance.ID)
	assert.Equal(t, "orders-pg", b.Instance.Name)
	assert.Equal(t, "orders-pg", b.Database.Cluster)
}

// Prometheus: the same facts as the StatsD gauges, scraped.

func scrape(t *testing.T, r *Reporter) string {
	t.Helper()
	srv := httptest.NewServer(r.Metrics())
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	assert.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestMetrics_SeriesMirrorTheBundle(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	r, err := New(Config{Metrics: "prometheus", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "billing-primary", Collect: database(okAt)}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	body := scrape(t, r)
	for _, want := range []string{
		`dbhealth_probe_ok{db="billing-primary"} 1`,
		`dbhealth_probe_latency_ms{db="billing-primary"} 3`,
		`dbhealth_connections_used{db="billing-primary"} 18`,
		`dbhealth_connections_max{db="billing-primary"} 100`,
		`dbhealth_tables{db="billing-primary"} 172`,
		`dbhealth_partitions{db="billing-primary"} 148`,
		`dbhealth_table_rows{db="billing-primary",table="public.usage_per_minute"} 7832`,
		`dbhealth_table_rows_exact{db="billing-primary",table="public.usage_per_minute"} 1`,
		`dbhealth_table_size_bytes{db="billing-primary",table="public.usage_per_minute"} 4096`,
		`dbhealth_collection_queries{db="billing-primary"} 9`,
		`dbhealth_collection_errors{db="billing-primary"} 0`,
		`dbhealth_last_collect_timestamp_seconds{db="billing-primary"}`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no series %q in:\n%s", want, body)
		}
	}
	// Absent fields are absent series.
	assert.False(t, strings.Contains(body, "dbhealth_size_bytes{"))
	assert.False(t, strings.Contains(body, "dbhealth_replication_lag_seconds{"))
	assert.False(t, strings.Contains(body, "dbhealth_table_newest_at_seconds{"))
}

// A field present last interval and absent this one is gone from the
// scrape, not a stale number: a role that loses pg_read_all_stats, or a
// table dropped, stops being reported rather than frozen.
func TestMetrics_AnAbsentFieldLeavesNoStaleSeries(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	full := true
	collect := func(ctx context.Context) wire.Database {
		d := database(okAt)(ctx)
		if !full {
			d.Resources.Connections = nil
			d.Tables = nil
		}
		return d
	}
	r, err := New(Config{Metrics: "prometheus", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: collect}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	assert.That(t, strings.Contains(scrape(t, r), `dbhealth_connections_used{db="x"}`))
	full = false
	r.Once(context.Background())
	body := scrape(t, r)
	assert.False(t, strings.Contains(body, `dbhealth_connections_used{db="x"}`))
	assert.False(t, strings.Contains(body, `dbhealth_table_rows{`))
	assert.That(t, strings.Contains(body, `dbhealth_probe_ok{db="x"} 1`))
}

func TestMetrics_TwoDatabasesDoNotCollide(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	r, err := New(Config{Metrics: "prometheus", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "a", Collect: database(okAt)}, {Name: "b", Collect: database(okAt)}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	body := scrape(t, r)
	assert.That(t, strings.Contains(body, `dbhealth_probe_ok{db="a"} 1`))
	assert.That(t, strings.Contains(body, `dbhealth_probe_ok{db="b"} 1`))
}

func TestMetrics_NoneConfiguredIsNoHandler(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	r, err := New(Config{StatsD: "127.0.0.1:1", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: database(okAt)}}})
	assert.NoError(t, err)
	assert.Nil(t, r.Metrics())
}

// OTLP: a collector's origin is enough; the path is the protocol's. The
// gauges reach it on Close, which flushes the periodic reader.
func TestMetrics_OTLPPostsToV1Metrics(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	var bodies [][]byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	core, logs := observer.New(zap.DebugLevel)
	for _, to := range []string{srv.URL, srv.URL + "/", srv.URL + "/v1/metrics"} {
		r, err := New(Config{Metrics: "otlp", OTLP: to, Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
			Instances: []Instance{{Name: "x", Collect: database(okAt)}}})
		assert.NoError(t, err)
		r.Once(context.Background())
		r.Close()
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 3, len(paths))
	for i, p := range paths {
		assert.Equal(t, "/v1/metrics", p)
		assert.That(t, strings.Contains(string(bodies[i]), "dbhealth_probe_ok"))
	}
	assert.Equal(t, 0, len(logs.FilterLevelExact(zap.WarnLevel).All()))
}

// An exporter that cannot reach its collector says so in dbhealth's own
// log, once per run of failures, not on stderr through a global handler.
func TestMetrics_OTLPFailureIsLoggedOnce(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	r, err := New(Config{Metrics: "otlp", OTLP: "http://127.0.0.1:1", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: database(okAt)}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	r.Close()
	warns := logs.FilterLevelExact(zap.WarnLevel).All()
	assert.Equal(t, 1, len(warns))
	assert.That(t, strings.Contains(warns[0].Message, "metrics export is failing"))
}

// Load: every field a gauge, absent fields absent.

func loaded(d wire.Database) wire.Database {
	n, w := 12, 1
	tps, rr, ratio, longest := 182.4, 90210.0, 0.993, 41.2
	dead, seq := int64(1203), 0.1
	d.Load = &wire.DatabaseLoad{SessionsActiveNow: &n, SessionsWaitingNow: &w, LongestQuerySeconds: &longest,
		TransactionsPerSecond: &tps, RowsReadPerSecond: &rr, CacheHitRatio: &ratio}
	d.Tables[0].DeadRows = &dead
	d.Tables[0].SeqScansPerSecond = &seq
	ins := 33.4
	d.Tables[0].RowsInsertedPerSecond = &ins
	d.Tables[0].SchemaHash = "9f2c1a7e4b3d8c05"
	d.Tables[0].SchemaChanges = []wire.DatabaseSchemaChange{{Column: "region", Change: "added", To: "text"}}
	return d
}

func TestStatsD_LoadGauges(t *testing.T) {
	d := loaded(database(okAt)(context.Background()))
	lines := strings.Split(strings.TrimSpace(strings.Join(datagrams("db", &d), "")), "\n")
	has := func(prefix string) bool {
		for _, l := range lines {
			if strings.HasPrefix(l, prefix) {
				return true
			}
		}
		return false
	}
	for _, want := range []string{
		"dbhealth.load.sessions_active_now:12|g|#db:db",
		"dbhealth.load.sessions_waiting_now:1|g|#db:db",
		"dbhealth.load.longest_query_seconds:41.2|g|#db:db",
		"dbhealth.load.transactions_per_second:182.4|g|#db:db",
		"dbhealth.load.rows_read_per_second:90210|g|#db:db",
		"dbhealth.load.cache_hit_ratio:0.993|g|#db:db",
		"dbhealth.table.dead_rows:1203|g|#db:db,table:public.usage_per_minute",
		"dbhealth.table.seq_scans_per_second:0.1|g|#db:db,table:public.usage_per_minute",
	} {
		if !has(want) {
			t.Errorf("no gauge %q in:\n%s", want, strings.Join(lines, "\n"))
		}
	}
	for _, absent := range []string{"dbhealth.load.rows_written_per_second", "dbhealth.load.queries_per_second",
		"dbhealth.load.sessions_idle_in_transaction_now", "dbhealth.table.index_scans_per_second"} {
		assert.False(t, has(absent))
	}
	// No load at all: no load gauges.
	plain := database(okAt)(context.Background())
	for _, l := range strings.Split(strings.Join(datagrams("db", &plain), ""), "\n") {
		assert.False(t, strings.HasPrefix(l, "dbhealth.load."))
	}
}

func TestMetrics_LoadSeries(t *testing.T) {
	core, _ := observer.New(zap.DebugLevel)
	with := true
	collect := func(ctx context.Context) wire.Database {
		d := database(okAt)(ctx)
		if with {
			return loaded(d)
		}
		return d
	}
	r, err := New(Config{Metrics: "prometheus", Interval: time.Minute, Log: zap.New(core), Now: fixedNow,
		Instances: []Instance{{Name: "x", Collect: collect}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	body := scrape(t, r)
	for _, want := range []string{
		`dbhealth_load_sessions_active_now{db="x"} 12`,
		`dbhealth_load_sessions_waiting_now{db="x"} 1`,
		`dbhealth_load_longest_query_seconds{db="x"} 41.2`,
		`dbhealth_load_transactions_per_second{db="x"} 182.4`,
		`dbhealth_load_rows_read_per_second{db="x"} 90210`,
		`dbhealth_load_cache_hit_ratio{db="x"} 0.993`,
		`dbhealth_table_dead_rows{db="x",table="public.usage_per_minute"} 1203`,
		`dbhealth_table_seq_scans_per_second{db="x",table="public.usage_per_minute"} 0.1`,
		`dbhealth_table_rows_inserted_per_second{db="x",table="public.usage_per_minute"} 33.4`,
		`dbhealth_table_schema_changes{db="x",table="public.usage_per_minute"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("no series %q in:\n%s", want, body)
		}
	}
	assert.False(t, strings.Contains(body, "dbhealth_load_rows_written_per_second{"))
	assert.False(t, strings.Contains(body, "dbhealth_load_queries_per_second{"))
	assert.False(t, strings.Contains(body, "dbhealth_table_index_scans_per_second{"))
	// The next interval without load: the series are gone, not stale.
	with = false
	r.Once(context.Background())
	body = scrape(t, r)
	assert.False(t, strings.Contains(body, "dbhealth_load_"))
	assert.False(t, strings.Contains(body, "dbhealth_table_dead_rows{"))
}
