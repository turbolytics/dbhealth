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
			Resources:  &wire.DatabaseResources{Connections: &wire.DatabaseConnections{Used: 18, Max: 100}},
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

func TestReport_BundleShape(t *testing.T) {
	cred, priv := credential(t)
	c := newControl(t, priv)
	r, _ := newReporter(t, c, cred, fixedNow, Instance{Name: "billing-primary", Collect: database(okAt)})
	r.Once(context.Background())
	posts := c.posts()
	assert.Equal(t, 1, len(posts))
	b := posts[0]
	assert.Equal(t, wire.Version, b.V)
	assert.Equal(t, 60, b.IntervalSeconds)
	assert.True(t, b.SentAt.Equal(fixedNow()))
	assert.Equal(t, "dbhealth-billing-primary", b.Instance.ID)
	assert.Equal(t, "billing-primary", b.Instance.Name)
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
	r, logs := newReporter(t, c, cred, fixedNow, Instance{Name: "x", DSN: dsn, Collect: database(okAt)})
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
	// probe 3 + connections 3 + 50 tables × (rows, rows_exact, size) + collection 3
	assert.Equal(t, 3+3+50*3+3, total)
}
