package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"

	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/postgres"
)

// fake is a source that answers from fields and counts what was asked.
type fake struct {
	probeOK    bool
	tables     []config.StaticTable
	tableErr   map[string]error
	partial    map[string]error // Table returns the row and this, wrapped in postgres.ErrPartial
	dropped    int
	queries    int
	discovers  int
	exacts     map[string]int
	freshes    map[string]int
	now        func() time.Time
	replicated *wire.DatabaseReplication
}

func newFake(now func() time.Time) *fake {
	return &fake{probeOK: true, tableErr: map[string]error{}, partial: map[string]error{}, exacts: map[string]int{}, freshes: map[string]int{}, now: now}
}

func (f *fake) Probe(ctx context.Context, timeout time.Duration) wire.DatabaseProbe {
	f.queries++
	if !f.probeOK {
		return wire.DatabaseProbe{OK: false, Error: "refused"}
	}
	at := f.now()
	return wire.DatabaseProbe{OK: true, LatencyMs: 3, LastOKAt: &at}
}

func (f *fake) Version(ctx context.Context) (string, error) {
	f.queries++
	return "18.1", nil
}

func (f *fake) Resources(ctx context.Context) (*wire.DatabaseResources, []wire.DatabaseError) {
	f.queries += 4
	n := int64(1)
	return &wire.DatabaseResources{SizeBytes: &n}, nil
}

func (f *fake) Discover(ctx context.Context, d config.Discover) ([]config.StaticTable, int, error) {
	f.queries++
	f.discovers++
	return f.tables, f.dropped, nil
}

func (f *fake) Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, error) {
	f.queries++
	if fresh {
		f.queries++
		f.freshes[t.Name]++
	}
	if exact {
		f.queries++
		f.exacts[t.Name]++
	}
	if err := f.tableErr[t.Name]; err != nil {
		return wire.DatabaseTable{}, err
	}
	rows := int64(100 + f.exacts[t.Name])
	// CheckedAt is the source's own clock after the query, 300ms late, as
	// a real query's is; the collector must schedule on its own reading.
	out := wire.DatabaseTable{Name: t.Name, FreshnessColumn: t.FreshnessColumn, Rows: &rows, RowsExact: exact, SizeBytes: 4096, CheckedAt: f.now().Add(300 * time.Millisecond)}
	if err := f.partial[t.Name]; err != nil {
		return out, fmt.Errorf("%w: %w", postgres.ErrPartial, err)
	}
	if fresh && t.FreshnessColumn != "" {
		at := f.now()
		out.NewestAt = &at
	}
	return out, nil
}

func (f *fake) Replication(ctx context.Context) (*wire.DatabaseReplication, error) {
	f.queries++
	return f.replicated, nil
}

func (f *fake) Queries() int { return f.queries }

var _ Source = (*fake)(nil)

const dsn = "postgres://u:hunter2@pg.internal:5432/billing"

func database() config.Database {
	return config.Database{Kind: "postgres", DSN: dsn, Name: "billing-primary", Cluster: "billing"}
}

func static(rows string, names ...string) config.Tables {
	t := config.Tables{Rows: rows, FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600}
	for _, n := range names {
		t.Static = append(t.Static, config.StaticTable{Name: n, FreshnessColumn: "updated_at"})
	}
	return t
}

func probe() config.Probe { return config.Probe{IntervalSeconds: 60, TimeoutSeconds: 5} }

// clock is a now() the tests advance by hand.
type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newClock() *clock { return &clock{t: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)} }

func TestCollect_FailedProbeSendsProbeAlone(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.probeOK = false
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.False(t, d.Probe.OK)
	assert.Equal(t, 1, d.Probe.ConsecutiveFailures)
	assert.Nil(t, d.Resources)
	assert.Nil(t, d.Tables)
	assert.Nil(t, d.Replication)
	assert.Equal(t, 1, d.Collection.Queries)
	assert.Equal(t, "postgres", d.Kind)
	assert.Equal(t, "pg.internal:5432/billing", d.Target)
	assert.Equal(t, "billing", d.Cluster)
}

func TestCollect_ConsecutiveFailuresCountAndLastOKIsKept(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("estimate"), probe(), f, ck.now)
	ok := c.Collect(context.Background())
	assert.True(t, ok.Probe.OK)
	assert.Equal(t, 0, ok.Probe.ConsecutiveFailures)
	okAt := *ok.Probe.LastOKAt

	f.probeOK = false
	ck.advance(time.Minute)
	c.Collect(context.Background())
	ck.advance(time.Minute)
	d := c.Collect(context.Background())
	assert.Equal(t, 2, d.Probe.ConsecutiveFailures)
	assert.NotNil(t, d.Probe.LastOKAt)
	assert.True(t, d.Probe.LastOKAt.Equal(okAt))

	f.probeOK = true
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 0, d.Probe.ConsecutiveFailures)
	assert.True(t, d.Probe.LastOKAt.After(okAt))
}

func TestCollect_OneTableErrorKeepsTheOthers(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tableErr["public.b"] = errors.New(`relation "public.b" does not exist`)
	c := New(database(), static("estimate", "public.a", "public.b"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.True(t, d.Probe.OK)
	assert.Equal(t, 1, len(d.Tables))
	assert.Equal(t, "public.a", d.Tables[0].Name)
	assert.Equal(t, 1, len(d.Collection.Errors))
	assert.Equal(t, "public.b", d.Collection.Errors[0].Table)
	assert.That(t, strings.Contains(d.Collection.Errors[0].Error, "does not exist"))
}

func TestCollect_ExactCountsRunOnTheirInterval(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("exact", "public.t"), probe(), f, ck.now)

	d := c.Collect(context.Background()) // t=0: count(*) runs
	assert.Equal(t, 1, f.exacts["public.t"])
	assert.True(t, d.Tables[0].RowsExact)
	assert.Equal(t, int64(101), *d.Tables[0].Rows)
	first := d.Tables[0].CheckedAt

	ck.advance(time.Minute)
	d = c.Collect(context.Background()) // t=60: not due; the last exact value, with its time
	assert.Equal(t, 1, f.exacts["public.t"])
	assert.True(t, d.Tables[0].RowsExact)
	assert.Equal(t, int64(101), *d.Tables[0].Rows)
	assert.True(t, d.Tables[0].CheckedAt.Equal(first))

	ck.advance(59 * time.Minute)
	d = c.Collect(context.Background()) // t=3600: due again
	assert.Equal(t, 2, f.exacts["public.t"])
	assert.Equal(t, int64(102), *d.Tables[0].Rows)
	assert.True(t, d.Tables[0].CheckedAt.After(first))
}

func TestCollect_AStaticTableCanAskForExactOnItsOwn(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	tables := static("estimate", "public.a", "public.b")
	tables.Static[1].Rows = "exact"
	c := New(database(), tables, probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Equal(t, 0, f.exacts["public.a"])
	assert.Equal(t, 1, f.exacts["public.b"])
	assert.False(t, d.Tables[0].RowsExact)
	assert.True(t, d.Tables[1].RowsExact)
}

func TestCollect_FreshnessRunsOnItsInterval(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	tables := static("estimate", "public.t")
	tables.FreshnessIntervalSeconds = 300
	c := New(database(), tables, probe(), f, ck.now)

	d := c.Collect(context.Background())
	assert.Equal(t, 1, f.freshes["public.t"])
	assert.NotNil(t, d.Tables[0].NewestAt)
	newest := *d.Tables[0].NewestAt

	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 1, f.freshes["public.t"])
	assert.NotNil(t, d.Tables[0].NewestAt) // carried from the last check
	assert.True(t, d.Tables[0].NewestAt.Equal(newest))

	ck.advance(4 * time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 2, f.freshes["public.t"])
	assert.True(t, d.Tables[0].NewestAt.After(newest))
}

func TestCollect_DiscoveryRerunsOnItsInterval(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tables = []config.StaticTable{{Name: "public.found", FreshnessColumn: "ts"}}
	tables := config.Tables{Rows: "estimate", FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600,
		Discover: &config.Discover{Schemas: []string{"public"}, FreshnessColumns: config.DefaultFreshnessColumns, MaxTables: 50}}
	c := New(database(), tables, probe(), f, ck.now)

	d := c.Collect(context.Background())
	assert.Equal(t, 1, f.discovers)
	assert.Equal(t, 1, len(d.Tables))
	assert.Equal(t, "public.found", d.Tables[0].Name)

	for i := 0; i < 9; i++ {
		ck.advance(time.Minute)
		c.Collect(context.Background())
	}
	assert.Equal(t, 1, f.discovers)

	ck.advance(time.Minute) // t = 10 × freshness interval
	c.Collect(context.Background())
	assert.Equal(t, 2, f.discovers)
}

func TestCollect_CostIsReported(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("exact", "public.a", "public.b"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	// probe, version, 4 resources, replication, 2 × (table + fresh + exact)
	assert.Equal(t, f.queries, d.Collection.Queries)
	assert.Equal(t, 13, d.Collection.Queries)
	assert.That(t, d.Collection.DurationMs >= 0)

	ck.advance(time.Minute)
	before := f.queries
	d = c.Collect(context.Background())
	assert.Equal(t, f.queries-before, d.Collection.Queries)
}

func TestCollect_VersionIsReadOnce(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("estimate"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Equal(t, "18.1", d.ServerVersion)
	// probe, version, 4 resources, replication
	assert.Equal(t, 7, d.Collection.Queries)
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, "18.1", d.ServerVersion)
	assert.Equal(t, 6, d.Collection.Queries)
}

func TestCollect_TargetIsRedacted(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	want, err := config.RedactedTarget(dsn)
	assert.NoError(t, err)
	assert.Equal(t, want, d.Target)
	raw, err := json.Marshal(wire.Bundle{Database: &d})
	assert.NoError(t, err)
	assert.False(t, strings.Contains(string(raw), "hunter2"))
	assert.False(t, strings.Contains(string(raw), dsn))
}

func TestCollect_ErrorsAreNeverNilInJSON(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	raw, err := json.Marshal(d)
	assert.NoError(t, err)
	assert.That(t, strings.Contains(string(raw), `"errors":[]`))
}

// Findings from the review of #4.

func TestCollect_AJitteredTickStillRunsTheCheck(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	c.Collect(context.Background())
	assert.Equal(t, 1, f.freshes["public.t"])
	// The reporter jitters the interval by up to ten percent; a tick 58s
	// after the last is still this interval's freshness check.
	ck.advance(58 * time.Second)
	c.Collect(context.Background())
	assert.Equal(t, 2, f.freshes["public.t"])
}

func TestCollect_ExactCountsDoNotDrift(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	c := New(database(), static("exact", "public.t"), probe(), f, ck.now)
	// The clock the collector schedules on is the one it reads at the
	// start of the interval, not when the table's query returns.
	for i := 0; i < 61; i++ {
		c.Collect(context.Background())
		ck.advance(time.Minute)
	}
	// t=0 and t=3600: exactly two, not one at 0 and one late.
	assert.Equal(t, 2, f.exacts["public.t"])
}

func TestCollect_APartialTableIsReportedWithItsError(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.partial["public.t"] = errors.New(`public.t: max(nope): column "nope" does not exist`)
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Equal(t, 1, len(d.Tables))
	assert.Nil(t, d.Tables[0].NewestAt)
	assert.Equal(t, 1, len(d.Collection.Errors))
	assert.Equal(t, "public.t", d.Collection.Errors[0].Table)
}

func TestCollect_TablesBeyondTheCapAreAnError(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tables = []config.StaticTable{{Name: "public.a"}}
	f.dropped = 150
	tables := config.Tables{Rows: "estimate", FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600,
		Discover: &config.Discover{Schemas: []string{"public"}, FreshnessColumns: config.DefaultFreshnessColumns, MaxTables: 50}}
	c := New(database(), tables, probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Equal(t, 1, len(d.Collection.Errors))
	assert.That(t, strings.Contains(d.Collection.Errors[0].Error, "150"))
	assert.That(t, strings.Contains(d.Collection.Errors[0].Error, "max_tables"))
}

func TestCollect_ForgetsATableDiscoveryStopsReturning(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tables = []config.StaticTable{{Name: "public.a"}, {Name: "public.b"}}
	tables := config.Tables{Rows: "estimate", FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600,
		Discover: &config.Discover{Schemas: []string{"public"}, FreshnessColumns: config.DefaultFreshnessColumns, MaxTables: 50}}
	c := New(database(), tables, probe(), f, ck.now)
	c.Collect(context.Background())
	assert.Equal(t, 2, len(c.last))
	f.tables = f.tables[:1]
	ck.advance(10 * time.Minute)
	c.Collect(context.Background())
	assert.Equal(t, 1, len(c.last))
}
