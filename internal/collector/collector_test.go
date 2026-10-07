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
	"github.com/turbolytics/dbhealth/internal/source"
)

// fake is a source that answers from fields and counts what was asked.
type fake struct {
	probeOK  bool
	tables   []config.StaticTable
	tableErr map[string]error
	partial  map[string]error // Table returns the row and this, wrapped in postgres.ErrPartial
	dropped  int
	// load: the fake's counters advance 60 commits, 1000 rows read, 50
	// written, 900 hits and 100 misses per call; tables 6 seq and 600 idx
	// scans.
	loadCounters  bool
	noCounters    bool
	loadErr       []wire.DatabaseError
	loads         int
	tableCounters bool
	calls         map[string]int64
	// columns is each table's schema as the source reads it.
	columns    map[string][]source.Column
	queries    int
	discovers  int
	exacts     map[string]int
	freshes    map[string]int
	now        func() time.Time
	replicated *wire.DatabaseReplication
}

func newFake(now func() time.Time) *fake {
	return &fake{probeOK: true, tableErr: map[string]error{}, partial: map[string]error{}, exacts: map[string]int{}, freshes: map[string]int{}, now: now, calls: map[string]int64{}, columns: map[string][]source.Column{}}
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

func (f *fake) Load(ctx context.Context) (source.Sample, source.Counters, []wire.DatabaseError) {
	f.queries += 2
	f.loads++
	f.calls["load"]++
	n := f.calls["load"]
	active, idle, waiting := 3, 1, 0
	longest := 0.5
	s := source.Sample{SessionsActive: &active, SessionsIdleInTransaction: &idle, SessionsWaiting: &waiting, LongestQuerySeconds: &longest}
	if f.noCounters {
		return s, source.Counters{}, f.loadErr
	}
	return s, source.Counters{
		At: f.now(), Queries: -1, BytesScanned: -1,
		Commits: 60 * n, Rollbacks: n, RowsRead: 1000 * n, RowsWritten: 50 * n,
		CacheHits: 900 * n, CacheMisses: 100 * n, Deadlocks: 0, TempBytes: 0,
	}, f.loadErr
}

// resetCounters makes the next reading start from zero, as pg_stat_reset
// or a restart does: the reading after it is below the one before.
func (f *fake) resetCounters() { f.calls["load"] = -1 }

func (f *fake) Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, source.TableCounters, error) {
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
		return wire.DatabaseTable{}, source.TableCounters{}, err
	}
	rows := int64(100 + f.exacts[t.Name])
	// CheckedAt is the source's own clock after the query, 300ms late, as
	// a real query's is; the collector must schedule on its own reading.
	out := wire.DatabaseTable{Name: t.Name, FreshnessColumn: t.FreshnessColumn, Rows: &rows, RowsExact: exact, SizeBytes: 4096, CheckedAt: f.now().Add(300 * time.Millisecond)}
	if err := f.partial[t.Name]; err != nil {
		return out, source.TableCounters{}, fmt.Errorf("%w: %w", postgres.ErrPartial, err)
	}
	if fresh && t.FreshnessColumn != "" {
		at := f.now()
		out.NewestAt = &at
	}
	counters := source.TableCounters{DeadRows: -1, SeqScans: -1, IdxScans: -1, Inserted: -1, Updated: -1, Deleted: -1}
	if f.tableCounters {
		f.calls["table:"+t.Name]++
		n := f.calls["table:"+t.Name]
		counters = source.TableCounters{DeadRows: 5, SeqScans: 6 * n, IdxScans: 600 * n, Inserted: 60 * n, Updated: 6 * n, Deleted: 0}
		d := counters.DeadRows
		out.DeadRows = &d
	}
	counters.Columns = f.columns[t.Name]
	return out, counters, nil
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
	// probe, version, 4 resources, replication, 2 for load, 2 × (table + fresh + exact)
	assert.Equal(t, f.queries, d.Collection.Queries)
	assert.Equal(t, 15, d.Collection.Queries)
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
	// probe, version, 4 resources, replication, 2 for load
	assert.Equal(t, 9, d.Collection.Queries)
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, "18.1", d.ServerVersion)
	assert.Equal(t, 8, d.Collection.Queries)
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

// Load: the sample every interval, the rates from the second interval on.

func loaded(f *fake) {
	f.loadCounters = true
}

func TestCollect_LoadIsSampleFirstThenRates(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.NotNil(t, d.Load)
	assert.Equal(t, 3, *d.Load.SessionsActiveNow)
	assert.Nil(t, d.Load.TransactionsPerSecond)
	assert.Nil(t, d.Load.CacheHitRatio)

	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.NotNil(t, d.Load.TransactionsPerSecond)
	assert.Equal(t, 1.0, *d.Load.TransactionsPerSecond) // the fake commits 60 a minute
	assert.Equal(t, 0.9, *d.Load.CacheHitRatio)
	assert.Nil(t, d.Load.QueriesPerSecond) // the fake has no statement counter
}

func TestCollect_ARestartForgetsTheCounters(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	c := New(database(), static("estimate"), probe(), f, ck.now)
	c.Collect(context.Background())
	ck.advance(time.Minute)
	assert.NotNil(t, c.Collect(context.Background()).Load.TransactionsPerSecond)
	// A new process over the same database: nothing to subtract from.
	fresh := New(database(), static("estimate"), probe(), f, ck.now)
	ck.advance(time.Minute)
	d := fresh.Collect(context.Background())
	assert.NotNil(t, d.Load)
	assert.Nil(t, d.Load.TransactionsPerSecond)
}

func TestCollect_ACounterResetSendsNoRateThatInterval(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	c := New(database(), static("estimate"), probe(), f, ck.now)
	c.Collect(context.Background())
	ck.advance(time.Minute)
	f.resetCounters()
	d := c.Collect(context.Background())
	assert.NotNil(t, d.Load)
	assert.Nil(t, d.Load.TransactionsPerSecond)
	ck.advance(time.Minute)
	assert.NotNil(t, c.Collect(context.Background()).Load.TransactionsPerSecond)
}

func TestCollect_LoadErrorsAreEntriesAndTheSampleStillSends(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	f.loadErr = []wire.DatabaseError{{Error: "pg_stat_database: permission denied"}}
	f.noCounters = true
	c := New(database(), static("estimate"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.NotNil(t, d.Load)
	assert.NotNil(t, d.Load.SessionsActiveNow)
	assert.Equal(t, 1, len(d.Collection.Errors))
	assert.That(t, strings.Contains(d.Collection.Errors[0].Error, "pg_stat_database"))
}

func TestCollect_LoadDisabledSendsNone(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	tables := static("estimate", "public.t")
	c := New(database(), tables, probe(), f, ck.now)
	c.load = config.LoadSection{Enabled: false}
	d := c.Collect(context.Background())
	assert.Nil(t, d.Load)
	assert.Equal(t, 0, f.loads)
	// probe, version, 4 resources, replication, 2 for the table (fresh)
	assert.Equal(t, 9, d.Collection.Queries)
}

func TestCollect_TableRatesFromTableCounters(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tableCounters = true
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Equal(t, int64(5), *d.Tables[0].DeadRows)
	assert.Nil(t, d.Tables[0].SeqScansPerSecond)
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.NotNil(t, d.Tables[0].SeqScansPerSecond)
	assert.Equal(t, 0.1, *d.Tables[0].SeqScansPerSecond) // the fake adds 6 seq scans a call
	assert.Equal(t, 10.0, *d.Tables[0].IndexScansPerSecond)
}

func TestCollect_WidestLoadFitsTheBundle(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	loaded(f)
	f.tableCounters = true
	var names []string
	for i := 0; i < wire.MaxDatabaseTables; i++ {
		names = append(names, fmt.Sprintf("%s.%s_%02d", strings.Repeat("s", 63), strings.Repeat("t", 60), i))
	}
	c := New(database(), static("exact", names...), probe(), f, ck.now)
	c.Collect(context.Background())
	ck.advance(time.Minute)
	d := c.Collect(context.Background())
	raw, err := json.Marshal(wire.Bundle{Database: &d})
	assert.NoError(t, err)
	assert.That(t, len(raw) < 64<<10)
}

// A table's writes are rates from its counters, like its scans: absent on
// the first reading, then inserted, updated and deleted a second.
func TestCollect_TableWritesFromTableCounters(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.tableCounters = true
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	assert.Nil(t, d.Tables[0].RowsInsertedPerSecond)
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 1.0, *d.Tables[0].RowsInsertedPerSecond) // the fake adds 60 inserts a call
	assert.Equal(t, 0.1, *d.Tables[0].RowsUpdatedPerSecond)
	assert.Equal(t, 0.0, *d.Tables[0].RowsDeletedPerSecond)
}

// A table's schema rides as a hash every report; the report where the
// hash moves carries each column's change, and the next carries none.
func TestCollect_SchemaChangeIsReportedOnceWithItsDiff(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.columns["public.t"] = []source.Column{{Name: "id", Type: "integer", NotNull: true}, {Name: "amount", Type: "integer"}, {Name: "legacy_id", Type: "text"}}
	c := New(database(), static("estimate", "public.t"), probe(), f, ck.now)
	d := c.Collect(context.Background())
	first := d.Tables[0].SchemaHash
	assert.Equal(t, 16, len(first))
	assert.Equal(t, 0, len(d.Tables[0].SchemaChanges))

	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, first, d.Tables[0].SchemaHash)
	assert.Equal(t, 0, len(d.Tables[0].SchemaChanges))

	f.columns["public.t"] = []source.Column{{Name: "id", Type: "integer", NotNull: true}, {Name: "amount", Type: "numeric(12,2)", NotNull: true}, {Name: "region", Type: "text"}}
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.That(t, d.Tables[0].SchemaHash != first)
	assert.DeepEqual(t, []wire.DatabaseSchemaChange{
		{Column: "amount", Change: "retyped", From: "integer", To: "numeric(12,2)"},
		{Column: "amount", Change: "nullability", From: "NULL", To: "NOT NULL"},
		{Column: "legacy_id", Change: "dropped", From: "text"},
		{Column: "region", Change: "added", To: "text"},
	}, d.Tables[0].SchemaChanges)

	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 0, len(d.Tables[0].SchemaChanges))
}

// Changes beyond the bundle's cap wait: a table whose changes do not fit
// keeps its old reading and sends them in the next bundle.
func TestCollect_SchemaChangesBeyondTheCapWaitForTheNextBundle(t *testing.T) {
	ck := newClock()
	f := newFake(ck.now)
	f.columns["public.a"] = []source.Column{{Name: "id", Type: "integer", NotNull: true}}
	f.columns["public.b"] = []source.Column{{Name: "id", Type: "integer", NotNull: true}}
	c := New(database(), static("estimate", "public.a", "public.b"), probe(), f, ck.now)
	c.Collect(context.Background())
	for i := 0; i < 50; i++ {
		f.columns["public.a"] = append(f.columns["public.a"], source.Column{Name: fmt.Sprintf("a%d", i), Type: "text"})
		f.columns["public.b"] = append(f.columns["public.b"], source.Column{Name: fmt.Sprintf("b%d", i), Type: "text"})
	}
	ck.advance(time.Minute)
	d := c.Collect(context.Background())
	assert.Equal(t, 50, len(d.Tables[0].SchemaChanges))
	assert.Equal(t, 0, len(d.Tables[1].SchemaChanges))
	ck.advance(time.Minute)
	d = c.Collect(context.Background())
	assert.Equal(t, 0, len(d.Tables[0].SchemaChanges))
	assert.Equal(t, 50, len(d.Tables[1].SchemaChanges))
}
