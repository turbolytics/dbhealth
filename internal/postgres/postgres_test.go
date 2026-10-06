package postgres

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/zeebo/assert"

	"github.com/turbolytics/dbhealth/internal/collector"
	"github.com/turbolytics/dbhealth/internal/config"
)

// The collector asks a Client these questions.
var _ collector.Source = (*Client)(nil)

// One postgres:18 for the package, with the schema the tests read:
//
//	public.events(id, created_at)  1,000 rows
//	public.blobs(id, data)         no timestamp column
//	audit.log(ts)                  a second schema
//	role reader                    CONNECT and SELECT, no pg_read_all_stats
var (
	adminDSN   string
	readerDSN  string
	scratchDSN string
	newestAt   time.Time
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	ctx := context.Background()
	pg, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("app"), tcpostgres.WithUsername("admin"), tcpostgres.WithPassword("admin"),
		tcpostgres.BasicWaitStrategies())
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres container:", err)
		os.Exit(1)
	}
	adminDSN, err = pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	readerDSN = strings.Replace(adminDSN, "admin:admin@", "reader:reader@", 1)
	// pg_stat_reset() is per database and blanks every table's estimate
	// with it; the test that calls it gets a database of its own.
	scratchDSN = strings.Replace(adminDSN, "/app?", "/scratch?", 1)
	if err := seed(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "seed:", err)
		os.Exit(1)
	}
	code := m.Run()
	_ = pg.Terminate(ctx)
	os.Exit(code)
}

func seed(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	newestAt = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	for _, q := range []string{
		`CREATE TABLE public.events (id serial PRIMARY KEY, created_at timestamptz NOT NULL)`,
		`INSERT INTO public.events (created_at) SELECT '2026-10-05T12:00:00Z'::timestamptz - (g || ' seconds')::interval FROM generate_series(0, 999) g`,
		`CREATE TABLE public.blobs (id serial PRIMARY KEY, data bytea)`,
		`INSERT INTO public.blobs (data) VALUES ('\x00'), ('\x01')`,
		`CREATE SCHEMA audit`,
		`CREATE TABLE audit.log (ts timestamptz)`,
		// The insert's counts are pending in this backend until a flush;
		// vacuum then counts the heap itself, so the two must not add up.
		`SELECT pg_stat_force_next_flush()`,
		`VACUUM ANALYZE`,
		`CREATE DATABASE scratch`,
		`CREATE ROLE reader LOGIN PASSWORD 'reader'`,
		`GRANT CONNECT ON DATABASE app TO reader`,
		`GRANT USAGE ON SCHEMA public, audit TO reader`,
		`GRANT SELECT ON ALL TABLES IN SCHEMA public, audit TO reader`,
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("%s: %w", q, err)
		}
	}
	return nil
}

func open(t *testing.T, dsn string) *Client {
	t.Helper()
	if testing.Short() {
		t.Skip("needs the postgres container")
	}
	c, err := Open(context.Background(), dsn, 5*time.Second)
	assert.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func TestProbe_OKAndTimed(t *testing.T) {
	c := open(t, adminDSN)
	p := c.Probe(context.Background(), time.Second)
	assert.True(t, p.OK)
	assert.That(t, p.LatencyMs >= 0)
	assert.NotNil(t, p.LastOKAt)
	assert.Equal(t, "", p.Error)
}

func TestProbe_RefusedIsAClass(t *testing.T) {
	if testing.Short() {
		t.Skip("needs the postgres container")
	}
	c, err := Open(context.Background(), "postgres://u:p@127.0.0.1:1/d?sslmode=disable", time.Second)
	assert.NoError(t, err) // a pool opens lazily; the probe is what fails
	defer c.Close()
	p := c.Probe(context.Background(), time.Second)
	assert.False(t, p.OK)
	assert.Equal(t, "refused", p.Error)
	assert.Nil(t, p.LastOKAt)
}

func TestProbe_TimeoutIsAClass(t *testing.T) {
	c := open(t, adminDSN)
	c.probeSQL = "SELECT pg_sleep(2)"
	p := c.Probe(context.Background(), 100*time.Millisecond)
	assert.False(t, p.OK)
	assert.Equal(t, "timeout", p.Error)
}

func TestProbe_AuthIsAClass(t *testing.T) {
	if testing.Short() {
		t.Skip("needs the postgres container")
	}
	c, err := Open(context.Background(), strings.Replace(adminDSN, "admin:admin@", "admin:wrong@", 1), time.Second)
	assert.NoError(t, err)
	defer c.Close()
	p := c.Probe(context.Background(), 2*time.Second)
	assert.False(t, p.OK)
	assert.Equal(t, "auth", p.Error)
}

func TestVersion(t *testing.T) {
	c := open(t, adminDSN)
	v, err := c.Version(context.Background())
	assert.NoError(t, err)
	assert.That(t, strings.HasPrefix(v, "18"))
}

func TestResources_AllFields(t *testing.T) {
	c := open(t, adminDSN)
	r, errs := c.Resources(context.Background())
	assert.Equal(t, 0, len(errs))
	assert.NotNil(t, r.Connections)
	assert.Equal(t, 100, r.Connections.Max)
	assert.That(t, r.Connections.Used >= 1)
	assert.NotNil(t, r.SizeBytes)
	assert.That(t, *r.SizeBytes > 0)
	assert.NotNil(t, r.OldestTransactionSeconds)
	assert.NotNil(t, r.Memory)
	assert.That(t, r.Memory.SharedBuffersBytes > 0)
}

// The review-focus case: a role without pg_read_all_stats sees other
// sessions' rows in pg_stat_activity with their type, state, wait event
// and transaction start hidden. A count of what it can see would be a
// small number passed off as true, so connections and the oldest
// transaction are withheld and the error says how many sessions are
// hidden and what to grant. Size and memory still come back.
func TestResources_ReaderRoleIsToldWhatIsHidden(t *testing.T) {
	admin := open(t, adminDSN)
	// Another session, held open in a transaction, that the reader cannot see into.
	tx, err := admin.pool.Begin(context.Background())
	assert.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = tx.Exec(context.Background(), "SELECT 1")
	assert.NoError(t, err)

	c := open(t, readerDSN)
	r, errs := c.Resources(context.Background())
	assert.Nil(t, r.Connections)
	assert.Nil(t, r.OldestTransactionSeconds)
	assert.NotNil(t, r.SizeBytes)
	assert.Equal(t, 1, len(errs))
	assert.Equal(t, "", errs[0].Table)
	assert.That(t, strings.Contains(errs[0].Error, "pg_stat_activity"))
	assert.That(t, strings.Contains(errs[0].Error, "pg_read_all_stats"))
}

func discover(t *testing.T, c *Client, d config.Discover) ([]config.StaticTable, int) {
	t.Helper()
	if len(d.FreshnessColumns) == 0 {
		d.FreshnessColumns = config.DefaultFreshnessColumns
	}
	if d.MaxTables == 0 {
		d.MaxTables = 50
	}
	tables, dropped, err := c.Discover(context.Background(), d)
	assert.NoError(t, err)
	return tables, dropped
}

func byName(tables []config.StaticTable) map[string]config.StaticTable {
	m := map[string]config.StaticTable{}
	for _, t := range tables {
		m[t.Name] = t
	}
	return m
}

func TestDiscover_FirstMatchingColumnWins(t *testing.T) {
	c := open(t, adminDSN)
	tables, _ := discover(t, c, config.Discover{Schemas: []string{"public"}})
	assert.Equal(t, "created_at", byName(tables)["public.events"].FreshnessColumn)

	tables, _ = discover(t, c, config.Discover{Schemas: []string{"public"}, FreshnessColumns: []string{"ts", "created_at"}})
	assert.Equal(t, "created_at", byName(tables)["public.events"].FreshnessColumn)

	tables, _ = discover(t, c, config.Discover{Schemas: []string{"public", "audit"}})
	assert.Equal(t, "ts", byName(tables)["audit.log"].FreshnessColumn)
	assert.Equal(t, 3, len(tables))
}

func TestDiscover_DefaultSchemasAreEveryoneButTheCatalog(t *testing.T) {
	c := open(t, adminDSN)
	tables, _ := discover(t, c, config.Discover{})
	m := byName(tables)
	_, events := m["public.events"]
	_, log := m["audit.log"]
	assert.True(t, events)
	assert.True(t, log)
	for name := range m {
		assert.False(t, strings.HasPrefix(name, "pg_catalog.") || strings.HasPrefix(name, "information_schema."))
	}
}

func TestDiscover_TableWithoutATimestampIsStillWatched(t *testing.T) {
	c := open(t, adminDSN)
	tables, _ := discover(t, c, config.Discover{Schemas: []string{"public"}})
	blobs, ok := byName(tables)["public.blobs"]
	assert.True(t, ok)
	assert.Equal(t, "", blobs.FreshnessColumn)
}

func TestDiscover_ExcludeAndMaxTables(t *testing.T) {
	c := open(t, adminDSN)
	tables, dropped := discover(t, c, config.Discover{Schemas: []string{"public"}, Exclude: []string{"blo*"}})
	_, blobs := byName(tables)["public.blobs"]
	assert.False(t, blobs)
	assert.Equal(t, 0, dropped)

	tables, dropped = discover(t, c, config.Discover{Schemas: []string{"public"}, MaxTables: 1})
	assert.Equal(t, 1, len(tables))
	assert.Equal(t, 1, dropped)
}

func TestTable_EstimateThenExact(t *testing.T) {
	c := open(t, adminDSN)
	events := config.StaticTable{Name: "public.events", FreshnessColumn: "created_at"}

	est, _, err := c.Table(context.Background(), events, true, false)
	assert.NoError(t, err)
	assert.Equal(t, "public.events", est.Name)
	assert.NotNil(t, est.Rows)
	assert.That(t, *est.Rows > 800 && *est.Rows < 1200)
	assert.False(t, est.RowsExact)
	assert.That(t, est.SizeBytes > 0)
	assert.NotNil(t, est.NewestAt)
	assert.True(t, est.NewestAt.Equal(newestAt))
	assert.False(t, est.CheckedAt.IsZero())

	exact, _, err := c.Table(context.Background(), events, false, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000), *exact.Rows)
	assert.True(t, exact.RowsExact)
	assert.Nil(t, exact.NewestAt)
}

func TestTable_WithoutASchemaIsPublic(t *testing.T) {
	c := open(t, adminDSN)
	got, _, err := c.Table(context.Background(), config.StaticTable{Name: "blobs"}, true, true)
	assert.NoError(t, err)
	assert.Equal(t, "public.blobs", got.Name)
	assert.Equal(t, int64(2), *got.Rows)
	assert.Nil(t, got.NewestAt)
}

func TestTable_DroppedTableIsAnError(t *testing.T) {
	c := open(t, adminDSN)
	_, err := c.pool.Exec(context.Background(), `CREATE TABLE public.gone (id int)`)
	assert.NoError(t, err)
	_, err = c.pool.Exec(context.Background(), `DROP TABLE public.gone`)
	assert.NoError(t, err)
	_, _, err = c.Table(context.Background(), config.StaticTable{Name: "public.gone"}, true, true)
	assert.Error(t, err)
	assert.That(t, strings.Contains(err.Error(), "public.gone"))
}

func TestReplication_PrimaryWithoutReplicasIsAbsent(t *testing.T) {
	c := open(t, adminDSN)
	r, err := c.Replication(context.Background())
	assert.NoError(t, err)
	assert.Nil(t, r)
}

func TestQueries_CountEveryRoundTrip(t *testing.T) {
	c := open(t, adminDSN)
	before := c.Queries()
	c.Probe(context.Background(), time.Second)
	assert.Equal(t, before+1, c.Queries())
	c.Resources(context.Background())
	assert.Equal(t, before+5, c.Queries())
}

// Findings from the review of #4.

func TestVersion_ErrorNamesNoHostOrUser(t *testing.T) {
	if testing.Short() {
		t.Skip("needs the postgres container")
	}
	c, err := Open(context.Background(), "postgres://secretuser:hunter2@127.0.0.1:1/mydb?sslmode=disable", time.Second)
	assert.NoError(t, err)
	defer c.Close()
	_, err = c.Version(context.Background())
	assert.Error(t, err)
	for _, leak := range []string{"secretuser", "hunter2", "mydb", "127.0.0.1"} {
		if strings.Contains(err.Error(), leak) {
			t.Fatalf("the error carries %q: %q", leak, err)
		}
	}
}

func TestTable_ABrokenFreshnessColumnStillReportsTheTable(t *testing.T) {
	c := open(t, adminDSN)
	row, _, err := c.Table(context.Background(), config.StaticTable{Name: "public.events", FreshnessColumn: "nope"}, true, false)
	assert.True(t, errors.Is(err, ErrPartial))
	assert.That(t, strings.Contains(err.Error(), "nope"))
	assert.Equal(t, "public.events", row.Name)
	assert.That(t, row.SizeBytes > 0)
	assert.NotNil(t, row.Rows)
	assert.Nil(t, row.NewestAt)
}

func TestReplication_StateColumnMayBeNull(t *testing.T) {
	// A role without pg_read_all_stats sees pg_stat_replication rows with
	// state and lag NULL. The scan must take them.
	c := open(t, adminDSN)
	row := c.pool.QueryRow(context.Background(), `SELECT 'r1'::text, NULL::text, NULL::float8`)
	r, err := scanReplica(row)
	assert.NoError(t, err)
	assert.Equal(t, "r1", r.Name)
	assert.Equal(t, "", r.State)
	assert.Nil(t, r.LagSeconds)
}
