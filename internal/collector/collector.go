// Package collector runs one interval for one database: probe, then
// resources, replication and the tables that are due, into one
// wire.Database. It never fails: a failed probe sends probe alone, and a
// failed query is an entry in collection.errors beside the others' facts.
package collector

import (
	"context"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/config"
)

// Source is what the collector asks. *postgres.Client is one.
type Source interface {
	Probe(ctx context.Context, timeout time.Duration) wire.DatabaseProbe
	Version(ctx context.Context) (string, error)
	Resources(ctx context.Context) (*wire.DatabaseResources, []wire.DatabaseError)
	Discover(ctx context.Context, d config.Discover) ([]config.StaticTable, int, error)
	Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, error)
	Replication(ctx context.Context) (*wire.DatabaseReplication, error)
	// Queries is the source's running count of round trips.
	Queries() int
}

// rediscoverEvery is how many freshness intervals pass between one
// discovery and the next.
const rediscoverEvery = 10

// Collector holds what carries from one interval to the next: the version,
// the discovered tables, each table's last freshness and exact count, and
// the probe's streak.
type Collector struct {
	db     config.Database
	tables config.Tables
	probe  config.Probe
	src    Source
	now    func() time.Time
	target string

	version    string
	discovered []config.StaticTable
	discoverAt time.Time
	last       map[string]*tableState
	failures   int
	lastOKAt   *time.Time
}

// tableState is what a table's last checks said, carried into the bundles
// between checks.
type tableState struct {
	newestAt  *time.Time
	freshAt   time.Time
	exactRows *int64
	exactAt   time.Time
}

// New prepares a collector for one database. The target the bundle
// carries is the DSN redacted to host:port/database.
func New(db config.Database, tables config.Tables, probe config.Probe, src Source, now func() time.Time) *Collector {
	target, err := config.RedactedTarget(db.DSN)
	if err != nil {
		target = db.Name
	}
	return &Collector{db: db, tables: tables, probe: probe, src: src, now: now, target: target, last: map[string]*tableState{}}
}

// Collect is one interval.
func (c *Collector) Collect(ctx context.Context) wire.Database {
	start := time.Now()
	queries := c.src.Queries()
	d := wire.Database{
		Kind:          c.db.Kind,
		Target:        c.target,
		Cluster:       c.db.Cluster,
		ServerVersion: c.version,
		Collection:    wire.DatabaseCollection{Errors: []wire.DatabaseError{}},
	}
	d.Probe = c.src.Probe(ctx, time.Duration(c.probe.TimeoutSeconds)*time.Second)
	if d.Probe.OK {
		c.failures = 0
		c.lastOKAt = d.Probe.LastOKAt
	} else {
		c.failures++
	}
	d.Probe.ConsecutiveFailures = c.failures
	d.Probe.LastOKAt = c.lastOKAt
	if d.Probe.OK {
		c.collectFacts(ctx, &d)
	}
	d.Collection.Queries = c.src.Queries() - queries
	d.Collection.DurationMs = time.Since(start).Milliseconds()
	if len(d.Collection.Errors) > wire.MaxDatabaseErrors {
		d.Collection.Errors = d.Collection.Errors[:wire.MaxDatabaseErrors]
	}
	return d
}

func (c *Collector) collectFacts(ctx context.Context, d *wire.Database) {
	fail := func(table string, err error) {
		d.Collection.Errors = append(d.Collection.Errors, wire.DatabaseError{Table: table, Error: err.Error()})
	}
	if c.version == "" {
		v, err := c.src.Version(ctx)
		if err != nil {
			fail("", err)
		} else {
			c.version = v
		}
	}
	d.ServerVersion = c.version

	var errs []wire.DatabaseError
	d.Resources, errs = c.src.Resources(ctx)
	d.Collection.Errors = append(d.Collection.Errors, errs...)

	rep, err := c.src.Replication(ctx)
	if err != nil {
		fail("", err)
	}
	d.Replication = rep

	now := c.now()
	for _, t := range c.watched(ctx, now, fail) {
		st := c.last[t.Name]
		if st == nil {
			st = &tableState{}
			c.last[t.Name] = st
		}
		exact := c.tables.Rows == "exact"
		if t.Rows != "" {
			exact = t.Rows == "exact"
		}
		fresh := t.FreshnessColumn != "" && due(st.freshAt, now, c.tables.FreshnessIntervalSeconds)
		exactNow := exact && due(st.exactAt, now, c.tables.RowsExactIntervalSeconds)
		row, err := c.src.Table(ctx, t, fresh, exactNow)
		if err != nil {
			fail(t.Name, err)
			continue
		}
		if fresh {
			st.newestAt, st.freshAt = row.NewestAt, now
		} else {
			row.NewestAt = st.newestAt
		}
		if exactNow {
			st.exactRows, st.exactAt = row.Rows, row.CheckedAt
		} else if exact && st.exactRows != nil {
			// Between exact counts the bundle carries the last one, with
			// the time it was taken, rather than an estimate.
			row.Rows, row.RowsExact, row.CheckedAt = st.exactRows, true, st.exactAt
		}
		d.Tables = append(d.Tables, row)
	}
	if len(d.Tables) > wire.MaxDatabaseTables {
		d.Tables = d.Tables[:wire.MaxDatabaseTables]
	}
}

// watched is the tables this interval looks at: the static list, or the
// last discovery, rerun every rediscoverEvery freshness intervals.
func (c *Collector) watched(ctx context.Context, now time.Time, fail func(string, error)) []config.StaticTable {
	if c.tables.Discover == nil {
		return c.tables.Static
	}
	if due(c.discoverAt, now, rediscoverEvery*c.tables.FreshnessIntervalSeconds) {
		found, _, err := c.src.Discover(ctx, *c.tables.Discover)
		if err != nil {
			fail("", err)
		} else {
			c.discovered, c.discoverAt = found, now
		}
	}
	return c.discovered
}

// due says whether a check last made at last is owed at now, on an
// interval of seconds. A zero last is owed.
func due(last, now time.Time, seconds int) bool {
	return last.IsZero() || !now.Before(last.Add(time.Duration(seconds)*time.Second))
}
