// Package collector runs one interval for one database: probe, then
// resources, replication and the tables that are due, into one
// wire.Database. It never fails: a failed probe sends probe alone, and a
// failed query is an entry in collection.errors beside the others' facts.
package collector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/source"
)

// Source is what the collector asks. *postgres.Client is one.
type Source = source.Source

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
	dropped    int
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

// Collect is one interval. Everything it schedules is against the clock
// read here, once, so a slow query cannot drift the next check.
func (c *Collector) Collect(ctx context.Context) wire.Database {
	start := time.Now()
	now := c.now()
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
		c.collectFacts(ctx, now, &d)
	}
	d.Collection.Queries = c.src.Queries() - queries
	d.Collection.DurationMs = time.Since(start).Milliseconds()
	if len(d.Collection.Errors) > wire.MaxDatabaseErrors {
		d.Collection.Errors = d.Collection.Errors[:wire.MaxDatabaseErrors]
	}
	return d
}

func (c *Collector) collectFacts(ctx context.Context, now time.Time, d *wire.Database) {
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

	watched := c.watched(ctx, now, fail)
	seen := make(map[string]bool, len(watched))
	for _, t := range watched {
		seen[t.Name] = true
		st := c.last[t.Name]
		if st == nil {
			st = &tableState{}
			c.last[t.Name] = st
		}
		exact := c.tables.Rows == "exact"
		if t.Rows != "" {
			exact = t.Rows == "exact"
		}
		fresh := t.FreshnessColumn != "" && c.due(st.freshAt, now, c.tables.FreshnessIntervalSeconds)
		exactNow := exact && c.due(st.exactAt, now, c.tables.RowsExactIntervalSeconds)
		row, err := c.src.Table(ctx, t, fresh, exactNow)
		if err != nil {
			fail(t.Name, err)
			if !errors.Is(err, source.ErrPartial) {
				continue
			}
		}
		row.CheckedAt = now
		// A check that was attempted is a check that was due, whatever it
		// returned: a broken column is retried on its interval, not every
		// probe.
		if fresh {
			st.newestAt, st.freshAt = row.NewestAt, now
		} else {
			row.NewestAt = st.newestAt
		}
		if exactNow && row.RowsExact {
			st.exactRows, st.exactAt = row.Rows, now
		} else if exactNow {
			st.exactAt = now
		}
		if exact && !row.RowsExact && st.exactRows != nil {
			// Between exact counts the bundle carries the last one, with
			// the time it was taken, rather than an estimate.
			row.Rows, row.RowsExact, row.CheckedAt = st.exactRows, true, st.exactAt
		}
		d.Tables = append(d.Tables, row)
	}
	for name := range c.last {
		if !seen[name] {
			delete(c.last, name)
		}
	}
	if len(d.Tables) > wire.MaxDatabaseTables {
		fail("", fmt.Errorf("%d tables beyond the %d the bundle carries were not sent", len(d.Tables)-wire.MaxDatabaseTables, wire.MaxDatabaseTables))
		d.Tables = d.Tables[:wire.MaxDatabaseTables]
	}
}

// watched is the tables this interval looks at: the static list, or the
// last discovery, rerun every rediscoverEvery freshness intervals.
func (c *Collector) watched(ctx context.Context, now time.Time, fail func(string, error)) []config.StaticTable {
	if c.tables.Discover == nil {
		return c.tables.Static
	}
	if c.due(c.discoverAt, now, rediscoverEvery*c.tables.FreshnessIntervalSeconds) {
		found, dropped, err := c.src.Discover(ctx, *c.tables.Discover)
		if err != nil {
			fail("", err)
		} else {
			c.discovered, c.discoverAt, c.dropped = found, now, dropped
		}
	}
	if c.dropped > 0 {
		fail("", fmt.Errorf("discovery: %d tables beyond max_tables (%d) are not watched", c.dropped, c.tables.Discover.MaxTables))
	}
	return c.discovered
}

// due says whether a check last made at last is owed at now, on an
// interval of seconds. A zero last is owed. The reporter jitters its
// interval by up to ten percent, so a check is owed from half a probe
// interval early: at 60s intervals, a tick at 58s is this minute's.
func (c *Collector) due(last, now time.Time, seconds int) bool {
	if last.IsZero() {
		return true
	}
	slack := time.Duration(c.probe.IntervalSeconds) * time.Second / 2
	return !now.Before(last.Add(time.Duration(seconds)*time.Second - slack))
}
