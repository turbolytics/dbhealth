// Package source is the contract between a database kind and the
// collector: the questions one interval asks. Postgres is the one kind in
// v1; the collector knows only this interface.
package source

import (
	"context"
	"errors"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/config"
)

// Counters is one reading of the system's cumulative counters, the
// collector's input for rates: a count since the stats began, or -1 when
// the kind does not have it (Postgres has no Queries without
// pg_stat_statements, which is not read). At is the reading's own clock.
type Counters struct {
	At                          time.Time
	Queries, Commits, Rollbacks int64
	RowsRead, RowsWritten       int64
	BytesScanned                int64
	CacheHits, CacheMisses      int64
	Deadlocks, TempBytes        int64
}

// Sample is the instant of collection: what is true now. A field the kind
// or the role cannot see is nil.
type Sample struct {
	SessionsActive            *int
	SessionsIdleInTransaction *int
	SessionsWaiting           *int
	QueriesQueued             *int
	LongestQuerySeconds       *float64
}

// TableCounters is a table's cumulative counters, read beside its row; -1
// when the kind does not have one.
type TableCounters struct {
	DeadRows, SeqScans, IdxScans int64
	// Inserted, Updated and Deleted are the system's write counters for
	// the table; -1 where the kind has none.
	Inserted, Updated, Deleted int64
	// Columns is the table's schema as read: each column in the system's
	// order, with its type as the system names it. The collector hashes
	// it and diffs it against the last reading; nil when it was not read.
	Columns []Column
}

// Column is one column of a table's schema.
type Column struct {
	Name, Type string
	NotNull    bool
}

// ErrPartial wraps a Table error when the row returned beside it carries
// the facts that were read before the failure: a table whose size and
// estimate came back, but whose freshness column does not exist, is
// reported with those and the error, not dropped.
var ErrPartial = errors.New("partial")

// Source is what the collector asks.
type Source interface {
	// Probe never returns an error: a failure is a probe with OK false.
	Probe(ctx context.Context, timeout time.Duration) wire.DatabaseProbe
	Version(ctx context.Context) (string, error)
	// Resources runs its queries independently; each failure is an entry.
	Resources(ctx context.Context) (*wire.DatabaseResources, []wire.DatabaseError)
	// Discover returns the tables kept, in name order, and how many
	// MaxTables dropped.
	Discover(ctx context.Context, d config.Discover) ([]config.StaticTable, int, error)
	// Table reads one table: size and estimate always, max(freshness
	// column) when fresh, count(*) when exact, and its counters. An error
	// wrapping ErrPartial comes with a row worth sending.
	Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, TableCounters, error)
	// Load is the instant's sample and the counters' reading, each query
	// its own failure in the errors. Per-query facts are not read: a
	// statement store's text can carry a literal, and dbhealth does not
	// promise to strip one.
	Load(ctx context.Context) (Sample, Counters, []wire.DatabaseError)
	// Replication is nil, nil on a primary with no replicas.
	Replication(ctx context.Context) (*wire.DatabaseReplication, error)
	// Queries is the running count of round trips.
	Queries() int
}
