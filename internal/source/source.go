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
	// column) when fresh, count(*) when exact. An error wrapping
	// ErrPartial comes with a row worth sending.
	Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, error)
	// Replication is nil, nil on a primary with no replicas.
	Replication(ctx context.Context) (*wire.DatabaseReplication, error)
	// Queries is the running count of round trips.
	Queries() int
}
