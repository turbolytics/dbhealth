package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/source"
)

// Load is what people are doing to the database: a sample of
// pg_stat_activity, and one reading of pg_stat_database's counters. The
// two are independent queries; a failure in one is an error entry and the
// other still comes back. Per-query facts are not read: pg_stat_statements
// keeps a utility statement's text verbatim, literal and all.
func (c *Client) Load(ctx context.Context) (source.Sample, source.Counters, []wire.DatabaseError) {
	var s source.Sample
	var errs []wire.DatabaseError

	var active, idleInTx, waiting, hidden int
	var longest float64
	err := c.row(ctx, `SELECT
		count(*) FILTER (WHERE state = 'active'),
		count(*) FILTER (WHERE state = 'idle in transaction'),
		count(*) FILTER (WHERE wait_event_type = 'Lock'),
		COALESCE(EXTRACT(EPOCH FROM now() - min(query_start) FILTER (WHERE state = 'active' AND pid <> pg_backend_pid())), 0)::float8,
		count(*) FILTER (WHERE backend_type IS NULL AND datname IS NOT NULL AND pid <> pg_backend_pid())
		FROM pg_stat_activity WHERE backend_type = 'client backend' OR backend_type IS NULL`,
		nil, &active, &idleInTx, &waiting, &longest, &hidden)
	switch {
	case err != nil:
		errs = append(errs, wire.DatabaseError{Error: "pg_stat_activity: " + sqlError(err)})
	case hidden > 0:
		// Another role's sessions, their state hidden: a sample of what
		// this role can see would be passed off as the whole. Resources
		// already names the grant; this is the same fact.
		errs = append(errs, wire.DatabaseError{Error: fmt.Sprintf(
			"pg_stat_activity: %d other sessions hidden from this role; grant pg_read_all_stats", hidden)})
	default:
		s.SessionsActive, s.SessionsIdleInTransaction, s.SessionsWaiting = &active, &idleInTx, &waiting
		s.LongestQuerySeconds = &longest
	}

	// -1 where Postgres has no counter: statements (without the extension)
	// and bytes scanned.
	counters := source.Counters{Queries: -1, BytesScanned: -1}
	var rowsRead, rowsWritten int64
	err = c.row(ctx, `SELECT xact_commit, xact_rollback,
		tup_returned + tup_fetched, tup_inserted + tup_updated + tup_deleted,
		blks_hit, blks_read, deadlocks, temp_bytes
		FROM pg_stat_database WHERE datname = current_database()`, nil,
		&counters.Commits, &counters.Rollbacks, &rowsRead, &rowsWritten,
		&counters.CacheHits, &counters.CacheMisses, &counters.Deadlocks, &counters.TempBytes)
	if err != nil {
		errs = append(errs, wire.DatabaseError{Error: "pg_stat_database: " + sqlError(err)})
		return s, source.Counters{}, errs
	}
	counters.RowsRead, counters.RowsWritten = rowsRead, rowsWritten
	counters.At = time.Now().UTC()
	return s, counters, errs
}
