// Package postgres asks one Postgres endpoint the questions the database
// section answers: is it serving, how close is it to its limits, how
// current are its tables, how far behind is its replication. Each is its
// own query, so one that fails never hides the others, and every query is
// counted.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/source"
)

// The collector asks a Client these questions.
var _ source.Source = (*Client)(nil)

// ErrPartial is source.ErrPartial: Table returned a row worth sending
// beside its error.
var ErrPartial = source.ErrPartial

// Client is a pool of two connections to one endpoint: one for the probe,
// one for everything else, so a slow count(*) never delays the probe.
type Client struct {
	pool    *pgxpool.Pool
	timeout time.Duration
	queries atomic.Int64

	probeSQL string // SELECT 1; a test swaps in a slow one
}

// Open prepares the pool. It does not connect: the first probe does, so
// an endpoint that is down opens fine and reports as not serving.
func Open(ctx context.Context, dsn string, timeout time.Duration) (*Client, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// pgx's error can quote the DSN; this one does not.
		return nil, errors.New("the dsn did not parse")
	}
	cfg.MaxConns = 2
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = timeout
	// Every statement is bounded by the config's timeout on the server
	// side too, so a hung query cannot hold the connection past it.
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", timeout.Milliseconds())
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("the pool did not open")
	}
	return &Client{pool: pool, timeout: timeout, probeSQL: "SELECT 1"}, nil
}

// Close releases the connections.
func (c *Client) Close() { c.pool.Close() }

// Queries is how many round trips this client has made, for the cost
// line in the bundle.
func (c *Client) Queries() int { return int(c.queries.Load()) }

func (c *Client) bounded(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// row runs one query that returns one row and scans it into dest.
func (c *Client) row(ctx context.Context, sql string, args []any, dest ...any) error {
	c.queries.Add(1)
	ctx, cancel := c.bounded(ctx)
	defer cancel()
	return c.pool.QueryRow(ctx, sql, args...).Scan(dest...)
}

// Probe is one round trip, timed. It never returns an error: a failure is
// a probe that is not OK, with the class of what went wrong.
func (c *Client) Probe(ctx context.Context, timeout time.Duration) wire.DatabaseProbe {
	c.queries.Add(1)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	start := time.Now()
	var one int
	err := c.pool.QueryRow(ctx, c.probeSQL).Scan(&one)
	latency := time.Since(start).Milliseconds()
	if err != nil {
		return wire.DatabaseProbe{OK: false, LatencyMs: latency, Error: classify(err)}
	}
	at := time.Now().UTC()
	return wire.DatabaseProbe{OK: true, LatencyMs: latency, LastOKAt: &at}
}

// classify names the kind of probe failure: refused, timeout, auth or
// other. The text of the error is not sent; it can carry the host.
func classify(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &pgErr) && strings.HasPrefix(pgErr.Code, "28"):
		return "auth"
	case errors.As(err, &pgErr) && pgErr.Code == "57014": // query_canceled: statement_timeout
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	var opErr *net.OpError
	var dnsErr *net.DNSError
	if errors.As(err, &opErr) || errors.As(err, &dnsErr) {
		return "refused"
	}
	s := err.Error()
	if strings.Contains(s, "connection refused") || strings.Contains(s, "no such host") || strings.Contains(s, "failed to connect") {
		return "refused"
	}
	return "other"
}

// Version is the server's version string, "18.1".
func (c *Client) Version(ctx context.Context) (string, error) {
	var v string
	if err := c.row(ctx, "SHOW server_version", nil, &v); err != nil {
		return "", errors.New("server_version: " + sqlError(err))
	}
	return v, nil
}

// Resources is how close the endpoint is to its limits. Each of the four
// queries runs on its own; a failure is an error naming the view and the
// other fields still come back.
func (c *Client) Resources(ctx context.Context) (*wire.DatabaseResources, []wire.DatabaseError) {
	var r wire.DatabaseResources
	var errs []wire.DatabaseError
	fail := func(view string, err error) {
		errs = append(errs, wire.DatabaseError{Error: view + ": " + sqlError(err)})
	}

	var used, max, waiting, hidden int
	err := c.row(ctx, `SELECT
		count(*) FILTER (WHERE backend_type = 'client backend'),
		current_setting('max_connections')::int,
		count(*) FILTER (WHERE wait_event_type = 'Lock'),
		count(*) FILTER (WHERE backend_type IS NULL AND datname IS NOT NULL AND pid <> pg_backend_pid())
		FROM pg_stat_activity`, nil, &used, &max, &waiting, &hidden)
	switch {
	case err != nil:
		fail("pg_stat_activity", err)
	case hidden > 0:
		// Another role's sessions show as rows with their type, state,
		// wait event and transaction start as NULL. A count of what this
		// role can see would be a small number passed off as true, so
		// connections and the oldest transaction are withheld and the
		// error says what to grant.
		errs = append(errs, wire.DatabaseError{Error: fmt.Sprintf(
			"pg_stat_activity: %d other sessions hidden from this role; grant pg_read_all_stats", hidden)})
	default:
		r.Connections = &wire.DatabaseConnections{Used: used, Max: max, Waiting: waiting}
	}

	var size int64
	if err := c.row(ctx, `SELECT pg_database_size(current_database())`, nil, &size); err != nil {
		fail("pg_database_size", err)
	} else {
		r.SizeBytes = &size
	}

	var oldest int64
	if hidden == 0 {
		if err := c.row(ctx, `SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(xact_start))::bigint, 0)
			FROM pg_stat_activity WHERE xact_start IS NOT NULL AND backend_type = 'client backend'`, nil, &oldest); err != nil {
			fail("pg_stat_activity.xact_start", err)
		} else {
			r.OldestTransactionSeconds = &oldest
		}
	}

	var shared int64
	if err := c.row(ctx, `SELECT pg_size_bytes(current_setting('shared_buffers'))`, nil, &shared); err != nil {
		fail("shared_buffers", err)
	} else {
		r.Memory = &wire.DatabaseMemory{SharedBuffersBytes: shared}
	}
	return &r, errs
}

// Discover lists the tables in the config's schemas, each with the first
// of the config's freshness columns it has, in name order. It returns the
// tables kept and how many MaxTables dropped. Exclude is globs on the
// table name, matched in Go.
func (c *Client) Discover(ctx context.Context, d config.Discover) ([]config.StaticTable, int, error) {
	c.queries.Add(1)
	ctx, cancel := c.bounded(ctx)
	defer cancel()
	args := []any{d.FreshnessColumns}
	schemaFilter := `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg_toast%'`
	if len(d.Schemas) > 0 {
		schemaFilter = `n.nspname = ANY($2)`
		args = append(args, d.Schemas)
	}
	rows, err := c.pool.Query(ctx, `
		WITH want AS (SELECT col, ord FROM unnest($1::text[]) WITH ORDINALITY AS w(col, ord))
		SELECT DISTINCT ON (n.nspname, c.relname) n.nspname, c.relname, w.col
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN information_schema.columns ic
		  ON ic.table_schema = n.nspname AND ic.table_name = c.relname AND ic.data_type LIKE 'timestamp%'
		LEFT JOIN want w ON w.col = ic.column_name
		WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition AND `+schemaFilter+`
		ORDER BY n.nspname, c.relname, w.ord NULLS LAST`, args...)
	if err != nil {
		return nil, 0, errors.New("discovery: " + sqlError(err))
	}
	defer rows.Close()
	var out []config.StaticTable
	for rows.Next() {
		var schema, table string
		var col *string
		if err := rows.Scan(&schema, &table, &col); err != nil {
			return nil, 0, errors.New("discovery: " + sqlError(err))
		}
		if excluded(table, d.Exclude) {
			continue
		}
		t := config.StaticTable{Name: schema + "." + table}
		if col != nil {
			t.FreshnessColumn = *col
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.New("discovery: " + sqlError(err))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	dropped := 0
	if d.MaxTables > 0 && len(out) > d.MaxTables {
		dropped = len(out) - d.MaxTables
		out = out[:d.MaxTables]
	}
	return out, dropped, nil
}

func excluded(table string, globs []string) bool {
	for _, g := range globs {
		if ok, _ := path.Match(g, table); ok {
			return true
		}
	}
	return false
}

// Table is one table's size, row count, dead rows and scan counters and,
// when asked, its newest timestamp and an exact count. The estimate is
// pg_stat_user_tables' n_live_tup; exact is count(*). A table that is
// gone is an error naming it.
func (c *Client) Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, source.TableCounters, error) {
	schema, table := splitName(t.Name)
	name := schema + "." + table
	out := wire.DatabaseTable{Name: name, FreshnessColumn: t.FreshnessColumn}
	counters := source.TableCounters{DeadRows: -1, SeqScans: -1, IdxScans: -1, Inserted: -1, Updated: -1, Deleted: -1}

	var live, dead, seq, idx, ins, upd, del *int64
	var size int64
	var vacuum *time.Time
	var columns *string
	// The columns ride in the same read: name, type as Postgres names it
	// and NOT NULL, in attribute order, unit- and record-separated.
	//
	// A partitioned table holds no rows; its partitions do. Every number
	// is summed over the table's partition tree. pg_partition_tree lists
	// nothing for a table that is not partitioned, so the table joins its
	// own tree, and UNION drops it where the tree already lists it. The
	// newest vacuum is the newest of any partition.
	err := c.row(ctx, `SELECT st.live, COALESCE(st.size, 0), st.vacuum,
		st.dead, st.seq, st.idx, st.ins, st.upd, st.del,
		(SELECT string_agg(a.attname || E'\x1f' || format_type(a.atttypid, a.atttypmod) || E'\x1f' || a.attnotnull::text, E'\x1e' ORDER BY a.attnum)
		   FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL (
		  SELECT sum(s.n_live_tup)::bigint AS live, sum(pg_total_relation_size(t.relid))::bigint AS size,
		         max(GREATEST(s.last_vacuum, s.last_autovacuum)) AS vacuum,
		         sum(s.n_dead_tup)::bigint AS dead, sum(s.seq_scan)::bigint AS seq, sum(s.idx_scan)::bigint AS idx,
		         sum(s.n_tup_ins)::bigint AS ins, sum(s.n_tup_upd)::bigint AS upd, sum(s.n_tup_del)::bigint AS del
		  FROM (SELECT relid, isleaf FROM pg_partition_tree(c.oid) UNION SELECT c.oid, c.relkind <> 'p') t
		  LEFT JOIN pg_stat_user_tables s ON s.relid = t.relid AND t.isleaf) st
		WHERE n.nspname = $1 AND c.relname = $2`, []any{schema, table}, &live, &size, &vacuum, &dead, &seq, &idx, &ins, &upd, &del, &columns)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, counters, fmt.Errorf("%s: no such table", name)
	}
	if err != nil {
		return out, counters, fmt.Errorf("%s: %s", name, sqlError(err))
	}
	out.SizeBytes = size
	out.Rows = live
	if dead != nil {
		counters.DeadRows = *dead
		out.DeadRows = dead
	}
	if seq != nil {
		counters.SeqScans = *seq
	}
	if idx != nil {
		counters.IdxScans = *idx
	}
	if ins != nil {
		counters.Inserted = *ins
	}
	if upd != nil {
		counters.Updated = *upd
	}
	if del != nil {
		counters.Deleted = *del
	}
	if columns != nil && *columns != "" {
		for _, rec := range strings.Split(*columns, "\x1e") {
			f := strings.SplitN(rec, "\x1f", 3)
			if len(f) == 3 {
				counters.Columns = append(counters.Columns, source.Column{Name: f[0], Type: f[1], NotNull: f[2] == "true"})
			}
		}
	}
	if vacuum != nil {
		v := vacuum.UTC()
		out.LastVacuumAt = &v
	}
	ident := pgx.Identifier{schema, table}.Sanitize()
	if fresh && t.FreshnessColumn != "" {
		var newest *time.Time
		col := pgx.Identifier{t.FreshnessColumn}.Sanitize()
		if err := c.row(ctx, `SELECT max(`+col+`) FROM `+ident, nil, &newest); err != nil {
			out.CheckedAt = time.Now().UTC()
			return out, counters, fmt.Errorf("%w: %s: max(%s): %s", ErrPartial, name, t.FreshnessColumn, sqlError(err))
		}
		if newest != nil {
			n := newest.UTC()
			out.NewestAt = &n
		}
	}
	if exact {
		var n int64
		if err := c.row(ctx, `SELECT count(*) FROM `+ident, nil, &n); err != nil {
			out.CheckedAt = time.Now().UTC()
			return out, counters, fmt.Errorf("%w: %s: count(*): %s", ErrPartial, name, sqlError(err))
		}
		out.Rows = &n
		out.RowsExact = true
	}
	out.CheckedAt = time.Now().UTC()
	return out, counters, nil
}

// splitName is schema and table from "schema.table"; a bare name is in
// public.
func splitName(name string) (string, string) {
	if i := strings.IndexByte(name, '.'); i > 0 {
		return name[:i], name[i+1:]
	}
	return "public", name
}

// Replication is this endpoint's view of its replication. A replica
// reports how far behind it is; a primary reports each replica it streams
// to. A primary with none has nothing to say, and returns nil.
func (c *Client) Replication(ctx context.Context) (*wire.DatabaseReplication, error) {
	var inRecovery bool
	var lag *float64
	var replayed *time.Time
	err := c.row(ctx, `SELECT pg_is_in_recovery(),
		CASE WHEN pg_is_in_recovery() THEN EXTRACT(EPOCH FROM now() - pg_last_xact_replay_timestamp())::float8 END,
		pg_last_xact_replay_timestamp()`, nil, &inRecovery, &lag, &replayed)
	if err != nil {
		return nil, errors.New("pg_is_in_recovery: " + sqlError(err))
	}
	if inRecovery {
		r := &wire.DatabaseReplication{Role: "replica", LagSeconds: lag}
		if replayed != nil {
			at := replayed.UTC()
			r.LastReplayedAt = &at
		}
		return r, nil
	}
	c.queries.Add(1)
	qctx, cancel := c.bounded(ctx)
	defer cancel()
	rows, err := c.pool.Query(qctx, `SELECT application_name, state, EXTRACT(EPOCH FROM replay_lag)::float8
		FROM pg_stat_replication ORDER BY application_name`)
	if err != nil {
		return nil, errors.New("pg_stat_replication: " + sqlError(err))
	}
	defer rows.Close()
	var replicas []wire.DatabaseReplica
	for rows.Next() {
		rep, err := scanReplica(rows)
		if err != nil {
			return nil, errors.New("pg_stat_replication: " + sqlError(err))
		}
		replicas = append(replicas, rep)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("pg_stat_replication: " + sqlError(err))
	}
	if len(replicas) == 0 {
		return nil, nil
	}
	if len(replicas) > wire.MaxDatabaseReplicas {
		replicas = replicas[:wire.MaxDatabaseReplicas]
	}
	return &wire.DatabaseReplication{Role: "primary", Replicas: replicas}, nil
}

// scanReplica reads one pg_stat_replication row. A role without
// pg_read_all_stats sees state and lag as NULL.
func scanReplica(row pgx.Row) (wire.DatabaseReplica, error) {
	var name, state *string
	var lag *float64
	if err := row.Scan(&name, &state, &lag); err != nil {
		return wire.DatabaseReplica{}, err
	}
	rep := wire.DatabaseReplica{LagSeconds: lag}
	if name != nil {
		rep.Name = *name
	}
	if state != nil {
		rep.State = *state
	}
	return rep, nil
}

// sqlError is the server's message for a failed query, or the class of a
// failure to reach it. Never the DSN or the host.
func sqlError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Message
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return classify(err)
}
