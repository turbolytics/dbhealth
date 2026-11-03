package report

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
)

// slotLost is the wal_status of a slot whose WAL the server removed. Its
// consumer can no longer resume, and a stopped slot is otherwise only slow.
const slotLost = "lost"

// fact is one exported metric. Wire is the field the outputs read, not a
// note about it, so a declaration cannot disagree with what is sent.
type fact struct {
	// Name is the Prometheus and OTLP name.
	Name string
	// StatsD is the name after "dbhealth.". Empty sends nothing to StatsD.
	StatsD string
	// Group is the question the metric answers, a heading in docs/metrics.md.
	Group string
	Help  string
	// Wire is the field's JSON path in wire.Database. Empty reads the
	// reporter's clock when it collected.
	Wire string
	// Float registers a float gauge. OTLP carries the difference, so a
	// reader's type changes if this does.
	Float bool
	// Count reads a list's length instead of its elements.
	Count bool
	// Equals reads 1 when the string field holds this word, otherwise 0.
	Equals string
	// When names a sibling of Wire's field. The metric is absent while the
	// sibling is, because it only describes the sibling.
	When string
	// Postgres is where the Postgres source reads the fact. Empty means
	// Postgres has no such reading, so it never sends the metric.
	Postgres string

	index []int  // Wire as field indexes
	when  int    // When as a field index, or -1
	label string // the label each element of a list on Wire's path carries
}

// listLabels is the label each list's elements carry, read from their name.
var listLabels = map[string]string{
	"tables":               "table",
	"replication.replicas": "replica",
	"replication.slots":    "slot",
}

const (
	serving   = "serving?"
	busy      = "how busy?"
	limits    = "near its limits?"
	counts    = "how many tables?"
	current   = "tables current?"
	written   = "tables written?"
	schema    = "schema changed?"
	replicate = "replication"
	slots     = "replication slots"
	cost      = "what it cost"
)

// catalog is every metric dbhealth exports, and the one place to add one.
// Prometheus, OTLP, StatsD and docs/metrics.md all read it. A name here is
// a promise to whoever built a dashboard on it, so a rename is its own
// change, with its own note.
var catalog = compile([]fact{
	{Name: "dbhealth_probe_ok", StatsD: "probe.ok", Group: serving, Help: "1 when the last probe answered", Wire: "probe.ok", Postgres: "SELECT 1"},
	{Name: "dbhealth_probe_latency_ms", StatsD: "probe.latency_ms", Group: serving, Help: "the last probe's round trip", Wire: "probe.latency_ms", Postgres: "SELECT 1"},
	{Name: "dbhealth_probe_consecutive_failures", StatsD: "probe.consecutive_failures", Group: serving, Help: "probes failed in a row", Wire: "probe.consecutive_failures", Postgres: "SELECT 1"},

	{Name: "dbhealth_load_sessions_active_now", StatsD: "load.sessions_active_now", Group: busy, Help: "sessions executing a statement", Wire: "load.sessions_active_now", Float: true, Postgres: "pg_stat_activity"},
	{Name: "dbhealth_load_sessions_idle_in_transaction_now", StatsD: "load.sessions_idle_in_transaction_now", Group: busy, Help: "sessions idle inside an open transaction", Wire: "load.sessions_idle_in_transaction_now", Float: true, Postgres: "pg_stat_activity"},
	{Name: "dbhealth_load_sessions_waiting_now", StatsD: "load.sessions_waiting_now", Group: busy, Help: "sessions blocked on a lock, a queue or a resource", Wire: "load.sessions_waiting_now", Float: true, Postgres: "pg_stat_activity"},
	{Name: "dbhealth_load_queries_queued_now", StatsD: "load.queries_queued_now", Group: busy, Help: "queries waiting to start", Wire: "load.queries_queued_now", Float: true},
	{Name: "dbhealth_load_longest_query_seconds", StatsD: "load.longest_query_seconds", Group: busy, Help: "age of the oldest statement still running", Wire: "load.longest_query_seconds", Float: true, Postgres: "pg_stat_activity"},
	{Name: "dbhealth_load_queries_per_second", StatsD: "load.queries_per_second", Group: busy, Help: "the interval's queries", Wire: "load.queries_per_second", Float: true},
	{Name: "dbhealth_load_transactions_per_second", StatsD: "load.transactions_per_second", Group: busy, Help: "the interval's commits", Wire: "load.transactions_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_rollbacks_per_second", StatsD: "load.rollbacks_per_second", Group: busy, Help: "the interval's rollbacks", Wire: "load.rollbacks_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_rows_read_per_second", StatsD: "load.rows_read_per_second", Group: busy, Help: "rows returned to clients", Wire: "load.rows_read_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_rows_written_per_second", StatsD: "load.rows_written_per_second", Group: busy, Help: "rows inserted, updated and deleted", Wire: "load.rows_written_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_bytes_scanned_per_second", StatsD: "load.bytes_scanned_per_second", Group: busy, Help: "storage read to answer queries", Wire: "load.bytes_scanned_per_second", Float: true},
	{Name: "dbhealth_load_cache_hit_ratio", StatsD: "load.cache_hit_ratio", Group: busy, Help: "the interval's reads served from memory, 0..1", Wire: "load.cache_hit_ratio", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_deadlocks_per_second", StatsD: "load.deadlocks_per_second", Group: busy, Help: "the interval's deadlocks", Wire: "load.deadlocks_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_temp_bytes_per_second", StatsD: "load.temp_bytes_per_second", Group: busy, Help: "sorts and hashes spilling to disk", Wire: "load.temp_bytes_per_second", Float: true, Postgres: "pg_stat_database"},
	{Name: "dbhealth_load_wal_bytes_per_second", StatsD: "load.wal_bytes_per_second", Group: busy, Help: "the write-ahead log the server wrote", Wire: "load.wal_bytes_per_second", Float: true, Postgres: "pg_current_wal_lsn"},

	{Name: "dbhealth_connections_used", StatsD: "connections.used", Group: limits, Help: "connections in use", Wire: "resources.connections.used", Postgres: "pg_stat_activity"},
	{Name: "dbhealth_connections_max", StatsD: "connections.max", Group: limits, Help: "max_connections", Wire: "resources.connections.max", Postgres: "max_connections"},
	{Name: "dbhealth_connections_waiting", StatsD: "connections.waiting", Group: limits, Help: "connections waiting on a lock", Wire: "resources.connections.waiting", Postgres: "pg_stat_activity"},
	{Name: "dbhealth_size_bytes", StatsD: "size_bytes", Group: limits, Help: "the database on disk, WAL not included", Wire: "resources.size_bytes", Postgres: "pg_database_size"},
	{Name: "dbhealth_wal_bytes", StatsD: "wal_bytes", Group: limits, Help: "the write-ahead log on disk, for the whole server", Wire: "resources.wal_bytes", Postgres: "pg_ls_waldir"},
	{Name: "dbhealth_oldest_transaction_seconds", StatsD: "oldest_transaction_seconds", Group: limits, Help: "age of the oldest open transaction", Wire: "resources.oldest_transaction_seconds", Postgres: "pg_stat_activity"},
	{Name: "dbhealth_memory_shared_buffers_bytes", StatsD: "memory.shared_buffers_bytes", Group: limits, Help: "shared_buffers", Wire: "resources.memory.shared_buffers_bytes", Postgres: "shared_buffers"},

	{Name: "dbhealth_tables", StatsD: "tables", Group: counts, Help: "tables in the database's own schemas, partitions included", Wire: "resources.table_count", Postgres: "pg_class"},
	{Name: "dbhealth_partitions", StatsD: "partitions", Group: counts, Help: "tables that are partitions", Wire: "resources.partition_count", Postgres: "pg_class"},

	{Name: "dbhealth_table_newest_at_seconds", StatsD: "table.newest_at", Group: current, Help: "max of the freshness column, Unix seconds", Wire: "tables.newest_at", Postgres: "max(freshness column)"},
	{Name: "dbhealth_table_rows", StatsD: "table.rows", Group: current, Help: "rows, estimated unless dbhealth_table_rows_exact is 1", Wire: "tables.rows", Postgres: "pg_stat_user_tables or count(*)"},
	{Name: "dbhealth_table_rows_exact", StatsD: "table.rows_exact", Group: current, Help: "1 when dbhealth_table_rows is a count(*)", Wire: "tables.rows_exact", When: "rows", Postgres: "count(*)"},
	{Name: "dbhealth_table_size_bytes", StatsD: "table.size_bytes", Group: current, Help: "the table on disk", Wire: "tables.size_bytes", Postgres: "pg_total_relation_size"},
	{Name: "dbhealth_table_dead_rows", StatsD: "table.dead_rows", Group: current, Help: "rows deleted or updated and not yet reclaimed", Wire: "tables.dead_rows", Postgres: "pg_stat_user_tables"},
	{Name: "dbhealth_table_seq_scans_per_second", StatsD: "table.seq_scans_per_second", Group: current, Help: "whole-table reads", Wire: "tables.seq_scans_per_second", Float: true, Postgres: "pg_stat_user_tables"},
	{Name: "dbhealth_table_index_scans_per_second", StatsD: "table.index_scans_per_second", Group: current, Help: "index reads", Wire: "tables.index_scans_per_second", Float: true, Postgres: "pg_stat_user_tables"},

	{Name: "dbhealth_table_rows_inserted_per_second", StatsD: "table.rows_inserted_per_second", Group: written, Help: "rows inserted, from the table's own counter", Wire: "tables.rows_inserted_per_second", Float: true, Postgres: "pg_stat_user_tables"},
	{Name: "dbhealth_table_rows_updated_per_second", StatsD: "table.rows_updated_per_second", Group: written, Help: "rows updated, from the table's own counter", Wire: "tables.rows_updated_per_second", Float: true, Postgres: "pg_stat_user_tables"},
	{Name: "dbhealth_table_rows_deleted_per_second", StatsD: "table.rows_deleted_per_second", Group: written, Help: "rows deleted, from the table's own counter", Wire: "tables.rows_deleted_per_second", Float: true, Postgres: "pg_stat_user_tables"},

	{Name: "dbhealth_table_schema_changes", StatsD: "table.schema_changes", Group: schema, Help: "columns added, dropped or retyped since the last report", Wire: "tables.schema_changes", Count: true, Postgres: "pg_attribute"},

	{Name: "dbhealth_replication_lag_seconds", StatsD: "replication.lag_seconds", Group: replicate, Help: "how far behind this endpoint is", Wire: "replication.lag_seconds", Float: true, Postgres: "pg_last_xact_replay_timestamp"},
	{Name: "dbhealth_replication_replica_lag_seconds", StatsD: "replication.replica.lag_seconds", Group: replicate, Help: "a replica's lag as its primary sees it", Wire: "replication.replicas.lag_seconds", Float: true, Postgres: "pg_stat_replication"},

	{Name: "dbhealth_replication_slot_active", StatsD: "replication.slot.active", Group: slots, Help: "1 when a consumer is connected to the slot", Wire: "replication.slots.active", Postgres: "pg_replication_slots"},
	{Name: "dbhealth_replication_slot_lost", StatsD: "replication.slot.lost", Group: slots, Help: "1 when the server removed WAL the slot needs", Wire: "replication.slots.wal_status", Equals: slotLost, Postgres: "pg_replication_slots"},
	{Name: "dbhealth_replication_slot_retained_bytes", StatsD: "replication.slot.retained_bytes", Group: slots, Help: "WAL the slot keeps on disk", Wire: "replication.slots.retained_bytes", Postgres: "pg_replication_slots"},
	{Name: "dbhealth_replication_slot_behind_bytes", StatsD: "replication.slot.behind_bytes", Group: slots, Help: "WAL the slot's consumer has not confirmed", Wire: "replication.slots.behind_bytes", Postgres: "pg_replication_slots"},
	{Name: "dbhealth_replication_slot_safe_wal_bytes", StatsD: "replication.slot.safe_wal_bytes", Group: slots, Help: "WAL the server can write before the slot is at risk", Wire: "replication.slots.safe_wal_bytes", Postgres: "pg_replication_slots"},

	{Name: "dbhealth_collection_queries", StatsD: "collection.queries", Group: cost, Help: "queries the last interval ran", Wire: "collection.queries", Postgres: "counted by dbhealth"},
	{Name: "dbhealth_collection_duration_ms", StatsD: "collection.duration_ms", Group: cost, Help: "what the last interval cost", Wire: "collection.duration_ms", Postgres: "counted by dbhealth"},
	{Name: "dbhealth_collection_errors", StatsD: "collection.errors", Group: cost, Help: "queries that failed in the last interval", Wire: "collection.errors", Count: true, Postgres: "counted by dbhealth"},
	{Name: "dbhealth_last_collect_timestamp_seconds", Group: cost, Help: "when the last collection ran, Unix seconds", Postgres: "the reporter's clock"},
})

// notExported is every wire field that has no metric, and why. Control
// still receives each one. A new wire field fails the guard until it lands
// in catalog or here, so a fact never goes missing from the outputs by
// accident.
var notExported = map[string]string{
	"kind":           "a word; names the database's system",
	"target":         "a word; names the endpoint",
	"cluster":        "a word; names the cluster",
	"server_version": "a word; names the version",

	"probe.last_ok_at": "a scrape already keeps dbhealth_probe_ok's history",
	"probe.error":      "a word; the class of the latest failure",

	"tables.freshness_column": "a word; the config sent back",
	"tables.last_vacuum_at":   "Control only so far; no reader has asked for a gauge",
	"tables.checked_at":       "dbhealth_last_collect_timestamp_seconds says when",
	"tables.schema_hash":      "a hash has no numeric meaning; dbhealth_table_schema_changes counts the changes",

	"replication.role":             "a word; primary or replica",
	"replication.last_replayed_at": "dbhealth_replication_lag_seconds says the same as an age",
	"replication.upstream":         "a word; names the upstream",
	"replication.replicas.state":   "a word; the replica's state",
	"replication.slots.type":       "a word; logical or physical",
	"replication.slots.plugin":     "a word; the decoding plugin",
	"replication.slots.database":   "a word; the database the slot reads",

	"queries.id":               "a label per query shape would grow series without bound",
	"queries.text":             "a label per query shape would grow series without bound",
	"queries.calls_per_second": "a label per query shape would grow series without bound",
	"queries.mean_ms":          "a label per query shape would grow series without bound",
	"queries.time_share":       "a label per query shape would grow series without bound",
	"queries.rows_per_call":    "a label per query shape would grow series without bound",
}

// compile resolves each fact's Wire path against the wire types. A path the
// types do not have, or a reading they cannot give, panics at startup and in
// every test, rather than exporting nothing for that metric.
func compile(facts []fact) []fact {
	for i := range facts {
		f := &facts[i]
		f.when = -1
		if f.Wire == "" {
			continue
		}
		t := reflect.TypeOf(wire.Database{})
		var parent reflect.Type
		var prefix []string
		parts := strings.Split(f.Wire, ".")
		for at, name := range parts {
			for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
				if t.Kind() == reflect.Slice {
					list := strings.Join(prefix, ".")
					label, ok := listLabels[list]
					if !ok {
						panic(fmt.Sprintf("%s: the list %s has no label", f.Name, list))
					}
					f.label = label
				}
				t = t.Elem()
			}
			idx, field, ok := wireField(t, name)
			if !ok {
				panic(fmt.Sprintf("%s: wire.Database has no field %s", f.Name, f.Wire))
			}
			f.index = append(f.index, idx)
			if at == len(parts)-1 {
				parent = t
			}
			t = field
			prefix = append(prefix, name)
		}
		if f.When != "" {
			idx, _, ok := wireField(parent, f.When)
			if !ok {
				panic(fmt.Sprintf("%s: %s has no sibling %s", f.Name, f.Wire, f.When))
			}
			f.when = idx
		}
		if err := f.check(t); err != nil {
			panic(fmt.Sprintf("%s: %v", f.Name, err))
		}
	}
	return facts
}

var timeType = reflect.TypeOf(time.Time{})

// check refuses a declaration whose field cannot give the reading.
func (f *fact) check(t reflect.Type) error {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch {
	case f.Count:
		if t.Kind() != reflect.Slice {
			return fmt.Errorf("Count reads a list, and %s is %s", f.Wire, t)
		}
	case f.Equals != "":
		if t.Kind() != reflect.String {
			return fmt.Errorf("Equals reads a word, and %s is %s", f.Wire, t)
		}
	case t == timeType, t.Kind() == reflect.Bool, t.Kind() == reflect.Int, t.Kind() == reflect.Int64:
	case t.Kind() == reflect.Float64:
		if !f.Float {
			return fmt.Errorf("%s is a float, and an int gauge would truncate it", f.Wire)
		}
	default:
		return fmt.Errorf("%s is %s, which has no number", f.Wire, t)
	}
	return nil
}

// wireField finds the struct field with this JSON name.
func wireField(t reflect.Type, name string) (int, reflect.Type, bool) {
	for i := 0; i < t.NumField(); i++ {
		if n, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ","); n == name {
			return i, t.Field(i).Type, true
		}
	}
	return 0, nil, false
}

// sample is one series' value: an int64, or a float64 for a Float fact.
type sample struct {
	label string
	value any
}

// samples is the fact's series in one section. A field the section does not
// hold has none: absent, not zero. at is when the reporter collected.
func (f *fact) samples(d *wire.Database, at time.Time) []sample {
	if f.Wire == "" {
		return []sample{{value: at.Unix()}}
	}
	var out []sample
	f.walk(reflect.ValueOf(d).Elem(), f.index, "", &out)
	return out
}

func (f *fact) walk(v reflect.Value, index []int, label string, out *[]sample) {
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return
		}
		v = v.Elem()
	}
	if len(index) == 0 {
		*out = append(*out, sample{label, f.number(v)})
		return
	}
	if v.Kind() == reflect.Slice {
		for i := 0; i < v.Len(); i++ {
			e := v.Index(i)
			f.walk(e, index, e.FieldByName("Name").String(), out)
		}
		return
	}
	if len(index) == 1 && f.when >= 0 && v.Field(f.when).IsNil() {
		return
	}
	f.walk(v.Field(index[0]), index[1:], label, out)
}

func (f *fact) number(v reflect.Value) any {
	var n int64
	switch {
	case f.Count:
		n = int64(v.Len())
	case f.Equals != "":
		n = boolInt(v.String() == f.Equals)
	case v.Type() == timeType:
		n = v.Interface().(time.Time).Unix()
	case v.Kind() == reflect.Bool:
		n = boolInt(v.Bool())
	case v.Kind() == reflect.Float64:
		return v.Float()
	default:
		n = v.Int()
	}
	if f.Float {
		return float64(n)
	}
	return n
}

func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
