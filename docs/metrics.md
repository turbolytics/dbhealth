# Metrics

This file lists every metric dbhealth exports. A test generates it from
`internal/report/catalog.go`, so change the catalog and run
`UPDATE_GOLDEN=1 go test ./internal/report/` to update it.

Every series carries the label `db`. A field the database could not give
is absent, not zero. StatsD names take the prefix `dbhealth.`. The field is
the JSON path in the report to Control.

## serving?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_probe_ok` | int | `probe.ok` |  | 1 when the last probe answered | `probe.ok` | SELECT 1 |
| `dbhealth_probe_latency_ms` | int | `probe.latency_ms` |  | the last probe's round trip | `probe.latency_ms` | SELECT 1 |
| `dbhealth_probe_consecutive_failures` | int | `probe.consecutive_failures` |  | probes failed in a row | `probe.consecutive_failures` | SELECT 1 |

## how busy?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_load_sessions_active_now` | float | `load.sessions_active_now` |  | sessions executing a statement | `load.sessions_active_now` | pg_stat_activity |
| `dbhealth_load_sessions_idle_in_transaction_now` | float | `load.sessions_idle_in_transaction_now` |  | sessions idle inside an open transaction | `load.sessions_idle_in_transaction_now` | pg_stat_activity |
| `dbhealth_load_sessions_waiting_now` | float | `load.sessions_waiting_now` |  | sessions blocked on a lock, a queue or a resource | `load.sessions_waiting_now` | pg_stat_activity |
| `dbhealth_load_queries_queued_now` | float | `load.queries_queued_now` |  | queries waiting to start | `load.queries_queued_now` | never sent |
| `dbhealth_load_longest_query_seconds` | float | `load.longest_query_seconds` |  | age of the oldest statement still running | `load.longest_query_seconds` | pg_stat_activity |
| `dbhealth_load_queries_per_second` | float | `load.queries_per_second` |  | the interval's queries | `load.queries_per_second` | never sent |
| `dbhealth_load_transactions_per_second` | float | `load.transactions_per_second` |  | the interval's commits | `load.transactions_per_second` | pg_stat_database |
| `dbhealth_load_rollbacks_per_second` | float | `load.rollbacks_per_second` |  | the interval's rollbacks | `load.rollbacks_per_second` | pg_stat_database |
| `dbhealth_load_rows_read_per_second` | float | `load.rows_read_per_second` |  | rows returned to clients | `load.rows_read_per_second` | pg_stat_database |
| `dbhealth_load_rows_written_per_second` | float | `load.rows_written_per_second` |  | rows inserted, updated and deleted | `load.rows_written_per_second` | pg_stat_database |
| `dbhealth_load_bytes_scanned_per_second` | float | `load.bytes_scanned_per_second` |  | storage read to answer queries | `load.bytes_scanned_per_second` | never sent |
| `dbhealth_load_cache_hit_ratio` | float | `load.cache_hit_ratio` |  | the interval's reads served from memory, 0..1 | `load.cache_hit_ratio` | pg_stat_database |
| `dbhealth_load_deadlocks_per_second` | float | `load.deadlocks_per_second` |  | the interval's deadlocks | `load.deadlocks_per_second` | pg_stat_database |
| `dbhealth_load_temp_bytes_per_second` | float | `load.temp_bytes_per_second` |  | sorts and hashes spilling to disk | `load.temp_bytes_per_second` | pg_stat_database |
| `dbhealth_load_wal_bytes_per_second` | float | `load.wal_bytes_per_second` |  | the write-ahead log the server wrote | `load.wal_bytes_per_second` | pg_current_wal_lsn |

## near its limits?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_connections_used` | int | `connections.used` |  | connections in use | `resources.connections.used` | pg_stat_activity |
| `dbhealth_connections_max` | int | `connections.max` |  | max_connections | `resources.connections.max` | max_connections |
| `dbhealth_connections_waiting` | int | `connections.waiting` |  | connections waiting on a lock | `resources.connections.waiting` | pg_stat_activity |
| `dbhealth_size_bytes` | int | `size_bytes` |  | the database on disk, WAL not included | `resources.size_bytes` | pg_database_size |
| `dbhealth_wal_bytes` | int | `wal_bytes` |  | the write-ahead log on disk, for the whole server | `resources.wal_bytes` | pg_ls_waldir, with wal_on_disk on |
| `dbhealth_oldest_transaction_seconds` | int | `oldest_transaction_seconds` |  | age of the oldest open transaction | `resources.oldest_transaction_seconds` | pg_stat_activity |
| `dbhealth_memory_shared_buffers_bytes` | int | `memory.shared_buffers_bytes` |  | shared_buffers | `resources.memory.shared_buffers_bytes` | shared_buffers |

## how many tables?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_tables` | int | `tables` |  | tables in the database's own schemas, partitions included | `resources.table_count` | pg_class |
| `dbhealth_partitions` | int | `partitions` |  | tables that are partitions | `resources.partition_count` | pg_class |

## tables current?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_table_newest_at_seconds` | int | `table.newest_at` | `table` | max of the freshness column, Unix seconds | `tables.newest_at` | max(freshness column) |
| `dbhealth_table_rows` | int | `table.rows` | `table` | rows, estimated unless dbhealth_table_rows_exact is 1 | `tables.rows` | pg_stat_user_tables or count(*) |
| `dbhealth_table_rows_exact` | int | `table.rows_exact` | `table` | 1 when dbhealth_table_rows is a count(*) | `tables.rows_exact` | count(*) |
| `dbhealth_table_size_bytes` | int | `table.size_bytes` | `table` | the table on disk | `tables.size_bytes` | pg_total_relation_size |
| `dbhealth_table_dead_rows` | int | `table.dead_rows` | `table` | rows deleted or updated and not yet reclaimed | `tables.dead_rows` | pg_stat_user_tables |
| `dbhealth_table_seq_scans_per_second` | float | `table.seq_scans_per_second` | `table` | whole-table reads | `tables.seq_scans_per_second` | pg_stat_user_tables |
| `dbhealth_table_index_scans_per_second` | float | `table.index_scans_per_second` | `table` | index reads | `tables.index_scans_per_second` | pg_stat_user_tables |

## tables written?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_table_rows_inserted_per_second` | float | `table.rows_inserted_per_second` | `table` | rows inserted, from the table's own counter | `tables.rows_inserted_per_second` | pg_stat_user_tables |
| `dbhealth_table_rows_updated_per_second` | float | `table.rows_updated_per_second` | `table` | rows updated, from the table's own counter | `tables.rows_updated_per_second` | pg_stat_user_tables |
| `dbhealth_table_rows_deleted_per_second` | float | `table.rows_deleted_per_second` | `table` | rows deleted, from the table's own counter | `tables.rows_deleted_per_second` | pg_stat_user_tables |

## schema changed?

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_table_schema_changes` | int | `table.schema_changes` | `table` | columns added, dropped or retyped since the last report | `tables.schema_changes` | pg_attribute |

## replication

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_replication_lag_seconds` | float | `replication.lag_seconds` |  | how far behind this endpoint is | `replication.lag_seconds` | pg_last_xact_replay_timestamp |
| `dbhealth_replication_replica_lag_seconds` | float | `replication.replica.lag_seconds` | `replica` | a replica's lag as its primary sees it | `replication.replicas.lag_seconds` | pg_stat_replication |

## replication slots

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_replication_slot_active` | int | `replication.slot.active` | `slot` | 1 when a consumer is connected to the slot | `replication.slots.active` | pg_replication_slots |
| `dbhealth_replication_slot_lost` | int | `replication.slot.lost` | `slot` | 1 when the server removed WAL the slot needs | `replication.slots.wal_status` | pg_replication_slots |
| `dbhealth_replication_slot_retained_bytes` | int | `replication.slot.retained_bytes` | `slot` | WAL the slot keeps on disk | `replication.slots.retained_bytes` | pg_replication_slots |
| `dbhealth_replication_slot_behind_bytes` | int | `replication.slot.behind_bytes` | `slot` | WAL the slot's consumer has not confirmed | `replication.slots.behind_bytes` | pg_replication_slots |
| `dbhealth_replication_slot_safe_wal_bytes` | int | `replication.slot.safe_wal_bytes` | `slot` | WAL the server can write before the slot is at risk | `replication.slots.safe_wal_bytes` | pg_replication_slots |

## what it cost

| Metric | Type | StatsD | Label | Meaning | Field | Postgres |
|---|---|---|---|---|---|---|
| `dbhealth_collection_queries` | int | `collection.queries` |  | queries the last interval ran | `collection.queries` | counted by dbhealth |
| `dbhealth_collection_duration_ms` | int | `collection.duration_ms` |  | what the last interval cost | `collection.duration_ms` | counted by dbhealth |
| `dbhealth_collection_errors` | int | `collection.errors` |  | queries that failed in the last interval | `collection.errors` | counted by dbhealth |
| `dbhealth_last_collect_timestamp_seconds` | int |  |  | when the last collection ran, Unix seconds |  | the reporter's clock |

## Not exported

Control receives these fields. No output sends them, for these reasons:

| Field | Why |
|---|---|
| `cluster` | a word; names the cluster |
| `kind` | a word; names the database's system |
| `probe.error` | a word; the class of the latest failure |
| `probe.last_ok_at` | a scrape already keeps dbhealth_probe_ok's history |
| `queries.calls_per_second` | a label per query shape would grow series without bound |
| `queries.id` | a label per query shape would grow series without bound |
| `queries.mean_ms` | a label per query shape would grow series without bound |
| `queries.rows_per_call` | a label per query shape would grow series without bound |
| `queries.text` | a label per query shape would grow series without bound |
| `queries.time_share` | a label per query shape would grow series without bound |
| `replication.last_replayed_at` | dbhealth_replication_lag_seconds says the same as an age |
| `replication.replicas.state` | a word; the replica's state |
| `replication.role` | a word; primary or replica |
| `replication.slots.database` | a word; the database the slot reads |
| `replication.slots.plugin` | a word; the decoding plugin |
| `replication.slots.type` | a word; logical or physical |
| `replication.upstream` | a word; names the upstream |
| `server_version` | a word; names the version |
| `tables.checked_at` | dbhealth_last_collect_timestamp_seconds says when |
| `tables.freshness_column` | a word; the config sent back |
| `tables.last_vacuum_at` | Control only so far; no reader has asked for a gauge |
| `tables.schema_hash` | a hash has no numeric meaning; dbhealth_table_schema_changes counts the changes |
| `target` | a word; names the endpoint |
