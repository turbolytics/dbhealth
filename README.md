# dbhealth

[![Docker Pulls](https://img.shields.io/docker/pulls/turbolytics/dbhealth)](https://hub.docker.com/r/turbolytics/dbhealth)

**Monitor your database's health.** Is it serving? How close is it to its
limits? Are its tables current? One container, one connection string,
answers every minute in [control](https://control.turbolytics.io),
Prometheus, Datadog, or anything that speaks OpenTelemetry.

## Quick start

```bash
docker run -d --name dbhealth \
  -e DBHEALTH_DSN='postgres://user:password@pg.internal:5432/billing' \
  -e DBHEALTH_KEY='sfc_...' \
  turbolytics/dbhealth
```

`DBHEALTH_KEY` is your org's credential from [control](https://control.turbolytics.io).
A minute later control shows `billing`: serving, 3 ms; 18 of 100
connections; 2.1 GB; `public.events` newest row 12 s ago, 48,213,904 rows.

**No control yet?** Point it at your own tools instead:

```bash
docker run -d -p 8000:8000 \
  -e DBHEALTH_DSN='postgres://user:password@pg.internal:5432/billing' \
  -e DBHEALTH_METRICS=prometheus \
  turbolytics/dbhealth

curl -s localhost:8000/metrics | grep dbhealth_probe_ok
dbhealth_probe_ok{db="billing"} 1
```

`-e DBHEALTH_METRICS=otlp -e DBHEALTH_OTLP=http://datadog-agent:4318` pushes
the same to the Datadog agent, Grafana Alloy, or any OpenTelemetry
collector. `/healthz` is on the same port for your supervisor.

## What you get

Facts, every minute, per database. Control, Prometheus and OTLP carry the
same names; StatsD the same with dots.

| | metric | from |
|---|---|---|
| **serving?** | `dbhealth_probe_ok`, `dbhealth_probe_latency_ms`, `dbhealth_probe_consecutive_failures` | one `SELECT 1`, timed |
| **how busy?** | `dbhealth_load_sessions_active_now` / `_waiting_now` / `_idle_in_transaction_now`, `dbhealth_load_longest_query_seconds`; `dbhealth_load_transactions_per_second`, `_rows_read_per_second`, `_rows_written_per_second`, `_cache_hit_ratio`, `_temp_bytes_per_second`, `_deadlocks_per_second` | `pg_stat_activity` now; `pg_stat_database` counters, two readings apart |
| **near its limits?** | `dbhealth_connections_used` / `_max` / `_waiting`, `dbhealth_size_bytes`, `dbhealth_oldest_transaction_seconds`, `dbhealth_memory_shared_buffers_bytes` | `pg_stat_activity`, `pg_database_size` |
| **tables current?** | `dbhealth_table_newest_at_seconds`, `dbhealth_table_rows`, `dbhealth_table_rows_exact`, `dbhealth_table_size_bytes`, `dbhealth_table_dead_rows`, `_seq_scans_per_second` / `_index_scans_per_second` — labelled `table` | `max(timestamp column)`, `n_live_tup` or `count(*)`, `pg_stat_user_tables` |
| **tables written?** | `dbhealth_table_rows_inserted_per_second` / `_updated_` / `_deleted_` — the table's volume as its own counters say it, not an estimate | `pg_stat_user_tables`, two readings apart |
| **schema changed?** | `dbhealth_table_schema_changes` — columns added, dropped, retyped or changed in nullability since the last report; control shows which, with the type before and after | `pg_attribute`, in the same read |
| **replication** | `dbhealth_replication_lag_seconds`, per-replica lag | `pg_stat_replication` |
| **what it cost** | `dbhealth_collection_queries`, `_duration_ms`, `_errors` | counted |

Every series carries `db`. A field the database could not give is absent,
not zero: a role without `pg_read_all_stats` gets an error naming the
grant, not a connection count that is wrong. A `_per_second` rate is the
interval's, from the database's own counters two readings apart; the
first interval after a start or a stats reset sends none. Query text is
never read.

**What it costs the database:** one `SELECT 1`, six catalog reads, and per
table one estimate and one `max()`, every interval. `count(*)` only where
you ask, once an hour. `load: {enabled: false}` drops two of the reads.

## Config

The quick start is the defaults: every table, up to 50, probed
once a minute, rows estimated. A file is the same with more than one
database, chosen tables, or exact counts:

```yaml
databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_PRIMARY_DSN }}"   # from the environment; control sees host:port/db only
    name: billing-primary
    cluster: billing
  - kind: postgres
    dsn: "{{ DBHEALTH_REPLICA_A_DSN }}"
    name: billing-replica-a
    cluster: billing

probe:
  interval_seconds: 60
  timeout_seconds: 5

tables:
  discover:                           # every table in these schemas, up to max_tables
    schemas: [public]
    exclude: ["sqlflow_*", "*_tmp"]
    freshness_columns: [bucket, minute, ts, created_at, updated_at]   # the first one a table has; event time before write time
    max_tables: 50
  # static:                           # or name them, so a schema change cannot add
  #   - name: public.usage_per_minute #   a two-billion-row table by accident
  #     freshness_column: minute
  #     rows: exact
  rows: estimate                      # estimate (free) or exact (count(*), once an hour)
  freshness_interval_seconds: 60
  rows_exact_interval_seconds: 3600

load:
  enabled: true                       # sessions and rates; two catalog reads an interval

report:
  to: https://ingest.turbolytics.io/v1/turbostats
  credential: "{{ TURBOSTATS_CREDENTIAL }}"
  # metrics: prometheus               # /metrics and /healthz on :8000
  # metrics: otlp                     # or pushed every interval
  # otlp: http://collector:4318
  # statsd: localhost:8125
```

```bash
dbhealth validate -c dbhealth.yml     # ok: 2 databases
dbhealth run -c dbhealth.yml
```

Each replica is its own entry: a primary that answers says nothing about
the replica your application reads from. `cluster` groups them in control.

| environment (no file) | |
|---|---|
| `DBHEALTH_DSN` | the connection string; required |
| `DBHEALTH_KEY` | the control credential |
| `DBHEALTH_NAME`, `DBHEALTH_CLUSTER` | default to the database's name |
| `DBHEALTH_METRICS` | `prometheus` or `otlp` |
| `DBHEALTH_LOAD` | `false` to leave the load facts out |
| `DBHEALTH_OTLP`, `DBHEALTH_LISTEN`, `DBHEALTH_STATSD` | the collector; the listen address (`:8000`); StatsD |

The connection string never leaves the process. What is sent is
`host:port/database`.

## Build it yourself

```bash
make build && ./bin/dbhealth version
make test-short            # unit, no Docker
make test-integration      # against postgres:18 in testcontainers
make release-test          # the image, run against Postgres
```
