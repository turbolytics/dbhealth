# dbhealth

Is my database serving? How close is it to its limits? Are its tables
current? `dbhealth` asks one Postgres those questions every minute and
reports the answers to [control](https://control.turbolytics.io),
beside your pipelines.

## One minute

```bash
git clone https://github.com/turbolytics/dbhealth && cd dbhealth && make build

export DBHEALTH_PRIMARY_DSN='postgres://user:password@pg.internal:5432/billing'
export TURBOSTATS_CREDENTIAL='sfc_...'     # from control.turbolytics.io: your org's credential

cat > dbhealth.yml <<'EOF'
databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_PRIMARY_DSN }}"
    name: billing-primary
report:
  to: https://control.turbolytics.io
  credential: "{{ TURBOSTATS_CREDENTIAL }}"
EOF

./bin/dbhealth validate -c dbhealth.yml
ok: 1 databases

./bin/dbhealth run -c dbhealth.yml
```

Everything else defaults: the first 50 tables by name, in every schema
but the catalog, probed once a minute, row counts estimated.

A minute later control shows `billing-primary`: serving, 3ms; 18 of 100
connections; 2.1 GB; `public.events` newest row 12s ago, 48,213,904 rows.

## The config

The full file, `examples/dbhealth.yml`, with a primary and a replica:

```yaml
databases:
  - kind: postgres
    dsn: "{{ DBHEALTH_PRIMARY_DSN }}"   # from the environment; control sees host:port/db only
    name: billing-primary
    cluster: billing

probe:
  interval_seconds: 60                # one probe and one report per database per minute
  timeout_seconds: 5

tables:
  discover:                           # every table in these schemas, up to max_tables
    schemas: [public]
    exclude: ["sqlflow_*", "*_tmp"]
    freshness_columns: [updated_at, created_at, minute, ts]
    max_tables: 50
  rows: estimate                      # estimate (free) or exact (count(*), once an hour)
  freshness_interval_seconds: 60
  rows_exact_interval_seconds: 3600

report:
  to: https://control.turbolytics.io
  credential: "{{ TURBOSTATS_CREDENTIAL }}"
  # statsd: localhost:8125            # the same facts as gauges
```

`discover` is the one-minute path: point it at a database and every table
is watched, its freshness from the first of `freshness_columns` it has. For
production, name the tables instead, so a schema change cannot add a
two-billion-row table to the watch list by accident:

```yaml
tables:
  static:
    - name: public.usage_per_minute
      freshness_column: minute
      rows: exact
    - name: public.events
      freshness_column: created_at
```

## What it costs the database

Every interval: one `SELECT 1`, four catalog reads, and per table one
estimate and one `max(column)`. `count(*)` only where you ask, once an hour.
Each report says what it cost, `collection.queries` and
`collection.duration_ms`, so you can see it.

## What it sends

Facts, never judgments: timestamps, counts and limits. Control decides what
is stale or full. The connection string never leaves the process; the report
carries `host:port/database`.

```json
{
  "database": {
    "kind": "postgres",
    "target": "pg.internal:5432/billing",
    "probe": {"ok": true, "latency_ms": 3, "last_ok_at": "2026-10-05T12:00:00Z", "consecutive_failures": 0},
    "resources": {"connections": {"used": 18, "max": 100, "waiting": 0}, "size_bytes": 2147483648},
    "tables": [
      {"name": "public.events", "freshness_column": "created_at", "newest_at": "2026-10-05T11:59:48Z",
       "rows": 48213904, "rows_exact": false, "size_bytes": 9126805504, "checked_at": "2026-10-05T12:00:00Z"}
    ],
    "collection": {"queries": 9, "duration_ms": 12, "errors": []}
  }
}
```

A query that fails lands in `collection.errors` with its table; the rest of
the report still sends. A role without `pg_read_all_stats` gets
`pg_stat_activity: 3 other sessions hidden from this role; grant pg_read_all_stats`
rather than a connection count that is wrong.

## Replicas

One entry per endpoint, replicas included: a primary that answers says
nothing about the replica your application reads from. `cluster` groups
them in control.
