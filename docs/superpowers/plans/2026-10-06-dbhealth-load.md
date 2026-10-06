# dbhealth load Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every bundle says what people are doing to the database: how busy now, how much work per second, memory or disk, and, when asked, which queries.

**Architecture:** Three pieces, one PR each. The wire types for `load` and `queries` in sql-flow, released. The Postgres reads: `pg_stat_activity` columns, `pg_stat_database` counters, `pg_stat_statements` opt-in, `pg_stat_user_tables` deltas. The collector turns two counter readings into rates and keeps the per-query answer between its intervals. Every other kind reuses the rate logic; only the reads are theirs.

**Tech Stack:** as v1: Go 1.26, pgx, `github.com/turbolytics/sql-flow/turbostats/wire`, testcontainers `postgres:18`.

**Spec:** `docs/superpowers/specs/2026-10-06-turbostats-database-load-design.md` (merged in sql-flow #445; this copy is for the executor).

## Global Constraints

- **Samples are `_now`, rates are `_per_second`.** A rate is `(counter_now - counter_then) / seconds`, from two readings. Never a sample passed off as a rate.
- **A rate is absent, never zero, on the first interval and when a counter went backwards.** The bundle omits it.
- **`cache_hit_ratio` is the interval's**, `hits / (hits + misses)` of the delta, not since the server started.
- **Per-query facts are opt-in** (`load.queries.top > 0`), at most `wire.MaxDatabaseQueries = 20`, text normalized and truncated to 200 characters, literals never sent, read on `load.queries.interval_seconds`, never every probe.
- **A field a kind or a role cannot give is absent**, with a `collection.errors` entry naming the view when a read failed.
- **Prose** per sql-flow-control's `CLAUDE.md`. Commit messages name the defect, the fix and the evidence.
- **Every PR is a draft, cut from `main`, reviewed, and merged before the next starts.** Push with an explicit refspec (`branch:refs/heads/branch`, quoted).

## Review Focus

1. **A stats reset between two readings** (`pg_stat_reset()`, or a restart). Every rate must be absent that interval, not a negative or a huge number. Test: Task 3's reset case, and Task 2 against a real `pg_stat_reset()`.
2. **A query text with a literal in it**, on a kind with no normalized form, or a Postgres without `pg_stat_statements.track`. Only `?` reaches the bundle. Test: Task 2's normalization table includes `WHERE password = 'hunter2'`.
3. **A role that can read `pg_stat_database` but not `pg_stat_statements`.** `load` is sent, `queries` is absent, one error names the extension or the grant. Test: Task 2 as `reader`.
4. **dbhealth restarts between intervals.** The saved counters are gone; the first bundle after restart has no rates. Test: Task 3, a fresh Collector against the same fake counters.
5. **`interval_seconds` 60 with the reporter's jitter**: a rate over 58 s or 66 s is divided by the real elapsed seconds between the two readings, not by the configured interval. Test: Task 3, readings 58 s apart.

---

## File structure

| path | responsibility |
|---|---|
| sql-flow `turbostats/wire/database.go` | `DatabaseLoad`, `DatabaseQuery`, the three table fields, `MaxDatabaseQueries` |
| sql-flow `turbostats/wire/schema/bundle.schema.json` | regenerated |
| sql-flow `internal/turbostats/dimensional_test.go` | the `queries` exemption and the widest bundle with load |
| `internal/config/config.go` | `Load{Enabled, Queries{Top, IntervalSeconds}}` and its rules |
| `internal/postgres/load.go`, `load_test.go` | the counter reading, the activity sample, `pg_stat_statements`, normalization |
| `internal/source/source.go` | `Counters`, `Load(ctx)`, `Queries(ctx, top)` on the interface |
| `internal/collector/rates.go`, `rates_test.go` | two readings → rates; the reset and restart rules |
| `internal/collector/collector.go` | load each interval, queries on their interval, table deltas |
| `internal/report/statsd.go`, `metrics.go` | the new gauges |
| `test/e2e/e2e_test.go` | load under the metering stack's workers |

---

### Task 1: The wire types (sql-flow)

**Files:**
- Modify: sql-flow `turbostats/wire/database.go`, `database_test.go`, `internal/turbostats/dimensional_test.go`, `CHANGELOG.md`
- Regenerate: `turbostats/wire/schema/bundle.schema.json` (`make schema`)

**Interfaces:**
- Produces:
  ```go
  const MaxDatabaseQueries = 20
  type DatabaseLoad struct {
      SessionsActiveNow            *int     `json:"sessions_active_now,omitempty"`
      SessionsIdleInTransactionNow *int     `json:"sessions_idle_in_transaction_now,omitempty"`
      SessionsWaitingNow           *int     `json:"sessions_waiting_now,omitempty"`
      QueriesQueuedNow             *int     `json:"queries_queued_now,omitempty"`
      LongestQuerySeconds          *float64 `json:"longest_query_seconds,omitempty"`
      QueriesPerSecond             *float64 `json:"queries_per_second,omitempty"`
      TransactionsPerSecond        *float64 `json:"transactions_per_second,omitempty"`
      RollbacksPerSecond           *float64 `json:"rollbacks_per_second,omitempty"`
      RowsReadPerSecond            *float64 `json:"rows_read_per_second,omitempty"`
      RowsWrittenPerSecond         *float64 `json:"rows_written_per_second,omitempty"`
      BytesScannedPerSecond        *float64 `json:"bytes_scanned_per_second,omitempty"`
      CacheHitRatio                *float64 `json:"cache_hit_ratio,omitempty"`
      DeadlocksPerSecond           *float64 `json:"deadlocks_per_second,omitempty"`
      TempBytesPerSecond           *float64 `json:"temp_bytes_per_second,omitempty"`
  }
  type DatabaseQuery struct {
      ID             string  `json:"id"`
      Text           string  `json:"text"`
      CallsPerSecond float64 `json:"calls_per_second"`
      MeanMs         float64 `json:"mean_ms"`
      TimeShare      float64 `json:"time_share"`
      RowsPerCall    float64 `json:"rows_per_call"`
  }
  // on Database:  Load *DatabaseLoad `json:"load,omitempty"`; Queries []DatabaseQuery `json:"queries,omitempty"`
  // on DatabaseTable: DeadRows *int64 `json:"dead_rows,omitempty"`; SeqScansPerSecond, IndexScansPerSecond *float64
  ```

- [ ] **Step 1: Write the failing tests** in `database_test.go`: `TestDatabaseLoad_SpecExampleRoundTrips` (the spec's `load` and `queries` JSON unmarshal into the types and marshal back to the same field names), `TestDatabaseLoad_AbsentRatesAreAbsent` (a `DatabaseLoad` with only `SessionsActiveNow` marshals to one key). In `dimensional_test.go`: `widestDatabase` takes a `queries int` and fills `Load` at its widest; `TestCollect_AFullDatabaseBundleStaysUnderTheCeiling` uses `wire.MaxDatabaseQueries`; the shape guard exempts `.Bundle.Database.Queries` with its bound.
- [ ] **Step 2: Run them**: `go test ./turbostats/wire/ ./internal/turbostats/ -run 'DatabaseLoad|AFullDatabaseBundle|NoFieldScales'` → build failure.
- [ ] **Step 3: Implement** the types, `make schema`, the CHANGELOG entry under Added.
- [ ] **Step 4: Run** `go test -race ./turbostats/... ./internal/turbostats/ ./internal/schema/`; log the widest bundle's bytes; it must stay under 48 KiB (raise nothing; if it does not fit, `MaxDatabaseQueries` comes down and the spec says so).
- [ ] **Step 5: Commit and push** `feat/wire-database-load` with an explicit refspec; draft PR. After merge: tag `v2026.10.06` (draft the message for approval; push only the tag), then `go get github.com/turbolytics/sql-flow@<that commit>` in dbhealth.

---

### Task 2: The Postgres reads

**Files:**
- Create: `internal/postgres/load.go`, `internal/postgres/load_test.go`
- Modify: `internal/source/source.go`, `internal/postgres/postgres.go` (`Table` gains the three counters)

**Interfaces:**
- Produces:
  ```go
  package source
  // Counters is one reading of the system's cumulative counters, the
  // collector's input for rates. Every field is a count since the stats
  // began; -1 means the kind does not have it.
  type Counters struct {
      At                                           time.Time
      Queries, Commits, Rollbacks                  int64
      RowsRead, RowsWritten, BytesScanned          int64
      CacheHits, CacheMisses                       int64
      Deadlocks, TempBytes                         int64
  }
  // Sample is the instant: what is true now.
  type Sample struct {
      SessionsActive, SessionsIdleInTransaction, SessionsWaiting, QueriesQueued *int
      LongestQuerySeconds *float64
  }
  type QueryStat struct { ID, Text string; Calls int64; TotalMs float64; Rows int64 }
  // on Source:
  Load(ctx) (Sample, Counters, []wire.DatabaseError)
  Queries(ctx, top int) ([]QueryStat, error)   // the statement store's top entries by total time; ErrUnsupported when the kind or the role has none
  // DatabaseTable gains, from the same pg_stat_user_tables read:
  // TableCounters{DeadRows int64; SeqScans, IdxScans int64} returned beside the row
  ```

- [ ] **Step 1: The queries**, each verified on Postgres 18 before the test is written:

  | field | SQL |
  |---|---|
  | sample | `SELECT count(*) FILTER (WHERE state='active'), count(*) FILTER (WHERE state='idle in transaction'), count(*) FILTER (WHERE wait_event_type='Lock'), COALESCE(EXTRACT(EPOCH FROM now() - min(query_start)) FILTER (WHERE state='active' AND pid <> pg_backend_pid()), 0) FROM pg_stat_activity WHERE backend_type='client backend'` — when the role sees other sessions' `state` as NULL (the v1 `hidden` check), the sample is absent and the existing error covers it |
  | counters | `SELECT xact_commit, xact_rollback, tup_returned + tup_fetched, tup_inserted + tup_updated + tup_deleted, blks_hit, blks_read, deadlocks, temp_bytes, stats_reset FROM pg_stat_database WHERE datname = current_database()` — `Queries` is -1 unless `pg_stat_statements` is readable, then `SELECT sum(calls) FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())` |
  | queries | `SELECT queryid::text, query, calls, total_exec_time, rows FROM pg_stat_statements WHERE dbid = … ORDER BY total_exec_time DESC LIMIT $1` — `ErrUnsupported` on SQLSTATE `42P01` (no extension) or `42501` (no grant), with the message naming `pg_stat_statements` |
  | table counters | add `s.n_dead_tup, s.seq_scan, s.idx_scan` to the existing `Table` read |

  `normalize(text string) string`: Postgres already replaces literals with `$1`; `normalize` still replaces any remaining single-quoted string or bare number with `?`, collapses whitespace, and truncates to 200 characters. Table-driven test with `WHERE password = 'hunter2'` → `WHERE password = ?`.

- [ ] **Step 2: Write the failing tests** in `load_test.go` against the package's container (seed adds `CREATE EXTENSION pg_stat_statements` where the image allows; the image needs `-c shared_preload_libraries=pg_stat_statements`, passed through `tcpostgres.WithConfigFile` or `Cmd`):

  | Test | Proves |
  |---|---|
  | `TestLoad_SampleCountsThisSession` | `SessionsActive >= 1` (this query), `SessionsWaiting == 0`, `LongestQuerySeconds >= 0` |
  | `TestLoad_CountersMoveWithWork` | read, insert 100 rows, `pg_stat_force_next_flush()`, read: `RowsWritten` up by 100, `Commits` up by at least 1, `CacheHits` up |
  | `TestLoad_ResetIsVisible` | after `pg_stat_reset()`, the second reading's `Commits` is below the first's (the collector's reset rule has something to see) |
  | `TestLoad_ReaderRoleGetsCountersNotSample` | as `reader`: `Counters` filled, `Sample` empty, the existing hidden-sessions error |
  | `TestQueries_TopByTotalTime` | run a distinctive query 5 times; `Queries(ctx, 5)` carries it with `Calls >= 5`, normalized text, no literal |
  | `TestQueries_WithoutTheExtensionIsUnsupported` | against a container without the extension: `errors.Is(err, source.ErrUnsupported)` and the message names `pg_stat_statements` |
  | `TestNormalize` | the table: `$1` kept, `'hunter2'` → `?`, `42` → `?`, whitespace collapsed, 300 chars → 200 |
  | `TestTable_CarriesItsCounters` | `DeadRows`, `SeqScans`, `IdxScans` returned beside the row; after `DELETE 10`, `DeadRows >= 10` before vacuum |

- [ ] **Step 3: Run** `go test ./internal/postgres/ -run 'Load|Queries|Normalize|CarriesItsCounters'` → build failure, `undefined: Load`.
- [ ] **Step 4: Implement** `load.go`; `Queries` counts toward `Client.Queries()` like every read.
- [ ] **Step 5: Run** `go test -race -count=1 ./internal/postgres/` → pass; `-short` still skips.
- [ ] **Step 6: Commit and push** `feat/postgres-load`, draft PR.

---

### Task 3: Rates, and the collector

**Files:**
- Create: `internal/collector/rates.go`, `internal/collector/rates_test.go`
- Modify: `internal/collector/collector.go`, `collector_test.go` (the fake gains counters, a sample, query stats), `internal/config/config.go`, `config_test.go`

**Interfaces:**
- Consumes: `source.Counters`, `source.Sample`, `source.QueryStat` (Task 2); `wire.DatabaseLoad`, `wire.DatabaseQuery` (Task 1).
- Produces:
  ```go
  package collector
  // rates turns two readings into the interval's rates. nil when prev is
  // zero (first interval) or any counter went backwards (reset); a kind's
  // -1 counters produce no field.
  func rates(prev, now source.Counters) *wire.DatabaseLoad
  // on Collector: counters source.Counters (the last reading), queriesAt time.Time, queries []wire.DatabaseQuery
  ```
  Config:
  ```go
  type Load struct { Enabled bool; Queries LoadQueries }   // Enabled defaults true
  type LoadQueries struct { Top, IntervalSeconds int }      // Top 0 (off); IntervalSeconds 300; Top ≤ wire.MaxDatabaseQueries; IntervalSeconds ≥ probe interval
  ```

- [ ] **Step 1: Write the failing tests**:

  `rates_test.go`:
  | Test | Proves |
  |---|---|
  | `TestRates_FirstReadingHasNone` | `rates(Counters{}, now)` is nil |
  | `TestRates_DividesByTheRealElapsed` | readings 58 s apart, commits +1160 → `TransactionsPerSecond` 20.0, not 19.33 |
  | `TestRates_AResetIsNoRate` | `now.Commits < prev.Commits` → nil, every field |
  | `TestRates_CacheHitIsTheIntervals` | prev hits 1e9 / misses 0, now hits 1e9+100 / misses 100 → `CacheHitRatio` 0.5 |
  | `TestRates_AKindWithoutACounterSendsNoField` | `Queries: -1` both readings → `QueriesPerSecond` nil, the rest set |
  | `TestRates_NoReadsNoRatio` | hits and misses unchanged → `CacheHitRatio` nil, not NaN |

  `collector_test.go` additions:
  | Test | Proves |
  |---|---|
  | `TestCollect_LoadIsSampleFirstThenRates` | interval 1: `Load.SessionsActiveNow` set, no `_per_second`; interval 2: rates present |
  | `TestCollect_ARestartForgetsTheCounters` | a new `Collector` over the same fake: its first bundle has no rates |
  | `TestCollect_QueriesRunOnTheirInterval` | `Top 5, IntervalSeconds 300`, probe 60: `Queries` called at t=0 and t=300; the bundles between carry the t=0 answer |
  | `TestCollect_QueriesAreOffByDefault` | `Top 0`: the fake's `Queries` is never called; no `queries` key |
  | `TestCollect_QueriesUnsupportedIsOneError` | the fake returns `ErrUnsupported`: `load` sent, `queries` absent, one error naming it, and the fake is not asked again until the next queries interval |
  | `TestCollect_TableRatesFromTableCounters` | `SeqScansPerSecond` from two table readings; `DeadRows` passed through |
  | `TestCollect_LoadDisabledSendsNone` | `Enabled false`: no `load`, the fake's `Load` never called, `collection.queries` unchanged from v1 |
  | `TestCollect_WidestLoadFitsTheBundle` | 50 tables with counters + 20 queries of 200-char text: marshalled bundle under 48 KiB |

  `config_test.go`: `load.enabled` defaults true; `queries.top` 21 refused naming 20; `queries.interval_seconds` below the probe interval refused; `TestConfig_ExampleLoads` still loads.

- [ ] **Step 2: Run** `go test ./internal/collector/ ./internal/config/` → build failures.
- [ ] **Step 3: Implement** `rates.go`; the collector: after `Resources`, `Load` when enabled → sample into `d.Load`, `rates(c.counters, now)` merged in, `c.counters = now`; `Queries` when `Top > 0` and due; table counters kept in `tableState` for the next interval's scan rates. The elapsed seconds for a rate are `now.At.Sub(prev.At).Seconds()`, the readings' own clock.
- [ ] **Step 4: Run** `go test -race ./internal/collector/ ./internal/config/` → pass.
- [ ] **Step 5: Commit and push** `feat/collector-load`, draft PR.

---

### Task 4: Outputs, docs, and the end-to-end

**Files:**
- Modify: `internal/report/statsd.go`, `metrics.go`, `report_test.go`; `README.md`; `examples/dbhealth.yml`; `test/e2e/e2e_test.go`; `scripts/release-test.sh`

- [ ] **Step 1: Write the failing tests**: `TestStatsD_LoadGauges` and `TestMetrics_LoadSeries` (every `load` field present → one gauge; absent → none; `dbhealth_queries_calls_per_second{db,query_id}` for the top queries with the text as no label — text is not a label, it is in control), `TestStatsD_DatagramsStayUnderTheMTU` with 20 queries. e2e: `TestE2E_LoadUnderTheWorkers` — against the metering stack with its count workers running, after two intervals `transactions_per_second > 0`, `cache_hit_ratio` in (0, 1], `sessions_active_now >= 1`; with `queries.top: 5`, the metering Postgres has no `pg_stat_statements`, so `queries` is absent and one error names it.
- [ ] **Step 2: Run** them → fail.
- [ ] **Step 3: Implement** the gauges (names: `dbhealth_load_sessions_active_now`, `dbhealth_load_transactions_per_second`, …, `dbhealth_table_dead_rows`, `dbhealth_query_calls_per_second{query_id}`, `dbhealth_query_mean_ms{query_id}`, `dbhealth_query_time_share{query_id}`); the README's "What you get" table gains a **how busy?** row and the `load:` block in the config; the release test checks `dbhealth_load_transactions_per_second` appears on the second scrape.
- [ ] **Step 4: Run** `make test`, `make release-test`, `make test-e2e` with the stack up.
- [ ] **Step 5: Commit and push** `feat/load-outputs`, draft PR.

---

## Self-review

**Spec coverage.** The `load` fields → Task 1 types, Task 2 reads, Task 3 rates. `queries` opt-in, bound, normalization, own interval → Tasks 1–3. Per-table `dead_rows` and scan rates → Tasks 2–3. Field rules (absent not zero, interval's cache ratio, reset) → Task 3's rates tests. Config → Task 3. Cost → counted as before; widest bundle → Tasks 1 and 3. Control: nothing required. Testing section → every row has a task. Other kinds: the `Counters`/`Sample` interface is theirs; collectors out of scope as the spec says.

**Type consistency.** `source.Counters` and `source.Sample` are defined once (Task 2) and consumed by name in Task 3; `wire.DatabaseLoad` field names match the spec's JSON. `rates(prev, now)` returns `*wire.DatabaseLoad` with samples left for the collector to fill.

**Review Focus** → tests: reset (Task 2 real, Task 3 rule), literal text (Task 2 table), reader without the extension (Task 2, Task 3 unsupported), restart (Task 3), jitter (Task 3 elapsed).
