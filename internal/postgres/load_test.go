package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/zeebo/assert"

	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/source"
)

func TestLoad_SampleCountsThisSession(t *testing.T) {
	c := open(t, adminDSN)
	s, _, errs := c.Load(context.Background())
	assert.Equal(t, 0, len(errs))
	assert.NotNil(t, s.SessionsActive)
	assert.That(t, *s.SessionsActive >= 1)
	assert.Equal(t, 0, *s.SessionsWaiting)
	assert.Equal(t, 0, *s.SessionsIdleInTransaction)
	assert.NotNil(t, s.LongestQuerySeconds)
	assert.That(t, *s.LongestQuerySeconds >= 0)
	assert.Nil(t, s.QueriesQueued) // Postgres has no queue
}

func TestLoad_CountersMoveWithWork(t *testing.T) {
	c := open(t, adminDSN)
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, `CREATE TABLE public.work (id serial PRIMARY KEY, at timestamptz NOT NULL DEFAULT now())`)
	assert.NoError(t, err)
	defer c.pool.Exec(ctx, `DROP TABLE public.work`)
	_, before, errs := c.Load(ctx)
	assert.Equal(t, 0, len(errs))
	assert.False(t, before.At.IsZero())
	_, err = c.pool.Exec(ctx, `INSERT INTO public.work (at) SELECT now() FROM generate_series(1, 100) AS g`)
	assert.NoError(t, err)
	_, err = c.pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	assert.NoError(t, err)
	_, after, _ := c.Load(ctx)
	assert.That(t, after.RowsWritten-before.RowsWritten >= 100)
	assert.That(t, after.Commits > before.Commits)
	assert.That(t, after.CacheHits > before.CacheHits)
	// Postgres has no statement counter without pg_stat_statements, which
	// dbhealth does not read: its text can carry a literal.
	assert.Equal(t, int64(-1), after.Queries)
	assert.Equal(t, int64(-1), after.BytesScanned)
	assert.That(t, after.At.After(before.At))
}

func TestLoad_ResetIsVisible(t *testing.T) {
	c := open(t, scratchDSN)
	ctx := context.Background()
	_, _, _ = c.Load(ctx)
	_, err := c.pool.Exec(ctx, `SELECT 1`)
	assert.NoError(t, err)
	_, before, _ := c.Load(ctx)
	_, err = c.pool.Exec(ctx, `SELECT pg_stat_reset()`)
	assert.NoError(t, err)
	_, after, _ := c.Load(ctx)
	assert.That(t, after.Commits < before.Commits)
}

// A role without pg_read_all_stats reads pg_stat_database but sees other
// sessions' state as NULL: the counters come back, the sample does not,
// and the error is the one resources already sends.
func TestLoad_ReaderRoleGetsCountersNotSample(t *testing.T) {
	admin := open(t, adminDSN)
	tx, err := admin.pool.Begin(context.Background())
	assert.NoError(t, err)
	defer tx.Rollback(context.Background())
	c := open(t, readerDSN)
	s, counters, errs := c.Load(context.Background())
	assert.That(t, counters.Commits > 0)
	assert.Nil(t, s.SessionsActive)
	assert.Nil(t, s.LongestQuerySeconds)
	assert.Equal(t, 1, len(errs))
	assert.That(t, strings.Contains(errs[0].Error, "pg_read_all_stats"))
}

func TestTable_CarriesItsCounters(t *testing.T) {
	c := open(t, adminDSN)
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, `CREATE TABLE public.churn (id serial PRIMARY KEY, v int)`)
	assert.NoError(t, err)
	defer c.pool.Exec(ctx, `DROP TABLE public.churn`)
	_, err = c.pool.Exec(ctx, `INSERT INTO public.churn (v) SELECT g FROM generate_series(1, 50) AS g`)
	assert.NoError(t, err)
	_, err = c.pool.Exec(ctx, `DELETE FROM public.churn WHERE v <= 10`)
	assert.NoError(t, err)
	_, err = c.pool.Exec(ctx, `SELECT count(*) FROM public.churn`)
	assert.NoError(t, err)
	_, err = c.pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	assert.NoError(t, err)
	row, counters, err := c.Table(ctx, config.StaticTable{Name: "public.churn"}, false, false)
	assert.NoError(t, err)
	assert.Equal(t, "public.churn", row.Name)
	assert.That(t, counters.DeadRows >= 10)
	assert.That(t, counters.SeqScans >= 1)
	assert.That(t, counters.IdxScans >= 0)
	// The bundle's row carries dead rows directly; the rates are the
	// collector's, from two readings.
	assert.NotNil(t, row.DeadRows)
	assert.Equal(t, counters.DeadRows, *row.DeadRows)
	assert.Nil(t, row.SeqScansPerSecond)
}

// A table reading carries its columns in order, each with its type as
// Postgres names it and whether it is NOT NULL, and its write counters.
func TestTable_CarriesItsColumnsAndWriteCounters(t *testing.T) {
	c := open(t, adminDSN)
	_, counters, err := c.Table(context.Background(), config.StaticTable{Name: "public.events"}, false, false)
	assert.NoError(t, err)
	assert.DeepEqual(t, []source.Column{
		{Name: "id", Type: "integer", NotNull: true},
		{Name: "created_at", Type: "timestamp with time zone", NotNull: true},
	}, counters.Columns)
	assert.That(t, counters.Inserted >= 1000)
	assert.That(t, counters.Updated >= 0)
	assert.That(t, counters.Deleted >= 0)
}
