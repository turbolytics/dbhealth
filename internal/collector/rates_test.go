package collector

import (
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"testing"
	"time"

	"github.com/zeebo/assert"

	"github.com/turbolytics/dbhealth/internal/source"
)

var t0 = time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)

// full is a reading with every counter the kind has.
func full(at time.Time, scale int64) source.Counters {
	return source.Counters{
		At: at, Queries: 100 * scale, Commits: 10 * scale, Rollbacks: scale,
		RowsRead: 1000 * scale, RowsWritten: 50 * scale, BytesScanned: 4096 * scale,
		CacheHits: 900 * scale, CacheMisses: 100 * scale, Deadlocks: 0, TempBytes: 0,
	}
}

func TestRates_FirstReadingHasNone(t *testing.T) {
	assert.Nil(t, rates(source.Counters{}, full(t0, 1)))
}

func TestRates_DividesByTheRealElapsed(t *testing.T) {
	prev := full(t0, 1)
	now := full(t0.Add(58*time.Second), 1)
	now.Commits = prev.Commits + 1160
	l := rates(prev, now)
	assert.NotNil(t, l)
	assert.Equal(t, 20.0, *l.TransactionsPerSecond)
}

func TestRates_AResetIsNoRate(t *testing.T) {
	prev := full(t0, 10)
	now := full(t0.Add(time.Minute), 1) // every counter below the last reading
	assert.Nil(t, rates(prev, now))
	// One counter backwards is enough: a reset is all or nothing.
	now = full(t0.Add(time.Minute), 11)
	now.Deadlocks = -0 // unchanged
	now.TempBytes = prev.TempBytes - 1
	assert.Nil(t, rates(prev, now))
}

func TestRates_CacheHitIsTheIntervals(t *testing.T) {
	prev := full(t0, 1)
	prev.CacheHits, prev.CacheMisses = 1_000_000_000, 0
	now := full(t0.Add(time.Minute), 1)
	now.CacheHits, now.CacheMisses = 1_000_000_100, 100
	l := rates(prev, now)
	assert.Equal(t, 0.5, *l.CacheHitRatio)
}

func TestRates_AKindWithoutACounterSendsNoField(t *testing.T) {
	prev := full(t0, 1)
	now := full(t0.Add(time.Minute), 2)
	prev.Queries, now.Queries = -1, -1
	prev.BytesScanned, now.BytesScanned = -1, -1
	l := rates(prev, now)
	assert.NotNil(t, l)
	assert.Nil(t, l.QueriesPerSecond)
	assert.Nil(t, l.BytesScannedPerSecond)
	assert.NotNil(t, l.TransactionsPerSecond)
	assert.NotNil(t, l.RowsReadPerSecond)
	assert.NotNil(t, l.RowsWrittenPerSecond)
	assert.NotNil(t, l.RollbacksPerSecond)
	assert.NotNil(t, l.DeadlocksPerSecond)
	assert.NotNil(t, l.TempBytesPerSecond)
}

func TestRates_NoReadsNoRatio(t *testing.T) {
	prev := full(t0, 1)
	now := full(t0.Add(time.Minute), 1)
	now.Commits++
	l := rates(prev, now)
	assert.Nil(t, l.CacheHitRatio)
	assert.NotNil(t, l.TransactionsPerSecond)
}

func TestRates_NoTimePassedIsNoRate(t *testing.T) {
	prev := full(t0, 1)
	now := full(t0, 2)
	assert.Nil(t, rates(prev, now))
}

func TestTableRates(t *testing.T) {
	prev := source.TableCounters{DeadRows: 5, SeqScans: 10, IdxScans: 100, Inserted: 1000, Updated: 10, Deleted: -1}
	now := source.TableCounters{DeadRows: 7, SeqScans: 16, IdxScans: 700, Inserted: 1060, Updated: 10, Deleted: -1}
	var row wire.DatabaseTable
	tableRates(&row, prev, now, 60*time.Second)
	assert.Equal(t, 0.1, *row.SeqScansPerSecond)
	assert.Equal(t, 10.0, *row.IndexScansPerSecond)
	assert.Equal(t, 1.0, *row.RowsInsertedPerSecond)
	assert.Equal(t, 0.0, *row.RowsUpdatedPerSecond)
	assert.Nil(t, row.RowsDeletedPerSecond) // a kind without the counter
	// A reset, and no time: none.
	row = wire.DatabaseTable{}
	tableRates(&row, now, prev, time.Minute)
	assert.Nil(t, row.SeqScansPerSecond)
	assert.Nil(t, row.RowsInsertedPerSecond)
	tableRates(&row, prev, now, 0)
	assert.Nil(t, row.SeqScansPerSecond)
}
