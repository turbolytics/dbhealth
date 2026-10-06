package collector

import (
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"

	"github.com/turbolytics/dbhealth/internal/source"
)

// rates turns two readings of the counters into the interval's rates: each
// is (now - prev) over the readings' own elapsed seconds. nil when prev is
// no reading (the first interval), when no time passed, or when any
// counter the kind has went backwards (a stats reset, a restart): a rate
// is absent then, never zero or wrong. A counter the kind does not have,
// -1 in both readings, produces no field.
func rates(prev, now source.Counters) *wire.DatabaseLoad {
	if prev.At.IsZero() || !now.At.After(prev.At) {
		return nil
	}
	secs := now.At.Sub(prev.At).Seconds()
	pairs := [][2]int64{
		{prev.Queries, now.Queries}, {prev.Commits, now.Commits}, {prev.Rollbacks, now.Rollbacks},
		{prev.RowsRead, now.RowsRead}, {prev.RowsWritten, now.RowsWritten}, {prev.BytesScanned, now.BytesScanned},
		{prev.CacheHits, now.CacheHits}, {prev.CacheMisses, now.CacheMisses},
		{prev.Deadlocks, now.Deadlocks}, {prev.TempBytes, now.TempBytes},
	}
	for _, p := range pairs {
		if p[0] >= 0 && p[1] < p[0] {
			return nil
		}
	}
	per := func(a, b int64) *float64 {
		if a < 0 || b < 0 {
			return nil
		}
		v := float64(b-a) / secs
		return &v
	}
	l := &wire.DatabaseLoad{
		QueriesPerSecond:      per(prev.Queries, now.Queries),
		TransactionsPerSecond: per(prev.Commits, now.Commits),
		RollbacksPerSecond:    per(prev.Rollbacks, now.Rollbacks),
		RowsReadPerSecond:     per(prev.RowsRead, now.RowsRead),
		RowsWrittenPerSecond:  per(prev.RowsWritten, now.RowsWritten),
		BytesScannedPerSecond: per(prev.BytesScanned, now.BytesScanned),
		DeadlocksPerSecond:    per(prev.Deadlocks, now.Deadlocks),
		TempBytesPerSecond:    per(prev.TempBytes, now.TempBytes),
	}
	// The interval's ratio, not the server's lifetime one; no reads, no
	// ratio.
	if prev.CacheHits >= 0 && prev.CacheMisses >= 0 {
		hits, misses := now.CacheHits-prev.CacheHits, now.CacheMisses-prev.CacheMisses
		if hits+misses > 0 {
			r := float64(hits) / float64(hits+misses)
			l.CacheHitRatio = &r
		}
	}
	return l
}

// tableRates is a table's scan rates from two readings, by the same rules.
func tableRates(prev, now source.TableCounters, elapsed time.Duration) (seq, idx *float64) {
	secs := elapsed.Seconds()
	if secs <= 0 {
		return nil, nil
	}
	rate := func(a, b int64) *float64 {
		if a < 0 || b < 0 || b < a {
			return nil
		}
		v := float64(b-a) / secs
		return &v
	}
	return rate(prev.SeqScans, now.SeqScans), rate(prev.IdxScans, now.IdxScans)
}
