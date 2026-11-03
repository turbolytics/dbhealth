package report

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"go.uber.org/zap"
)

// outputs is what one section looks like to a reader of each output: every
// StatsD line and every Prometheus line, sorted, because neither output
// promises an order.
func outputs(t *testing.T, d *wire.Database) []string {
	t.Helper()
	var lines []string
	for _, l := range strings.Split(strings.Join(datagrams("db", d), ""), "\n") {
		if l != "" {
			lines = append(lines, l)
		}
	}
	r, err := New(Config{Metrics: "prometheus", Interval: time.Minute, Log: zap.NewNop(), Now: fixedNow,
		Instances: []Instance{{Name: "db", Collect: func(context.Context) wire.Database { return *d }}}})
	assert.NoError(t, err)
	r.Once(context.Background())
	for _, l := range strings.Split(scrape(t, r), "\n") {
		if strings.HasPrefix(l, "dbhealth_") || strings.HasPrefix(l, "# TYPE dbhealth_") || strings.HasPrefix(l, "# HELP dbhealth_") {
			lines = append(lines, l)
		}
	}
	sort.Strings(lines)
	return lines
}

// matchGolden compares got with a file in the repository. UPDATE_GOLDEN=1
// rewrites the file, and the diff is what a reviewer reads.
func matchGolden(t *testing.T, path, got string) {
	t.Helper()
	if os.Getenv("UPDATE_GOLDEN") != "" {
		assert.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		assert.NoError(t, os.WriteFile(path, []byte(got), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	assert.NoError(t, err)
	if string(want) != got {
		t.Fatalf("%s is out of date. Read the difference, then run UPDATE_GOLDEN=1 go test ./internal/report/\n--- got\n%s", path, got)
	}
}

// Every field holds a value of its own, so a metric that reads its
// neighbour's field, or a metric renamed or dropped, changes this file.
func TestOutputs_EveryFieldReachesItsMetric(t *testing.T) {
	matchGolden(t, "testdata/outputs.golden", strings.Join(outputs(t, filled()), "\n")+"\n")
}

// A field the database could not give is absent, not zero. These are the
// only series a section with nothing but a failed probe sends.
func TestOutputs_AnAbsentFieldSendsNothing(t *testing.T) {
	var got []string
	for _, l := range outputs(t, &wire.Database{}) {
		if !strings.HasPrefix(l, "#") {
			got = append(got, l)
		}
	}
	want := []string{
		"dbhealth.collection.duration_ms:0|g|#db:db",
		"dbhealth.collection.errors:0|g|#db:db",
		"dbhealth.collection.queries:0|g|#db:db",
		"dbhealth.probe.consecutive_failures:0|g|#db:db",
		"dbhealth.probe.latency_ms:0|g|#db:db",
		"dbhealth.probe.ok:0|g|#db:db",
		`dbhealth_collection_duration_ms{db="db"} 0`,
		`dbhealth_collection_errors{db="db"} 0`,
		`dbhealth_collection_queries{db="db"} 0`,
		`dbhealth_last_collect_timestamp_seconds{db="db"} 1.791201605e+09`,
		`dbhealth_probe_consecutive_failures{db="db"} 0`,
		`dbhealth_probe_latency_ms{db="db"} 0`,
		`dbhealth_probe_ok{db="db"} 0`,
	}
	assert.Equal(t, strings.Join(want, "\n"), strings.Join(got, "\n"))
}

// dbhealth_table_rows_exact describes dbhealth_table_rows, so a table with
// no row count sends neither. A slot with no wal_status still says it is
// not lost, because lost is the alarm and its absence reads as healthy.
func TestOutputs_RowsExactNeedsRowsAndLostNeedsNoStatus(t *testing.T) {
	got := strings.Join(outputs(t, &wire.Database{
		Tables:      []wire.DatabaseTable{{Name: "orders", RowsExact: true}},
		Replication: &wire.DatabaseReplication{Slots: []wire.DatabaseReplicationSlot{{Name: "cdc"}}},
	}), "\n")
	assert.False(t, strings.Contains(got, `dbhealth_table_rows_exact{`))
	assert.False(t, strings.Contains(got, "dbhealth.table.rows_exact:"))
	assert.That(t, strings.Contains(got, `dbhealth_replication_slot_lost{db="db",slot="cdc"} 0`))
	assert.That(t, strings.Contains(got, "dbhealth.replication.slot.lost:0|g|#db:db,slot:cdc"))
}
