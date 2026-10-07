package report

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// metrics is the bundle's facts as OpenTelemetry gauges, the way sql-flow
// records its own: one meter, observable gauges that read the last
// collection of each database when a reader asks, and the reader chosen
// by the exporter name. Prometheus serves /metrics; otlp pushes every
// interval to a collector. A field absent from the last collection is
// not observed, so it is absent from the scrape rather than stale.
type metrics struct {
	mp       *sdkmetric.MeterProvider
	registry *prom.Registry // Prometheus only
	log      *zap.Logger
	mu       sync.Mutex
	last     map[string]*wire.Database
	lastAt   map[string]time.Time
	failing  bool
}

// loggingExporter wraps the OTLP exporter so a failed export is a line in
// dbhealth's log, once per run of failures, rather than the SDK's global
// handler printing to stderr every interval.
type loggingExporter struct {
	sdkmetric.Exporter
	m *metrics
}

func (e *loggingExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	err := e.Exporter.Export(ctx, rm)
	e.m.mu.Lock()
	defer e.m.mu.Unlock()
	switch {
	case err != nil && !e.m.failing:
		e.m.failing = true
		e.m.log.Warn("metrics export is failing", zap.Error(err))
	case err != nil:
		e.m.log.Debug("metrics export failed", zap.Error(err))
	case e.m.failing:
		e.m.failing = false
		e.m.log.Info("metrics export recovered")
	}
	return err
}

// otlpPath is where OTLP/HTTP takes metrics; a collector's origin is
// enough in the config.
const otlpPath = "/v1/metrics"

// newMetrics builds the provider for exporter: "prometheus", or "otlp"
// with the collector's endpoint, as "http://collector:4318". An empty
// exporter is nil, no metrics. An export that fails is logged through
// log, once per run of failures.
func newMetrics(exporter, endpoint string, interval time.Duration, log *zap.Logger) (*metrics, error) {
	m := &metrics{last: map[string]*wire.Database{}, lastAt: map[string]time.Time{}, log: log}
	var reader sdkmetric.Reader
	switch strings.ToLower(strings.TrimSpace(exporter)) {
	case "":
		return nil, nil
	case "prometheus":
		m.registry = prom.NewRegistry()
		exp, err := prometheus.New(prometheus.WithRegisterer(m.registry),
			prometheus.WithoutScopeInfo(), prometheus.WithoutTargetInfo(), prometheus.WithoutUnits())
		if err != nil {
			return nil, fmt.Errorf("prometheus exporter: %w", err)
		}
		reader = exp
	case "otlp":
		if endpoint == "" {
			return nil, errors.New("otlp: report.otlp names the collector, as http://collector:4318")
		}
		u, err := url.Parse(endpoint)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, errors.New("otlp: not a URL; the collector, as http://collector:4318")
		}
		if u.Path == "" || u.Path == "/" {
			u.Path = otlpPath
		}
		exp, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(u.String()))
		if err != nil {
			return nil, fmt.Errorf("otlp exporter: %w", err)
		}
		reader = sdkmetric.NewPeriodicReader(&loggingExporter{Exporter: exp, m: m}, sdkmetric.WithInterval(interval))
	default:
		return nil, fmt.Errorf("unsupported metrics exporter: %q (supported: prometheus, otlp)", exporter)
	}
	m.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	if err := m.instruments(); err != nil {
		return nil, err
	}
	return m, nil
}

// Handler serves /metrics for the Prometheus exporter; nil for the others.
func (m *metrics) handler() http.Handler {
	if m == nil || m.registry == nil {
		return nil
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *metrics) shutdown(ctx context.Context) {
	if m != nil && m.mp != nil {
		_ = m.mp.Shutdown(ctx)
	}
}

// set records one database's latest collection for the next observation.
func (m *metrics) set(db string, d *wire.Database, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.last[db] = d
	m.lastAt[db] = at
}

// instruments registers every gauge and the one callback that observes
// them all from the last collections.
func (m *metrics) instruments() error {
	meter := m.mp.Meter("dbhealth")
	i64 := func(name, desc string) (metric.Int64ObservableGauge, error) {
		return meter.Int64ObservableGauge(name, metric.WithDescription(desc))
	}
	f64 := func(name, desc string) (metric.Float64ObservableGauge, error) {
		return meter.Float64ObservableGauge(name, metric.WithDescription(desc))
	}
	var err error
	g := map[string]metric.Int64ObservableGauge{}
	for _, it := range [][2]string{
		{"dbhealth_probe_ok", "1 when the last probe answered"},
		{"dbhealth_probe_latency_ms", "the last probe's round trip"},
		{"dbhealth_probe_consecutive_failures", "probes failed in a row"},
		{"dbhealth_connections_used", "connections in use"},
		{"dbhealth_connections_max", "max_connections"},
		{"dbhealth_connections_waiting", "connections waiting on a lock"},
		{"dbhealth_size_bytes", "the database on disk"},
		{"dbhealth_oldest_transaction_seconds", "age of the oldest open transaction"},
		{"dbhealth_memory_shared_buffers_bytes", "shared_buffers"},
		{"dbhealth_table_rows", "rows, estimated unless dbhealth_table_rows_exact is 1"},
		{"dbhealth_table_rows_exact", "1 when dbhealth_table_rows is a count(*)"},
		{"dbhealth_table_size_bytes", "the table on disk"},
		{"dbhealth_table_newest_at_seconds", "max of the freshness column, Unix seconds"},
		{"dbhealth_table_dead_rows", "rows deleted or updated and not yet reclaimed"},
		{"dbhealth_table_schema_changes", "columns added, dropped or retyped since the last report"},
		{"dbhealth_collection_queries", "queries the last interval ran"},
		{"dbhealth_collection_duration_ms", "what the last interval cost"},
		{"dbhealth_collection_errors", "queries that failed in the last interval"},
		{"dbhealth_last_collect_timestamp_seconds", "when the last collection ran, Unix seconds"},
	} {
		if g[it[0]], err = i64(it[0], it[1]); err != nil {
			return err
		}
	}
	// Load, every field a float gauge so one callback serves ints and
	// rates alike; the integer samples marshal as integers anyway.
	loadGauges := map[string]metric.Float64ObservableGauge{}
	for _, it := range [][2]string{
		{"sessions_active_now", "sessions executing a statement"},
		{"sessions_idle_in_transaction_now", "sessions idle inside an open transaction"},
		{"sessions_waiting_now", "sessions blocked on a lock, a queue or a resource"},
		{"queries_queued_now", "queries waiting to start"},
		{"longest_query_seconds", "age of the oldest statement still running"},
		{"queries_per_second", "the interval's queries"},
		{"transactions_per_second", "the interval's commits"},
		{"rollbacks_per_second", "the interval's rollbacks"},
		{"rows_read_per_second", "rows returned to clients"},
		{"rows_written_per_second", "rows inserted, updated and deleted"},
		{"bytes_scanned_per_second", "storage read to answer queries"},
		{"cache_hit_ratio", "the interval's reads served from memory, 0..1"},
		{"deadlocks_per_second", "the interval's deadlocks"},
		{"temp_bytes_per_second", "sorts and hashes spilling to disk"},
	} {
		if loadGauges[it[0]], err = f64("dbhealth_load_"+it[0], it[1]); err != nil {
			return err
		}
	}
	tableSeq, err := f64("dbhealth_table_seq_scans_per_second", "whole-table reads")
	if err != nil {
		return err
	}
	tableIdx, err := f64("dbhealth_table_index_scans_per_second", "index reads")
	if err != nil {
		return err
	}
	tableIns, err := f64("dbhealth_table_rows_inserted_per_second", "rows inserted, from the table's own counter")
	if err != nil {
		return err
	}
	tableUpd, err := f64("dbhealth_table_rows_updated_per_second", "rows updated, from the table's own counter")
	if err != nil {
		return err
	}
	tableDel, err := f64("dbhealth_table_rows_deleted_per_second", "rows deleted, from the table's own counter")
	if err != nil {
		return err
	}
	lag, err := f64("dbhealth_replication_lag_seconds", "how far behind this endpoint is")
	if err != nil {
		return err
	}
	replicaLag, err := f64("dbhealth_replication_replica_lag_seconds", "a replica's lag as its primary sees it")
	if err != nil {
		return err
	}
	all := []metric.Observable{lag, replicaLag, tableSeq, tableIdx, tableIns, tableUpd, tableDel}
	for _, v := range g {
		all = append(all, v)
	}
	for _, v := range loadGauges {
		all = append(all, v)
	}
	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		for db, d := range m.last {
			base := metric.WithAttributes(attribute.String("db", db))
			o.ObserveInt64(g["dbhealth_probe_ok"], int64(boolInt(d.Probe.OK)), base)
			o.ObserveInt64(g["dbhealth_probe_latency_ms"], d.Probe.LatencyMs, base)
			o.ObserveInt64(g["dbhealth_probe_consecutive_failures"], int64(d.Probe.ConsecutiveFailures), base)
			o.ObserveInt64(g["dbhealth_last_collect_timestamp_seconds"], m.lastAt[db].Unix(), base)
			if r := d.Resources; r != nil {
				if c := r.Connections; c != nil {
					o.ObserveInt64(g["dbhealth_connections_used"], int64(c.Used), base)
					o.ObserveInt64(g["dbhealth_connections_max"], int64(c.Max), base)
					o.ObserveInt64(g["dbhealth_connections_waiting"], int64(c.Waiting), base)
				}
				if r.SizeBytes != nil {
					o.ObserveInt64(g["dbhealth_size_bytes"], *r.SizeBytes, base)
				}
				if r.OldestTransactionSeconds != nil {
					o.ObserveInt64(g["dbhealth_oldest_transaction_seconds"], *r.OldestTransactionSeconds, base)
				}
				if r.Memory != nil {
					o.ObserveInt64(g["dbhealth_memory_shared_buffers_bytes"], r.Memory.SharedBuffersBytes, base)
				}
			}
			for _, t := range d.Tables {
				tags := metric.WithAttributes(attribute.String("db", db), attribute.String("table", t.Name))
				if t.Rows != nil {
					o.ObserveInt64(g["dbhealth_table_rows"], *t.Rows, tags)
					o.ObserveInt64(g["dbhealth_table_rows_exact"], int64(boolInt(t.RowsExact)), tags)
				}
				o.ObserveInt64(g["dbhealth_table_size_bytes"], t.SizeBytes, tags)
				if t.NewestAt != nil {
					o.ObserveInt64(g["dbhealth_table_newest_at_seconds"], t.NewestAt.Unix(), tags)
				}
				if t.DeadRows != nil {
					o.ObserveInt64(g["dbhealth_table_dead_rows"], *t.DeadRows, tags)
				}
				if t.SeqScansPerSecond != nil {
					o.ObserveFloat64(tableSeq, *t.SeqScansPerSecond, tags)
				}
				if t.IndexScansPerSecond != nil {
					o.ObserveFloat64(tableIdx, *t.IndexScansPerSecond, tags)
				}
				if t.RowsInsertedPerSecond != nil {
					o.ObserveFloat64(tableIns, *t.RowsInsertedPerSecond, tags)
				}
				if t.RowsUpdatedPerSecond != nil {
					o.ObserveFloat64(tableUpd, *t.RowsUpdatedPerSecond, tags)
				}
				if t.RowsDeletedPerSecond != nil {
					o.ObserveFloat64(tableDel, *t.RowsDeletedPerSecond, tags)
				}
				o.ObserveInt64(g["dbhealth_table_schema_changes"], int64(len(t.SchemaChanges)), tags)
			}
			if d.Load != nil {
				for _, f := range loadFields(d.Load) {
					switch v := f.value.(type) {
					case int:
						o.ObserveFloat64(loadGauges[f.name], float64(v), base)
					case float64:
						o.ObserveFloat64(loadGauges[f.name], v, base)
					}
				}
			}
			if r := d.Replication; r != nil {
				if r.LagSeconds != nil {
					o.ObserveFloat64(lag, *r.LagSeconds, base)
				}
				for _, rep := range r.Replicas {
					if rep.LagSeconds != nil {
						o.ObserveFloat64(replicaLag, *rep.LagSeconds,
							metric.WithAttributes(attribute.String("db", db), attribute.String("replica", rep.Name)))
					}
				}
			}
			o.ObserveInt64(g["dbhealth_collection_queries"], int64(d.Collection.Queries), base)
			o.ObserveInt64(g["dbhealth_collection_duration_ms"], d.Collection.DurationMs, base)
			o.ObserveInt64(g["dbhealth_collection_errors"], int64(len(d.Collection.Errors)), base)
		}
		return nil
	}, all...)
	return err
}
