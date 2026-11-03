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

// instruments registers a gauge for each fact in the catalog and the one
// callback that observes them all from the last collections.
func (m *metrics) instruments() error {
	meter := m.mp.Meter("dbhealth")
	ints := map[string]metric.Int64ObservableGauge{}
	floats := map[string]metric.Float64ObservableGauge{}
	var all []metric.Observable
	for i := range catalog {
		f := &catalog[i]
		if f.Float {
			g, err := meter.Float64ObservableGauge(f.Name, metric.WithDescription(f.Help))
			if err != nil {
				return err
			}
			floats[f.Name] = g
			all = append(all, g)
			continue
		}
		g, err := meter.Int64ObservableGauge(f.Name, metric.WithDescription(f.Help))
		if err != nil {
			return err
		}
		ints[f.Name] = g
		all = append(all, g)
	}
	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		for db, d := range m.last {
			for i := range catalog {
				f := &catalog[i]
				for _, s := range f.samples(d, m.lastAt[db]) {
					attrs := []attribute.KeyValue{attribute.String("db", db)}
					if f.label != "" {
						attrs = append(attrs, attribute.String(f.label, s.label))
					}
					switch v := s.value.(type) {
					case int64:
						o.ObserveInt64(ints[f.Name], v, metric.WithAttributes(attrs...))
					case float64:
						o.ObserveFloat64(floats[f.Name], v, metric.WithAttributes(attrs...))
					}
				}
			}
		}
		return nil
	}, all...)
	return err
}
