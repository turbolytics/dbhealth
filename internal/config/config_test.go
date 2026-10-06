package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zeebo/assert"
)

// minimal is the smallest file that passes every rule.
const minimal = `
databases:
  - kind: postgres
    dsn: postgres://u@h:5432/d
report:
  statsd: localhost:8125
`

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "dbhealth.yml")
	assert.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

// loadErr loads body and returns the error's text; it fails the test when
// Load accepted the file.
func loadErr(t *testing.T, body string) string {
	t.Helper()
	_, err := Load(write(t, body))
	if err == nil {
		t.Fatalf("Load accepted a file that breaks a rule:\n%s", body)
	}
	return err.Error()
}

func contains(t *testing.T, s string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(s, w) {
			t.Fatalf("%q does not mention %q", s, w)
		}
	}
}

func TestConfig_ExampleLoads(t *testing.T) {
	t.Setenv("DBHEALTH_PRIMARY_DSN", "postgres://u:p@primary:5432/billing")
	t.Setenv("DBHEALTH_REPLICA_A_DSN", "postgres://u:p@replica-a:5432/billing")
	t.Setenv("TURBOSTATS_CREDENTIAL", "cred")
	f, err := Load("../../examples/dbhealth.yml")
	assert.NoError(t, err)
	assert.Equal(t, 2, len(f.Databases))
	assert.Equal(t, "billing-primary", f.Databases[0].Name)
	assert.Equal(t, "billing", f.Databases[1].Cluster)
	assert.Equal(t, 60, f.Probe.IntervalSeconds)
	assert.Equal(t, "estimate", f.Tables.Rows)
	assert.Equal(t, "https://ingest.turbolytics.io/v1/turbostats", f.Report.To)
	assert.Equal(t, "cred", f.Report.Credential)
	assert.DeepEqual(t, []string{"sqlflow_*", "*_tmp"}, f.Tables.Discover.Exclude)
}

func TestConfig_DefaultsApply(t *testing.T) {
	f, err := Load(write(t, minimal))
	assert.NoError(t, err)
	assert.Equal(t, 60, f.Probe.IntervalSeconds)
	assert.Equal(t, 5, f.Probe.TimeoutSeconds)
	assert.Equal(t, "estimate", f.Tables.Rows)
	assert.Equal(t, 60, f.Tables.FreshnessIntervalSeconds)
	assert.Equal(t, 3600, f.Tables.RowsExactIntervalSeconds)
	assert.NotNil(t, f.Tables.Discover)
	assert.Equal(t, 0, len(f.Tables.Static))
	assert.DeepEqual(t, []string{"updated_at", "created_at", "minute", "ts"}, f.Tables.Discover.FreshnessColumns)
	assert.Equal(t, 50, f.Tables.Discover.MaxTables)
}

func TestConfig_NoDatabasesIsAnError(t *testing.T) {
	contains(t, loadErr(t, "databases: []\nreport:\n  statsd: localhost:8125\n"), "databases")
}

func TestConfig_KindMustBePostgresInV1(t *testing.T) {
	body := strings.Replace(minimal, "kind: postgres", "kind: mysql", 1)
	contains(t, loadErr(t, body), "mysql", "not in this version")
}

func TestConfig_UnknownKindIsAnError(t *testing.T) {
	body := strings.Replace(minimal, "kind: postgres", "kind: oracle", 1)
	contains(t, loadErr(t, body), "oracle")
}

func TestConfig_DSNIsRequired(t *testing.T) {
	body := strings.Replace(minimal, "    dsn: postgres://u@h:5432/d\n", "", 1)
	contains(t, loadErr(t, body), "dsn")
}

func TestConfig_NameDefaultsToTarget(t *testing.T) {
	body := strings.Replace(minimal, "dsn: postgres://u@h:5432/d", "dsn: postgres://u:p@h:5432/d", 1)
	f, err := Load(write(t, body))
	assert.NoError(t, err)
	assert.Equal(t, "h:5432/d", f.Databases[0].Name)
}

func TestConfig_DuplicateNamesAreAnError(t *testing.T) {
	body := `
databases:
  - kind: postgres
    dsn: postgres://u@h:5432/d
    name: x
  - kind: postgres
    dsn: postgres://u@h:5432/e
    name: x
report:
  statsd: localhost:8125
`
	contains(t, loadErr(t, body), "duplicate", "x")
}

func TestConfig_DiscoverAndStaticAreExclusive(t *testing.T) {
	body := minimal + `
tables:
  discover:
    schemas: [public]
  static:
    - name: public.t
`
	contains(t, loadErr(t, body), "discover", "static")
}

func TestConfig_RowsIsEstimateOrExact(t *testing.T) {
	contains(t, loadErr(t, minimal+"tables:\n  rows: all\n"), "rows", "all")
}

func TestConfig_StaticTableNeedsAName(t *testing.T) {
	contains(t, loadErr(t, minimal+"tables:\n  static:\n    - freshness_column: x\n"), "name")
}

func TestConfig_IntervalsArePositive(t *testing.T) {
	contains(t, loadErr(t, minimal+"probe:\n  interval_seconds: 0\n"), "interval_seconds")
}

func TestConfig_ExactIntervalNotBelowProbe(t *testing.T) {
	err := loadErr(t, minimal+"probe:\n  interval_seconds: 60\ntables:\n  rows_exact_interval_seconds: 10\n")
	contains(t, err, "rows_exact_interval_seconds", "at least the probe interval")
}

func TestConfig_EnvIsExpanded(t *testing.T) {
	t.Setenv("X", "postgres://a@b/c")
	body := strings.Replace(minimal, "dsn: postgres://u@h:5432/d", `dsn: "{{ X }}"`, 1)
	f, err := Load(write(t, body))
	assert.NoError(t, err)
	assert.Equal(t, "postgres://a@b/c", f.Databases[0].DSN)
}

func TestConfig_UnsetEnvIsAnError(t *testing.T) {
	os.Unsetenv("NOPE")
	body := strings.Replace(minimal, "dsn: postgres://u@h:5432/d", `dsn: "{{ NOPE }}"`, 1)
	contains(t, loadErr(t, body), "NOPE")
}

func TestConfig_ReportToIsRequiredUnlessStatsD(t *testing.T) {
	body := strings.Replace(minimal, "report:\n  statsd: localhost:8125\n", "", 1)
	contains(t, loadErr(t, body), "report")
	err := loadErr(t, body)
	if !strings.Contains(err, "report.to") && !strings.Contains(err, "report.statsd") {
		t.Fatalf("%q names neither report.to nor report.statsd", err)
	}
}

func TestConfig_ReportToNeedsACredential(t *testing.T) {
	body := strings.Replace(minimal, "report:\n  statsd: localhost:8125\n", "report:\n  to: https://control.example\n", 1)
	contains(t, loadErr(t, body), "report.credential")
}

func TestConfig_UnknownKeyIsAnError(t *testing.T) {
	contains(t, loadErr(t, minimal+"probe:\n  interval: 5\n"), "interval")
}

func TestConfig_LoadNeverLogsOrReturnsThePassword(t *testing.T) {
	body := `
databases:
  - kind: oracle
    dsn: postgres://u:secret@h:5432/d
report:
  statsd: localhost:8125
`
	err := loadErr(t, body)
	if strings.Contains(err, "secret") {
		t.Fatalf("the error carries the password: %q", err)
	}
}

func TestRedactedTarget(t *testing.T) {
	cases := []struct{ dsn, want string }{
		{"postgres://u:secret@pg.internal:5432/billing?sslmode=require", "pg.internal:5432/billing"},
		{"postgresql://u@pg.internal/billing", "pg.internal:5432/billing"},
		{"host=pg.internal port=5433 dbname=billing user=u password=secret", "pg.internal:5433/billing"},
		{"host=pg.internal dbname=billing", "pg.internal:5432/billing"},
	}
	for _, c := range cases {
		got, err := RedactedTarget(c.dsn)
		assert.NoError(t, err)
		assert.Equal(t, c.want, got)
	}
	_, err := RedactedTarget("not a dsn")
	assert.Error(t, err)
}

func TestRedactedTarget_ErrorNeverCarriesTheDSN(t *testing.T) {
	_, err := RedactedTarget("host=pg.internal password=secret port=notaport")
	assert.Error(t, err)
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("the error carries the password: %q", err)
	}
}

// Findings from the review of #2.

func TestRedactedTarget_AnUnescapedPasswordIsRefused(t *testing.T) {
	for _, dsn := range []string{"postgres://u:12/ss@h/d", "postgres://u:123#x@h/d"} {
		got, err := RedactedTarget(dsn)
		if err == nil {
			t.Fatalf("%q was accepted as %q; part of the password is in it", dsn, got)
		}
		contains(t, err.Error(), "escape")
		if strings.Contains(err.Error(), "12") {
			t.Fatalf("the error carries the password: %q", err)
		}
	}
}

func TestRedactedTarget_SchemeMustBePostgres(t *testing.T) {
	_, err := RedactedTarget("mysql://h/d")
	assert.Error(t, err)
	contains(t, err.Error(), "mysql")
}

func TestRedactedTarget_IPv6HostKeepsItsBrackets(t *testing.T) {
	got, err := RedactedTarget("postgres://u:p@[::1]:5432/d")
	assert.NoError(t, err)
	assert.Equal(t, "[::1]:5432/d", got)
}

func TestRedactedTarget_KeyValueWithASchemeInsideAValue(t *testing.T) {
	got, err := RedactedTarget("host=h password=a://b dbname=d")
	assert.NoError(t, err)
	assert.Equal(t, "h:5432/d", got)
}

func TestConfig_YamlErrorsDoNotQuoteTheValue(t *testing.T) {
	err := loadErr(t, minimal+"probe:\n  interval_seconds: s3cr3t\n")
	if strings.Contains(err, "s3cr3t") {
		t.Fatalf("the error carries the value: %q", err)
	}
	contains(t, err, "line 8")
}

func TestConfig_EnvValueIsNotParsedAsYaml(t *testing.T) {
	t.Setenv("X", `host=h dbname=d password=a\nb"c`)
	body := strings.Replace(minimal, "dsn: postgres://u@h:5432/d", `dsn: "{{ X }}"`, 1)
	f, err := Load(write(t, body))
	assert.NoError(t, err)
	assert.Equal(t, `host=h dbname=d password=a\nb"c`, f.Databases[0].DSN)
}

func TestConfig_UnsetEnvInACommentIsFine(t *testing.T) {
	os.Unsetenv("NOPE")
	_, err := Load(write(t, minimal+"# {{ NOPE }} is a comment\n"))
	assert.NoError(t, err)
}

func TestConfig_UnknownKeyErrorNamesTheFileLine(t *testing.T) {
	body := minimal + "tables:\n  static:\n    - name: public.t\n      fresh: x\n"
	contains(t, loadErr(t, body), "line 10", "fresh")
}

func TestConfig_AnchorsWork(t *testing.T) {
	body := `
defaults: &d
  interval_seconds: 30
databases:
  - kind: postgres
    dsn: postgres://u@h:5432/d
probe: *d
report:
  statsd: localhost:8125
`
	_, err := Load(write(t, body))
	// `defaults` is an unknown top-level key; the anchor itself must not
	// be what fails.
	contains(t, err.Error(), "defaults")
	body = strings.Replace(body, "defaults: &d\n  interval_seconds: 30\n", "", 1)
	body = strings.Replace(body, "probe: *d", "probe: &d\n  interval_seconds: 30\ntables:\n  freshness_interval_seconds: 30", 1)
	f, err := Load(write(t, body))
	assert.NoError(t, err)
	assert.Equal(t, 30, f.Probe.IntervalSeconds)
}

func TestConfig_DuplicateStaticNamesAreAnError(t *testing.T) {
	body := minimal + "tables:\n  static:\n    - name: public.t\n    - name: public.t\n"
	contains(t, loadErr(t, body), "static", "public.t", "duplicate")
}

func TestConfig_BadExcludeGlobIsAnError(t *testing.T) {
	contains(t, loadErr(t, minimal+"tables:\n  discover:\n    exclude: [\"[\"]\n"), "exclude", "[")
}

func TestConfig_ExactIntervalDefaultFollowsALongProbe(t *testing.T) {
	f, err := Load(write(t, minimal+"probe:\n  interval_seconds: 7200\n"))
	assert.NoError(t, err)
	assert.Equal(t, 7200, f.Tables.RowsExactIntervalSeconds)
}

func TestConfig_MaxTablesIsBoundedByTheBundle(t *testing.T) {
	contains(t, loadErr(t, minimal+"tables:\n  discover:\n    max_tables: 51\n"), "max_tables", "50")
	var static strings.Builder
	static.WriteString(minimal + "tables:\n  static:\n")
	for i := 0; i < 51; i++ {
		fmt.Fprintf(&static, "    - name: public.t%d\n", i)
	}
	contains(t, loadErr(t, static.String()), "static", "50")
}

// validate must refuse what run cannot open: pgx reads the whole DSN,
// so RedactedTarget asks it, and a bad parameter fails here, with a
// message that names the parameter and not the DSN.
func TestConfig_ValidateRefusesWhatPgxCannotOpen(t *testing.T) {
	body := strings.Replace(minimal, "dsn: postgres://u@h:5432/d", "dsn: postgres://u:secret@h:5432/d?sslmode=bogus", 1)
	err := loadErr(t, body)
	contains(t, err, "dsn", "sslmode")
	if strings.Contains(err, "secret") {
		t.Fatalf("the error carries the password: %q", err)
	}
}

// A database with no cluster is its own cluster: control groups endpoints
// by it, and a report without one is refused.
func TestConfig_ClusterDefaultsToName(t *testing.T) {
	f, err := Load(write(t, minimal))
	assert.NoError(t, err)
	assert.Equal(t, "h:5432/d", f.Databases[0].Name)
	assert.Equal(t, "h:5432/d", f.Databases[0].Cluster)
}

// With no file, the environment watches one database with discovery on and
// reports to control's ingest.
func TestFromEnv(t *testing.T) {
	t.Setenv("DBHEALTH_DSN", "postgres://u:secret@pg.internal:5432/billing")
	t.Setenv("DBHEALTH_KEY", "sfp_key")
	f, err := FromEnv()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(f.Databases))
	db := f.Databases[0]
	assert.Equal(t, "postgres", db.Kind)
	assert.Equal(t, "billing", db.Name)
	assert.Equal(t, "billing", db.Cluster)
	assert.NotNil(t, f.Tables.Discover)
	assert.Equal(t, 60, f.Probe.IntervalSeconds)
	assert.Equal(t, DefaultReportTo, f.Report.To)
	assert.Equal(t, "sfp_key", f.Report.Credential)

	t.Setenv("DBHEALTH_NAME", "billing-primary")
	t.Setenv("DBHEALTH_CLUSTER", "billing")
	t.Setenv("DBHEALTH_REPORT_TO", "http://localhost:8090")
	f, err = FromEnv()
	assert.NoError(t, err)
	assert.Equal(t, "billing-primary", f.Databases[0].Name)
	assert.Equal(t, "billing", f.Databases[0].Cluster)
	assert.Equal(t, "http://localhost:8090", f.Report.To)
}

// Without a DSN there is nothing to watch, and the error names the
// variable. A DSN that does not parse is refused without its password.
func TestFromEnv_Refusals(t *testing.T) {
	t.Setenv("DBHEALTH_DSN", "")
	_, err := FromEnv()
	assert.Error(t, err)
	contains(t, err.Error(), "DBHEALTH_DSN")

	t.Setenv("DBHEALTH_DSN", "postgres://u:secret@pg.internal:notaport/billing")
	t.Setenv("DBHEALTH_KEY", "sfp_key")
	_, err = FromEnv()
	assert.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "secret"))
}

func TestConfig_MetricsIsAnOutputOnItsOwn(t *testing.T) {
	body := strings.Replace(minimal, "report:\n  statsd: localhost:8125\n", "report:\n  metrics: prometheus\n", 1)
	f, err := Load(write(t, body))
	assert.NoError(t, err)
	assert.Equal(t, "prometheus", f.Report.Metrics)
	assert.Equal(t, ":8000", f.Report.Listen)
	err2 := loadErr(t, strings.Replace(minimal, "report:\n  statsd: localhost:8125\n", "", 1))
	contains(t, err2, "report.metrics")
}

func TestConfig_MetricsIsPrometheusOrOTLP(t *testing.T) {
	contains(t, loadErr(t, minimal+"  metrics: graphite\n"), "report.metrics", "graphite", "prometheus", "otlp")
	contains(t, loadErr(t, minimal+"  metrics: otlp\n"), "report.otlp")
	f, err := Load(write(t, minimal+"  metrics: otlp\n  otlp: http://collector:4318\n  listen: 127.0.0.1:9100\n"))
	assert.NoError(t, err)
	assert.Equal(t, "http://collector:4318", f.Report.OTLP)
	assert.Equal(t, "127.0.0.1:9100", f.Report.Listen)
}

func TestConfig_FromEnvReadsMetrics(t *testing.T) {
	t.Setenv("DBHEALTH_DSN", "postgres://u:p@h:5432/d")
	t.Setenv("DBHEALTH_KEY", "sfc_x")
	t.Setenv("DBHEALTH_METRICS", "otlp")
	t.Setenv("DBHEALTH_OTLP", "http://datadog-agent:4318")
	t.Setenv("DBHEALTH_LISTEN", "127.0.0.1:9100")
	f, err := FromEnv()
	assert.NoError(t, err)
	assert.Equal(t, "otlp", f.Report.Metrics)
	assert.Equal(t, "http://datadog-agent:4318", f.Report.OTLP)
	assert.Equal(t, "127.0.0.1:9100", f.Report.Listen)
}
