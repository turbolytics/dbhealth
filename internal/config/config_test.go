package config

import (
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
	assert.Equal(t, "https://control.turbolytics.io", f.Report.To)
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
