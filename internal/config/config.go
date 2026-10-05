// Package config reads dbhealth.yml: the databases to watch, how often, which
// tables, and where to report. Load applies the defaults and checks every
// rule, and names the key and the value in every error. No error, and no
// log line, ever carries a DSN.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/yaml.v3"
)

// File is dbhealth.yml.
type File struct {
	Databases []Database `yaml:"databases"`
	Probe     Probe      `yaml:"probe"`
	Tables    Tables     `yaml:"tables"`
	Report    Report     `yaml:"report"`
}

// Database is one endpoint to watch: one instance in control.
type Database struct {
	Kind string `yaml:"kind"`
	// DSN is the connection string. It stays in the process.
	DSN string `yaml:"dsn"`
	// Name is the instance name control shows. Defaults to the redacted
	// target, host:port/database.
	Name string `yaml:"name"`
	// Cluster groups a primary with its replicas.
	Cluster string `yaml:"cluster"`
}

// Probe is one round trip per interval, per database.
type Probe struct {
	IntervalSeconds int `yaml:"interval_seconds"`
	TimeoutSeconds  int `yaml:"timeout_seconds"`
}

// Tables says which tables to watch and how. Exactly one of Discover and
// Static.
type Tables struct {
	Discover *Discover     `yaml:"discover"`
	Static   []StaticTable `yaml:"static"`
	// Rows is "estimate" or "exact", the default for every table.
	Rows                     string `yaml:"rows"`
	FreshnessIntervalSeconds int    `yaml:"freshness_interval_seconds"`
	RowsExactIntervalSeconds int    `yaml:"rows_exact_interval_seconds"`
}

// Discover watches every table in the given schemas, up to MaxTables.
type Discover struct {
	Schemas []string `yaml:"schemas"`
	// Exclude is globs on the table name.
	Exclude []string `yaml:"exclude"`
	// FreshnessColumns is tried in order; the first that exists is the
	// table's.
	FreshnessColumns []string `yaml:"freshness_columns"`
	MaxTables        int      `yaml:"max_tables"`
}

// StaticTable is one named table.
type StaticTable struct {
	Name            string `yaml:"name"`
	FreshnessColumn string `yaml:"freshness_column"`
	// Rows overrides Tables.Rows for this table: "estimate" or "exact".
	Rows string `yaml:"rows"`
}

// Report is where the facts go: control, StatsD, or both.
type Report struct {
	To         string `yaml:"to"`
	Credential string `yaml:"credential"`
	StatsD     string `yaml:"statsd"`
}

// UnmarshalYAML refuses an interval written as zero or negative. An absent
// key still takes the default; a written 0 is a mistake, not a request.
func (p *Probe) UnmarshalYAML(n *yaml.Node) error {
	var raw struct {
		IntervalSeconds *int `yaml:"interval_seconds"`
		TimeoutSeconds  *int `yaml:"timeout_seconds"`
	}
	if err := decodeKnown(n, &raw, "probe"); err != nil {
		return err
	}
	var err error
	if p.IntervalSeconds, err = positive(raw.IntervalSeconds, "probe.interval_seconds"); err != nil {
		return err
	}
	p.TimeoutSeconds, err = positive(raw.TimeoutSeconds, "probe.timeout_seconds")
	return err
}

func (t *Tables) UnmarshalYAML(n *yaml.Node) error {
	var raw struct {
		Discover                 *Discover     `yaml:"discover"`
		Static                   []StaticTable `yaml:"static"`
		Rows                     string        `yaml:"rows"`
		FreshnessIntervalSeconds *int          `yaml:"freshness_interval_seconds"`
		RowsExactIntervalSeconds *int          `yaml:"rows_exact_interval_seconds"`
	}
	if err := decodeKnown(n, &raw, "tables"); err != nil {
		return err
	}
	t.Discover, t.Static, t.Rows = raw.Discover, raw.Static, raw.Rows
	var err error
	if t.FreshnessIntervalSeconds, err = positive(raw.FreshnessIntervalSeconds, "tables.freshness_interval_seconds"); err != nil {
		return err
	}
	t.RowsExactIntervalSeconds, err = positive(raw.RowsExactIntervalSeconds, "tables.rows_exact_interval_seconds")
	return err
}

func (d *Discover) UnmarshalYAML(n *yaml.Node) error {
	var raw struct {
		Schemas          []string `yaml:"schemas"`
		Exclude          []string `yaml:"exclude"`
		FreshnessColumns []string `yaml:"freshness_columns"`
		MaxTables        *int     `yaml:"max_tables"`
	}
	if err := decodeKnown(n, &raw, "tables.discover"); err != nil {
		return err
	}
	d.Schemas, d.Exclude, d.FreshnessColumns = raw.Schemas, raw.Exclude, raw.FreshnessColumns
	var err error
	d.MaxTables, err = positive(raw.MaxTables, "tables.discover.max_tables")
	return err
}

// decodeKnown decodes a mapping node into v and refuses a key v has no
// field for, as the top-level decoder does, since a node's own Decode
// cannot be told KnownFields.
func decodeKnown(n *yaml.Node, v any, path string) error {
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	if err := enc.Encode(n); err != nil {
		return err
	}
	dec := yaml.NewDecoder(&buf)
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// positive is the value a written interval must have; nil is "not written"
// and stays zero for the default.
func positive(v *int, key string) (int, error) {
	if v == nil {
		return 0, nil
	}
	if *v <= 0 {
		return 0, fmt.Errorf("%s: %d; must be positive", key, *v)
	}
	return *v, nil
}

// Kinds v1 knows, and whether it ships them.
var kinds = map[string]bool{
	"postgres":  true,
	"mysql":     false,
	"mongo":     false,
	"snowflake": false,
	"redshift":  false,
}

// defaultPort is the kind's port when the DSN names none.
var defaultPort = map[string]string{
	"postgres": "5432",
}

// DefaultFreshnessColumns is what discovery tries when the file names none.
var DefaultFreshnessColumns = []string{"updated_at", "created_at", "minute", "ts"}

// Load reads path, expands {{ NAME }} from the environment, applies the
// defaults and checks every rule.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	expanded, err := expandEnv(raw)
	if err != nil {
		return nil, err
	}
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(expanded))
	dec.KnownFields(true)
	// An empty file decodes to EOF; that is "no databases", below.
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	f.applyDefaults()
	if err := f.validate(); err != nil {
		return nil, err
	}
	return &f, nil
}

var envRef = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// expandEnv replaces every {{ NAME }} with the environment's value. An
// unset name is an error naming it, not an empty string.
func expandEnv(raw []byte) ([]byte, error) {
	var missing []string
	out := envRef.ReplaceAllFunc(raw, func(m []byte) []byte {
		name := string(envRef.FindSubmatch(m)[1])
		v, ok := os.LookupEnv(name)
		if !ok {
			missing = append(missing, name)
			return m
		}
		return []byte(v)
	})
	if len(missing) == 1 {
		return nil, fmt.Errorf("environment variable %s is not set", missing[0])
	}
	if len(missing) > 1 {
		return nil, fmt.Errorf("environment variables %s are not set", strings.Join(missing, ", "))
	}
	return out, nil
}

func (f *File) applyDefaults() {
	if f.Probe.IntervalSeconds == 0 {
		f.Probe.IntervalSeconds = 60
	}
	if f.Probe.TimeoutSeconds == 0 {
		f.Probe.TimeoutSeconds = 5
	}
	if f.Tables.Rows == "" {
		f.Tables.Rows = "estimate"
	}
	if f.Tables.FreshnessIntervalSeconds == 0 {
		f.Tables.FreshnessIntervalSeconds = 60
	}
	if f.Tables.RowsExactIntervalSeconds == 0 {
		f.Tables.RowsExactIntervalSeconds = 3600
	}
	if f.Tables.Discover == nil && len(f.Tables.Static) == 0 {
		f.Tables.Discover = &Discover{}
	}
	if d := f.Tables.Discover; d != nil {
		if len(d.FreshnessColumns) == 0 {
			d.FreshnessColumns = append([]string(nil), DefaultFreshnessColumns...)
		}
		if d.MaxTables == 0 {
			d.MaxTables = 50
		}
	}
	for i := range f.Databases {
		db := &f.Databases[i]
		if db.Name == "" && db.DSN != "" {
			if t, err := RedactedTarget(db.DSN); err == nil {
				db.Name = t
			}
		}
	}
}

func (f *File) validate() error {
	if len(f.Databases) == 0 {
		return errors.New("databases: at least one database is required")
	}
	seen := map[string]int{}
	for i, db := range f.Databases {
		ships, known := kinds[db.Kind]
		switch {
		case db.Kind == "":
			return fmt.Errorf("databases[%d].kind: is required; v1 is postgres", i)
		case !known:
			return fmt.Errorf("databases[%d].kind: %q is not a kind dbhealth knows; v1 is postgres", i, db.Kind)
		case !ships:
			return fmt.Errorf("databases[%d].kind: %q is not in this version; v1 is postgres", i, db.Kind)
		}
		if db.DSN == "" {
			return fmt.Errorf("databases[%d].dsn: is required", i)
		}
		if _, err := RedactedTarget(db.DSN); err != nil {
			return fmt.Errorf("databases[%d].dsn: %w", i, err)
		}
		if j, dup := seen[db.Name]; dup {
			return fmt.Errorf("databases[%d].name: %q is a duplicate of databases[%d]", i, db.Name, j)
		}
		seen[db.Name] = i
	}
	if f.Probe.IntervalSeconds <= 0 {
		return fmt.Errorf("probe.interval_seconds: %d; must be positive", f.Probe.IntervalSeconds)
	}
	if f.Probe.TimeoutSeconds <= 0 {
		return fmt.Errorf("probe.timeout_seconds: %d; must be positive", f.Probe.TimeoutSeconds)
	}
	t := f.Tables
	if t.Discover != nil && len(t.Static) > 0 {
		return errors.New("tables: discover and static are exclusive; set one")
	}
	if t.Rows != "estimate" && t.Rows != "exact" {
		return fmt.Errorf("tables.rows: %q; must be estimate or exact", t.Rows)
	}
	for i, s := range t.Static {
		if s.Name == "" {
			return fmt.Errorf("tables.static[%d].name: is required", i)
		}
		if s.Rows != "" && s.Rows != "estimate" && s.Rows != "exact" {
			return fmt.Errorf("tables.static[%d].rows: %q; must be estimate or exact", i, s.Rows)
		}
	}
	if t.Discover != nil && t.Discover.MaxTables <= 0 {
		return fmt.Errorf("tables.discover.max_tables: %d; must be positive", t.Discover.MaxTables)
	}
	if t.FreshnessIntervalSeconds <= 0 {
		return fmt.Errorf("tables.freshness_interval_seconds: %d; must be positive", t.FreshnessIntervalSeconds)
	}
	if t.RowsExactIntervalSeconds < f.Probe.IntervalSeconds {
		return fmt.Errorf("tables.rows_exact_interval_seconds: %d; must be at least the probe interval, %d",
			t.RowsExactIntervalSeconds, f.Probe.IntervalSeconds)
	}
	r := f.Report
	if r.To == "" && r.StatsD == "" {
		return errors.New("report: set report.to, report.statsd, or both")
	}
	if r.To != "" && r.Credential == "" {
		return errors.New("report.credential: is required when report.to is set")
	}
	return nil
}

// RedactedTarget is host:port/database from any DSN form, URL or key=value.
// It is what the bundle carries in place of the DSN. The error never
// carries the DSN.
func RedactedTarget(dsn string) (string, error) {
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "", errors.New("not a URL dsn")
		}
		host, port := u.Hostname(), u.Port()
		if port == "" {
			port = defaultPort["postgres"]
		}
		if host == "" {
			return "", errors.New("the dsn names no host")
		}
		return host + ":" + port + strings.TrimSuffix("/"+strings.TrimPrefix(u.Path, "/"), "/"), nil
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		// pgconn's error can quote the DSN; this one does not.
		return "", errors.New("not a key=value dsn")
	}
	if cfg.Host == "" {
		return "", errors.New("the dsn names no host")
	}
	port := strconv.Itoa(int(cfg.Port))
	if cfg.Port == 0 {
		port = defaultPort["postgres"]
	}
	target := cfg.Host + ":" + port
	if cfg.Database != "" {
		target += "/" + cfg.Database
	}
	return target, nil
}
