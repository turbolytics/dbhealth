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
	"net"
	"net/url"
	"os"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"gopkg.in/yaml.v3"
)

// File is dbhealth.yml.
type File struct {
	Databases []Database
	Probe     Probe
	Tables    Tables
	Report    Report
}

// Database is one endpoint to watch: one instance in control.
type Database struct {
	Kind string
	// DSN is the connection string. It stays in the process.
	DSN string
	// Name is the instance name control shows. Defaults to the redacted
	// target, host:port/database.
	Name string
	// Cluster groups a primary with its replicas.
	Cluster string
}

// Probe is one round trip per interval, per database.
type Probe struct {
	IntervalSeconds int
	TimeoutSeconds  int
}

// Tables says which tables to watch and how. Exactly one of Discover and
// Static.
type Tables struct {
	Discover *Discover
	Static   []StaticTable
	// Rows is "estimate" or "exact", the default for every table.
	Rows                     string
	FreshnessIntervalSeconds int
	RowsExactIntervalSeconds int
}

// Discover watches every table in the given schemas, up to MaxTables.
type Discover struct {
	Schemas []string
	// Exclude is globs on the table name.
	Exclude []string
	// FreshnessColumns is tried in order; the first that exists is the
	// table's.
	FreshnessColumns []string
	MaxTables        int
}

// StaticTable is one named table.
type StaticTable struct {
	Name            string
	FreshnessColumn string
	// Rows overrides Tables.Rows for this table: "estimate" or "exact".
	Rows string
}

// Report is where the facts go: control, StatsD, or both.
type Report struct {
	To         string
	Credential string
	StatsD     string
}

// The file's shape as written. Intervals are pointers so that a key that is
// absent (take the default) is told from one written as 0 (an error).
type file struct {
	Databases []Database `yaml:"databases"`
	Probe     struct {
		IntervalSeconds *int `yaml:"interval_seconds"`
		TimeoutSeconds  *int `yaml:"timeout_seconds"`
	} `yaml:"probe"`
	Tables struct {
		Discover *struct {
			Schemas          []string `yaml:"schemas"`
			Exclude          []string `yaml:"exclude"`
			FreshnessColumns []string `yaml:"freshness_columns"`
			MaxTables        *int     `yaml:"max_tables"`
		} `yaml:"discover"`
		Static []struct {
			Name            string `yaml:"name"`
			FreshnessColumn string `yaml:"freshness_column"`
			Rows            string `yaml:"rows"`
		} `yaml:"static"`
		Rows                     string `yaml:"rows"`
		FreshnessIntervalSeconds *int   `yaml:"freshness_interval_seconds"`
		RowsExactIntervalSeconds *int   `yaml:"rows_exact_interval_seconds"`
	} `yaml:"tables"`
	Report Report `yaml:"report"`
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
const defaultPort = "5432"

// DefaultFreshnessColumns is what discovery tries when the file names none.
var DefaultFreshnessColumns = []string{"updated_at", "created_at", "minute", "ts"}

// Load reads path, decodes it, expands {{ NAME }} in every string from the
// environment, applies the defaults and checks every rule.
func Load(path string) (*File, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var in file
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	// An empty file decodes to EOF; that is "no databases", below.
	if err := dec.Decode(&in); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: %s", path, scrub(err))
	}
	if err := expandEnv(reflect.ValueOf(&in).Elem()); err != nil {
		return nil, err
	}
	f, err := in.resolve()
	if err != nil {
		return nil, err
	}
	if err := f.validate(); err != nil {
		return nil, err
	}
	return f, nil
}

// quoted matches the `value` yaml.v3 puts in a decode error. A value can
// be a secret written under the wrong key, so it is cut from the message.
var quoted = regexp.MustCompile("`[^`]*` ")

func scrub(err error) string {
	return quoted.ReplaceAllString(err.Error(), "")
}

var envRef = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*)\s*\}\}`)

// expandEnv replaces every {{ NAME }} in every string under v with the
// environment's value. It runs on the decoded file, not the text, so a
// value is never parsed as YAML and a reference in a comment is not one.
// An unset name is an error naming it, not an empty string.
func expandEnv(v reflect.Value) error {
	var missing []string
	var walk func(v reflect.Value)
	walk = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.String:
			s := v.String()
			if !strings.Contains(s, "{{") {
				return
			}
			v.SetString(envRef.ReplaceAllStringFunc(s, func(m string) string {
				name := envRef.FindStringSubmatch(m)[1]
				val, ok := os.LookupEnv(name)
				if !ok {
					missing = append(missing, name)
					return m
				}
				return val
			}))
		case reflect.Pointer:
			if !v.IsNil() {
				walk(v.Elem())
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				walk(v.Field(i))
			}
		case reflect.Slice:
			for i := 0; i < v.Len(); i++ {
				walk(v.Index(i))
			}
		}
	}
	walk(v)
	switch len(missing) {
	case 0:
		return nil
	case 1:
		return fmt.Errorf("environment variable %s is not set", missing[0])
	default:
		return fmt.Errorf("environment variables %s are not set", strings.Join(missing, ", "))
	}
}

// resolve turns the file as written into a File with the defaults applied.
// A written interval must be positive; an absent one takes the default.
func (in *file) resolve() (*File, error) {
	f := &File{Databases: in.Databases, Report: in.Report}
	var err error
	if f.Probe.IntervalSeconds, err = interval(in.Probe.IntervalSeconds, 60, "probe.interval_seconds"); err != nil {
		return nil, err
	}
	if f.Probe.TimeoutSeconds, err = interval(in.Probe.TimeoutSeconds, 5, "probe.timeout_seconds"); err != nil {
		return nil, err
	}
	t := &f.Tables
	t.Rows = in.Tables.Rows
	if t.Rows == "" {
		t.Rows = "estimate"
	}
	if t.FreshnessIntervalSeconds, err = interval(in.Tables.FreshnessIntervalSeconds, 60, "tables.freshness_interval_seconds"); err != nil {
		return nil, err
	}
	// count(*) runs at most hourly, and never more often than the probe.
	if t.RowsExactIntervalSeconds, err = interval(in.Tables.RowsExactIntervalSeconds, max(3600, f.Probe.IntervalSeconds), "tables.rows_exact_interval_seconds"); err != nil {
		return nil, err
	}
	for _, s := range in.Tables.Static {
		t.Static = append(t.Static, StaticTable(s))
	}
	if d := in.Tables.Discover; d != nil || len(t.Static) == 0 {
		t.Discover = &Discover{}
		if d != nil {
			t.Discover.Schemas, t.Discover.Exclude, t.Discover.FreshnessColumns = d.Schemas, d.Exclude, d.FreshnessColumns
			if t.Discover.MaxTables, err = interval(d.MaxTables, 50, "tables.discover.max_tables"); err != nil {
				return nil, err
			}
		} else {
			t.Discover.MaxTables = 50
		}
		if len(t.Discover.FreshnessColumns) == 0 {
			t.Discover.FreshnessColumns = append([]string(nil), DefaultFreshnessColumns...)
		}
	}
	for i := range f.Databases {
		db := &f.Databases[i]
		if db.Name == "" && db.DSN != "" {
			if target, err := RedactedTarget(db.DSN); err == nil {
				db.Name = target
			}
		}
	}
	return f, nil
}

func interval(v *int, def int, key string) (int, error) {
	if v == nil {
		return def, nil
	}
	if *v <= 0 {
		return 0, fmt.Errorf("%s: %d; must be positive", key, *v)
	}
	return *v, nil
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
	t := f.Tables
	if t.Discover != nil && len(t.Static) > 0 {
		return errors.New("tables: discover and static are exclusive; set one")
	}
	if t.Rows != "estimate" && t.Rows != "exact" {
		return fmt.Errorf("tables.rows: %q; must be estimate or exact", t.Rows)
	}
	names := map[string]int{}
	for i, s := range t.Static {
		if s.Name == "" {
			return fmt.Errorf("tables.static[%d].name: is required", i)
		}
		if j, dup := names[s.Name]; dup {
			return fmt.Errorf("tables.static[%d].name: %q is a duplicate of tables.static[%d]", i, s.Name, j)
		}
		names[s.Name] = i
		if s.Rows != "" && s.Rows != "estimate" && s.Rows != "exact" {
			return fmt.Errorf("tables.static[%d].rows: %q; must be estimate or exact", i, s.Rows)
		}
	}
	if t.Discover != nil {
		for i, g := range t.Discover.Exclude {
			if _, err := path.Match(g, ""); err != nil {
				return fmt.Errorf("tables.discover.exclude[%d]: %q is not a glob", i, g)
			}
		}
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

var urlScheme = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*://`)

// RedactedTarget is host:port/database from any DSN form, URL or key=value.
// It is what the bundle carries in place of the DSN. The error never
// carries the DSN, or any part of it.
func RedactedTarget(dsn string) (string, error) {
	if m := urlScheme.FindString(dsn); m != "" {
		return redactURL(dsn, strings.TrimSuffix(m, "://"))
	}
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		// pgconn's error can quote the DSN; this one does not.
		return "", errors.New("not a key=value dsn")
	}
	if cfg.Host == "" {
		return "", errors.New("the dsn names no host")
	}
	port := defaultPort
	if cfg.Port != 0 {
		port = strconv.Itoa(int(cfg.Port))
	}
	target := net.JoinHostPort(cfg.Host, port)
	if cfg.Database != "" {
		target += "/" + cfg.Database
	}
	return target, nil
}

func redactURL(dsn, scheme string) (string, error) {
	if scheme != "postgres" && scheme != "postgresql" {
		return "", fmt.Errorf("a %s:// dsn; postgres:// or postgresql:// is required", scheme)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", errors.New("not a URL dsn")
	}
	// A password that is not URL-escaped moves the "@" out of the
	// authority, and url.Parse then reads part of it as the host or the
	// path. Refuse rather than report a target made of password.
	if u.User == nil && strings.ContainsAny(dsn, "@#") || u.Fragment != "" || u.RawFragment != "" {
		return "", errors.New("the password in the dsn must be URL-escaped (a / # @ or ? in it breaks the URL)")
	}
	host, port := u.Hostname(), u.Port()
	if host == "" {
		return "", errors.New("the dsn names no host")
	}
	if port == "" {
		port = defaultPort
	} else if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return "", errors.New("the port in the dsn is not a port")
	}
	target := net.JoinHostPort(host, port)
	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		target += "/" + db
	}
	return target, nil
}
