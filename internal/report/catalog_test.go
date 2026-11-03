package report

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

// wireLeaves is the JSON path of every field in t that holds a value. A list
// adds no step of its own, as in a fact's Wire.
func wireLeaves(t reflect.Type, prefix string) []string {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct || t == timeType {
		return []string{prefix}
	}
	var out []string
	for i := 0; i < t.NumField(); i++ {
		path := jsonName(t.Field(i))
		if prefix != "" {
			path = prefix + "." + path
		}
		out = append(out, wireLeaves(t.Field(i).Type, path)...)
	}
	return out
}

// audit is what is wrong between the wire's fields and the declarations.
func audit(leaves []string, facts []fact, skipped map[string]string) []string {
	claimed := map[string]bool{}
	var counted []string
	for _, f := range facts {
		if f.Wire == "" {
			continue
		}
		claimed[f.Wire] = true
		if f.Count {
			counted = append(counted, f.Wire+".")
		}
	}
	for list := range listLabels {
		claimed[list+".name"] = true
	}
	isLeaf := map[string]bool{}
	var problems []string
	for _, leaf := range leaves {
		isLeaf[leaf] = true
		covered := claimed[leaf]
		for _, c := range counted {
			covered = covered || strings.HasPrefix(leaf, c)
		}
		reason, has := skipped[leaf]
		switch {
		case covered && has:
			problems = append(problems, leaf+" has a metric and sits in notExported")
		case !covered && !has:
			problems = append(problems, leaf+" reaches no output: add it to catalog, or to notExported with the reason")
		case has && reason == "":
			problems = append(problems, leaf+" sits in notExported without a reason")
		}
	}
	for path := range skipped {
		if !isLeaf[path] {
			problems = append(problems, path+" sits in notExported and the wire has no such field")
		}
	}
	sort.Strings(problems)
	return problems
}

func TestCatalog_EveryWireFieldHasAMetricOrAReason(t *testing.T) {
	leaves := wireLeaves(reflect.TypeOf(wire.Database{}), "")
	assert.That(t, len(leaves) > 50)
	if problems := audit(leaves, catalog, notExported); len(problems) > 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

func TestAudit_NamesEachKindOfGap(t *testing.T) {
	facts := []fact{{Wire: "a"}, {Wire: "b"}, {Wire: "list", Count: true}}
	for _, tc := range []struct {
		name    string
		leaves  []string
		skipped map[string]string
		want    string
	}{
		{"a field nobody claimed", []string{"a", "b", "new"}, nil, "new reaches no output"},
		{"a field with a metric and a reason", []string{"a", "b"}, map[string]string{"a": "why"}, "a has a metric"},
		{"a reason for no field", []string{"a", "b"}, map[string]string{"gone": "why"}, "gone sits in notExported"},
		{"a skip with no reason", []string{"a", "b", "c"}, map[string]string{"c": ""}, "c sits in notExported without"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := strings.Join(audit(tc.leaves, facts, tc.skipped), "\n")
			assert.That(t, strings.Contains(got, tc.want))
		})
	}
	// A counted list covers what its elements hold.
	assert.Equal(t, 0, len(audit([]string{"a", "b", "list.x"}, facts, nil)))
}

func TestCatalog_NamesAreUnique(t *testing.T) {
	names := map[string]bool{}
	for _, f := range catalog {
		for _, n := range []string{f.Name, "statsd:" + f.StatsD} {
			if n != "statsd:" && names[n] {
				t.Errorf("%s is declared twice", n)
			}
			names[n] = true
		}
	}
}

func TestCompile_RefusesADeclarationThatCannotRead(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    fact
		want string
	}{
		{"a path the wire lacks", fact{Name: "x", Wire: "resources.nope"}, "no field resources.nope"},
		{"a sibling the wire lacks", fact{Name: "x", Wire: "tables.rows", When: "nope"}, "no sibling nope"},
		{"a word with no reading", fact{Name: "x", Wire: "kind"}, "has no number"},
		{"a count of no list", fact{Name: "x", Wire: "probe.ok", Count: true}, "Count reads a list"},
		{"an equals on no word", fact{Name: "x", Wire: "probe.ok", Equals: "up"}, "Equals reads a word"},
		{"a float in an int gauge", fact{Name: "x", Wire: "replication.lag_seconds"}, "truncate"},
		{"a list with no label", fact{Name: "x", Wire: "queries.mean_ms", Float: true}, "has no label"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				got := fmt.Sprint(recover())
				assert.That(t, strings.Contains(got, tc.want))
			}()
			compile([]fact{tc.f})
		})
	}
}

// Postgres names what the source reads. A reader follows it to the view, so
// a view the source stopped querying must not stay in the docs.
func TestCatalog_PostgresSourcesAreQueried(t *testing.T) {
	var src strings.Builder
	files, err := filepath.Glob("../postgres/*.go")
	assert.NoError(t, err)
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(name)
		assert.NoError(t, err)
		src.Write(b)
	}
	word := regexp.MustCompile(`pg_[a-z_]+|max_connections|shared_buffers`)
	for _, f := range catalog {
		for _, w := range word.FindAllString(f.Postgres, -1) {
			if !strings.Contains(src.String(), w) {
				t.Errorf("%s says it comes from %s, and internal/postgres does not read it", f.Name, w)
			}
		}
	}
}

// The README shortens names, so it stays hand-written. A whole name in it
// must still be one an operator can scrape.
func TestCatalog_ReadmeNamesOnlyMetricsThatExist(t *testing.T) {
	b, err := os.ReadFile("../../README.md")
	assert.NoError(t, err)
	known := map[string]bool{}
	for _, f := range catalog {
		known[f.Name] = true
	}
	names := regexp.MustCompile(`dbhealth_[a-z_]+`).FindAllString(string(b), -1)
	assert.That(t, len(names) > 10)
	for _, n := range names {
		// A name that ends in _ is the README's shorthand, or a config prefix.
		if !known[n] && !strings.HasSuffix(n, "_") {
			t.Errorf("README names %s, and the catalog has no such metric", n)
		}
	}
}

func catalogMarkdown() string {
	var b strings.Builder
	b.WriteString(`# Metrics

This file lists every metric dbhealth exports. A test generates it from
` + "`internal/report/catalog.go`" + `, so change the catalog and run
` + "`UPDATE_GOLDEN=1 go test ./internal/report/`" + ` to update it.

Every series carries the label ` + "`db`" + `. A field the database could not give
is absent, not zero. StatsD names take the prefix ` + "`dbhealth.`" + `. The field is
the JSON path in the report to Control.
`)
	group := ""
	for _, f := range catalog {
		if f.Group != group {
			group = f.Group
			fmt.Fprintf(&b, "\n## %s\n\n| Metric | Type | StatsD | Label | Meaning | Field | Postgres |\n|---|---|---|---|---|---|---|\n", group)
		}
		kind := "int"
		if f.Float {
			kind = "float"
		}
		from := f.Postgres
		if from == "" {
			from = "never sent"
		}
		fmt.Fprintf(&b, "| `%s` | %s | %s | %s | %s | %s | %s |\n",
			f.Name, kind, code(f.StatsD), code(f.label), f.Help, code(f.Wire), from)
	}
	b.WriteString("\n## Not exported\n\nControl receives these fields. No output sends them, for these reasons:\n\n| Field | Why |\n|---|---|\n")
	var paths []string
	for p := range notExported {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, p := range paths {
		fmt.Fprintf(&b, "| `%s` | %s |\n", p, notExported[p])
	}
	return b.String()
}

func code(s string) string {
	if s == "" {
		return ""
	}
	return "`" + s + "`"
}

func TestCatalog_DocsMatch(t *testing.T) {
	matchGolden(t, "../../docs/metrics.md", catalogMarkdown())
}
