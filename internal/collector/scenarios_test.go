package collector

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/dbhealth/internal/source"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
	"gopkg.in/yaml.v3"
)

// testdata/scenarios.yml is the collector's memory as stories: what a
// database does tick by tick, and what the bundle must carry after each
// tick. A tick is one collection: the clock moves to its time, the tick's
// events happen to the database, the collector reads, and the expectation
// is checked against what it would send. It is the place to read what a
// schema change, a restart or a full bundle does, and the place to add a
// case.
const scenarioFile = "testdata/scenarios.yml"

type scenario struct {
	Name        string              `yaml:"name"`
	About       string              `yaml:"about"`
	Description string              `yaml:"description"`
	Tables      map[string][]string `yaml:"tables"`
	Ticks       []tick              `yaml:"ticks"`
}

// tick is one collection, and what happens to the database just before it.
type tick struct {
	At span `yaml:"at"`
	// Events, in the order they are applied.
	Alter       *alter    `yaml:"alter"`
	Write       *write    `yaml:"write"`
	Reset       string    `yaml:"reset"`
	Restart     bool      `yaml:"restart"`
	Down        *bool     `yaml:"down"`
	DropTable   string    `yaml:"drop_table"`
	CreateTable *creation `yaml:"create_table"`
	Expect      []expect  `yaml:"expect"`
}

type alter struct {
	Table    string            `yaml:"table"`
	Add      []string          `yaml:"add"`
	AddMany  *addMany          `yaml:"add_many"`
	Drop     []string          `yaml:"drop"`
	Retype   map[string]string `yaml:"retype"`
	NotNull  []string          `yaml:"notnull"`
	Nullable []string          `yaml:"nullable"`
	Rename   map[string]string `yaml:"rename"`
}

type addMany struct {
	Prefix string `yaml:"prefix"`
	Count  int    `yaml:"count"`
	Type   string `yaml:"type"`
}

type write struct {
	Table    string `yaml:"table"`
	Inserted int64  `yaml:"inserted"`
	Updated  int64  `yaml:"updated"`
	Deleted  int64  `yaml:"deleted"`
}

type creation struct {
	Table   string   `yaml:"table"`
	Columns []string `yaml:"columns"`
}

// expect is what one table's row in the bundle must say. Hash is set,
// same, moved or none; Changes is none, some, same, or the changes as
// "column change from → to". A rate is a number, or none.
type expect struct {
	Table    string  `yaml:"table"`
	Hash     string  `yaml:"hash"`
	Changes  changes `yaml:"changes"`
	Inserted *rate   `yaml:"inserted_per_second"`
	Error    string  `yaml:"error"`
	Tables   *int    `yaml:"tables"`
	Errors   *int    `yaml:"errors"`
}

type changes struct {
	word string
	list []wire.DatabaseSchemaChange
}

func (c *changes) UnmarshalYAML(n *yaml.Node) error {
	if n.Kind == yaml.ScalarNode {
		c.word = n.Value
		return nil
	}
	var lines []string
	if err := n.Decode(&lines); err != nil {
		return err
	}
	for _, l := range lines {
		ch, err := parseChange(l)
		if err != nil {
			return err
		}
		c.list = append(c.list, ch)
	}
	return nil
}

// parseChange reads "amount retyped integer → numeric(12,2)", "legacy_id
// dropped text", "region added text", "amount nullability NULL → NOT NULL".
func parseChange(s string) (wire.DatabaseSchemaChange, error) {
	f := strings.SplitN(s, " ", 3)
	if len(f) < 2 {
		return wire.DatabaseSchemaChange{}, fmt.Errorf("change %q: want column, change, types", s)
	}
	ch := wire.DatabaseSchemaChange{Column: f[0], Change: f[1]}
	rest := ""
	if len(f) == 3 {
		rest = f[2]
	}
	switch ch.Change {
	case "added":
		ch.To = rest
	case "dropped":
		ch.From = rest
	case "retyped", "nullability":
		from, to, ok := strings.Cut(rest, " → ")
		if !ok {
			return ch, fmt.Errorf("change %q: want from → to", s)
		}
		ch.From, ch.To = from, to
	default:
		return ch, fmt.Errorf("change %q: %q is no change", s, ch.Change)
	}
	return ch, nil
}

type rate struct {
	none  bool
	value float64
}

func (r *rate) UnmarshalYAML(n *yaml.Node) error {
	if n.Value == "none" {
		r.none = true
		return nil
	}
	return n.Decode(&r.value)
}

type span time.Duration

func (s *span) UnmarshalYAML(n *yaml.Node) error {
	d, err := time.ParseDuration(n.Value)
	if err != nil {
		return err
	}
	*s = span(d)
	return nil
}

// parseColumn reads "amount integer" or "id integer!", ! being NOT NULL.
func parseColumn(s string) source.Column {
	name, typ, _ := strings.Cut(s, " ")
	notNull := strings.HasSuffix(typ, "!")
	return source.Column{Name: name, Type: strings.TrimSuffix(typ, "!"), NotNull: notNull}
}

// db is the scenario's database: tables with columns and write counters,
// which the ticks alter, and which the collector reads as a Source.
type db struct {
	tables map[string]*table
	down   bool
	now    func() time.Time
}

type table struct {
	columns                    []source.Column
	inserted, updated, deleted int64
}

func (d *db) Probe(ctx context.Context, timeout time.Duration) wire.DatabaseProbe {
	if d.down {
		return wire.DatabaseProbe{OK: false, Error: "refused"}
	}
	at := d.now()
	return wire.DatabaseProbe{OK: true, LatencyMs: 2, LastOKAt: &at}
}

func (d *db) Version(ctx context.Context) (string, error) { return "PostgreSQL 18.0", nil }

func (d *db) Resources(ctx context.Context) (*wire.DatabaseResources, []wire.DatabaseError) {
	size := int64(1 << 20)
	return &wire.DatabaseResources{SizeBytes: &size}, nil
}

func (d *db) Discover(ctx context.Context, c config.Discover) ([]config.StaticTable, int, error) {
	return nil, 0, nil
}

func (d *db) Load(ctx context.Context) (source.Sample, source.Counters, []wire.DatabaseError) {
	return source.Sample{}, source.Counters{Queries: -1, BytesScanned: -1}, nil
}

func (d *db) Replication(ctx context.Context) (*wire.DatabaseReplication, error) { return nil, nil }

func (d *db) Queries() int { return 0 }

func (d *db) Table(ctx context.Context, t config.StaticTable, fresh, exact bool) (wire.DatabaseTable, source.TableCounters, error) {
	tb, ok := d.tables[t.Name]
	if !ok {
		return wire.DatabaseTable{}, source.TableCounters{}, fmt.Errorf("%s: no such table", t.Name)
	}
	rows := int64(100)
	out := wire.DatabaseTable{Name: t.Name, Rows: &rows, SizeBytes: 8192, CheckedAt: d.now()}
	counters := source.TableCounters{DeadRows: -1, SeqScans: -1, IdxScans: -1,
		Inserted: tb.inserted, Updated: tb.updated, Deleted: tb.deleted,
		Columns: append([]source.Column(nil), tb.columns...)}
	return out, counters, nil
}

func (d *db) apply(t *testing.T, tk tick) {
	t.Helper()
	if tk.Down != nil {
		d.down = *tk.Down
	}
	if tk.DropTable != "" {
		delete(d.tables, tk.DropTable)
	}
	if c := tk.CreateTable; c != nil {
		tb := &table{}
		for _, col := range c.Columns {
			tb.columns = append(tb.columns, parseColumn(col))
		}
		d.tables[c.Table] = tb
	}
	if a := tk.Alter; a != nil {
		tb, ok := d.tables[a.Table]
		if !ok {
			t.Fatalf("alter %s: no such table", a.Table)
		}
		for _, col := range a.Add {
			tb.columns = append(tb.columns, parseColumn(col))
		}
		if m := a.AddMany; m != nil {
			for i := 0; i < m.Count; i++ {
				tb.columns = append(tb.columns, source.Column{Name: fmt.Sprintf("%s%d", m.Prefix, i), Type: m.Type})
			}
		}
		for _, name := range a.Drop {
			tb.columns = without(tb.columns, name)
		}
		for i := range tb.columns {
			c := &tb.columns[i]
			if typ, ok := a.Retype[c.Name]; ok {
				c.Type = typ
			}
			if to, ok := a.Rename[c.Name]; ok {
				c.Name = to
			}
		}
		for _, name := range a.NotNull {
			set(tb.columns, name, true)
		}
		for _, name := range a.Nullable {
			set(tb.columns, name, false)
		}
	}
	if w := tk.Write; w != nil {
		tb, ok := d.tables[w.Table]
		if !ok {
			t.Fatalf("write %s: no such table", w.Table)
		}
		tb.inserted += w.Inserted
		tb.updated += w.Updated
		tb.deleted += w.Deleted
	}
	if tk.Reset != "" {
		tb, ok := d.tables[tk.Reset]
		if !ok {
			t.Fatalf("reset %s: no such table", tk.Reset)
		}
		tb.inserted, tb.updated, tb.deleted = 0, 0, 0
	}
}

func without(cols []source.Column, name string) []source.Column {
	out := cols[:0:0]
	for _, c := range cols {
		if c.Name != name {
			out = append(out, c)
		}
	}
	return out
}

func set(cols []source.Column, name string, notNull bool) {
	for i := range cols {
		if cols[i].Name == name {
			cols[i].NotNull = notNull
		}
	}
}

func loadScenarios(t *testing.T) []scenario {
	t.Helper()
	raw, err := os.ReadFile(scenarioFile)
	assert.NoError(t, err)
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	// A misspelt key is a scenario that tests less than it says.
	dec.KnownFields(true)
	var scenarios []scenario
	assert.NoError(t, dec.Decode(&scenarios))
	return scenarios
}

// Every scenario holds: after each tick, the bundle carries what the
// scenario expects.
func TestScenarios_Hold(t *testing.T) {
	for _, sc := range loadScenarios(t) {
		t.Run(sc.About+"/"+sc.Name, func(t *testing.T) { run(t, sc) })
	}
}

// last is what a table's row said at the previous tick, for same.
type last struct {
	hash    string
	changes []wire.DatabaseSchemaChange
}

func run(t *testing.T, sc scenario) {
	t.Helper()
	start := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	now := start
	d := &db{tables: map[string]*table{}, now: func() time.Time { return now }}
	names := make([]string, 0, len(sc.Tables))
	for name, cols := range sc.Tables {
		tb := &table{}
		for _, c := range cols {
			tb.columns = append(tb.columns, parseColumn(c))
		}
		d.tables[name] = tb
		names = append(names, name)
	}
	sort.Strings(names)
	tables := config.Tables{Rows: "estimate", FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600}
	for _, n := range names {
		tables.Static = append(tables.Static, config.StaticTable{Name: n})
	}
	newCollector := func() *Collector {
		return New(config.Database{Kind: "postgres", DSN: "postgres://u:p@db:5432/app", Name: "app", Cluster: "app"},
			tables, config.Probe{IntervalSeconds: 60, TimeoutSeconds: 5}, d, func() time.Time { return now })
	}
	c := newCollector()
	seen := map[string]last{}
	for i, tk := range sc.Ticks {
		now = start.Add(time.Duration(tk.At))
		if tk.Restart {
			c = newCollector()
		}
		d.apply(t, tk)
		bundle := c.Collect(context.Background())
		rows := map[string]wire.DatabaseTable{}
		for _, r := range bundle.Tables {
			rows[r.Name] = r
		}
		for _, e := range tk.Expect {
			check(t, sc, i, tk, e, bundle, rows, seen)
		}
		for name, r := range rows {
			seen[name] = last{hash: r.SchemaHash, changes: r.SchemaChanges}
		}
	}
}

func check(t *testing.T, sc scenario, i int, tk tick, e expect, bundle wire.Database, rows map[string]wire.DatabaseTable, seen map[string]last) {
	t.Helper()
	where := fmt.Sprintf("%s, tick %d at %s", sc.Name, i, time.Duration(tk.At))
	if e.Tables != nil && len(bundle.Tables) != *e.Tables {
		t.Errorf("%s: the bundle carries %d tables, want %d", where, len(bundle.Tables), *e.Tables)
	}
	if e.Errors != nil && len(bundle.Collection.Errors) != *e.Errors {
		t.Errorf("%s: the bundle carries %d errors, want %d: %v", where, len(bundle.Collection.Errors), *e.Errors, bundle.Collection.Errors)
	}
	if e.Error != "" {
		found := false
		for _, err := range bundle.Collection.Errors {
			if strings.Contains(err.Error, e.Error) {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: no error says %q: %v", where, e.Error, bundle.Collection.Errors)
		}
	}
	if e.Table == "" {
		return
	}
	row, present := rows[e.Table]
	prev := seen[e.Table]
	switch e.Hash {
	case "":
	case "none":
		if present && row.SchemaHash != "" {
			t.Errorf("%s: %s carries hash %s, want none", where, e.Table, row.SchemaHash)
		}
	case "set":
		if !present || len(row.SchemaHash) != 16 {
			t.Errorf("%s: %s carries hash %q, want one set", where, e.Table, row.SchemaHash)
		}
	case "same":
		if !present || row.SchemaHash == "" || row.SchemaHash != prev.hash {
			t.Errorf("%s: %s carries hash %q, want the last tick's %q", where, e.Table, row.SchemaHash, prev.hash)
		}
	case "moved":
		if !present || row.SchemaHash == "" || row.SchemaHash == prev.hash {
			t.Errorf("%s: %s carries hash %q, want one moved from %q", where, e.Table, row.SchemaHash, prev.hash)
		}
	default:
		t.Fatalf("%s: hash %q is no expectation", where, e.Hash)
	}
	switch e.Changes.word {
	case "":
		if e.Changes.list != nil && !reflect.DeepEqual(row.SchemaChanges, e.Changes.list) {
			t.Errorf("%s: %s carries changes %v, want %v", where, e.Table, row.SchemaChanges, e.Changes.list)
		}
	case "none":
		if len(row.SchemaChanges) != 0 {
			t.Errorf("%s: %s carries changes %v, want none", where, e.Table, row.SchemaChanges)
		}
	case "some":
		if len(row.SchemaChanges) == 0 {
			t.Errorf("%s: %s carries no changes, want some", where, e.Table)
		}
	case "same":
		if len(prev.changes) == 0 || !reflect.DeepEqual(row.SchemaChanges, prev.changes) {
			t.Errorf("%s: %s carries changes %v, want the last tick's %v", where, e.Table, row.SchemaChanges, prev.changes)
		}
	default:
		t.Fatalf("%s: changes %q is no expectation", where, e.Changes.word)
	}
	if r := e.Inserted; r != nil {
		switch {
		case r.none && row.RowsInsertedPerSecond != nil:
			t.Errorf("%s: %s carries %v rows inserted a second, want none", where, e.Table, *row.RowsInsertedPerSecond)
		case !r.none && (row.RowsInsertedPerSecond == nil || *row.RowsInsertedPerSecond != r.value):
			t.Errorf("%s: %s carries %v rows inserted a second, want %v", where, e.Table, row.RowsInsertedPerSecond, r.value)
		}
	}
}

// Every scenario has a name, a description and a known facet; no two
// share a name; and every facet has at least one story.
func TestScenarios_AreWellFormed(t *testing.T) {
	facets := map[string]int{"shape": 0, "time": 0, "source": 0, "cap": 0, "writes": 0}
	names := map[string]bool{}
	for _, sc := range loadScenarios(t) {
		if sc.Name == "" || sc.Description == "" {
			t.Errorf("a scenario (%q) has no name or no description", sc.Name)
		}
		if names[sc.Name] {
			t.Errorf("two scenarios are named %q", sc.Name)
		}
		names[sc.Name] = true
		if _, ok := facets[sc.About]; !ok {
			t.Errorf("%q is about %q, which is no facet", sc.Name, sc.About)
		}
		facets[sc.About]++
		if len(sc.Ticks) == 0 {
			t.Errorf("%q has no ticks", sc.Name)
		}
		checks := 0
		for _, tk := range sc.Ticks {
			checks += len(tk.Expect)
		}
		if checks == 0 {
			t.Errorf("%q expects nothing", sc.Name)
		}
	}
	for facet, n := range facets {
		if n == 0 {
			t.Errorf("no scenario is about %s", facet)
		}
	}
}
