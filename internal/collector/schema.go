package collector

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/turbolytics/dbhealth/internal/source"
	"github.com/turbolytics/sql-flow/turbostats/wire"
)

// schemaHash is a table's schema in sixteen hex characters: each column's
// name, type and nullability in the system's order, so a receiver that
// sees it move knows the table changed shape. Empty when nothing was read.
func schemaHash(cols []source.Column) string {
	if len(cols) == 0 {
		return ""
	}
	h := sha256.New()
	for _, c := range cols {
		h.Write([]byte(c.Name))
		h.Write([]byte{0})
		h.Write([]byte(c.Type))
		h.Write([]byte{0})
		if c.NotNull {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// diffColumns is what changed between two readings of a schema: the old
// columns in their order, each dropped, retyped or changed in nullability,
// then the new columns added. A column both retyped and made NOT NULL is
// two changes.
func diffColumns(prev, now []source.Column) []wire.DatabaseSchemaChange {
	byName := make(map[string]source.Column, len(now))
	for _, c := range now {
		byName[c.Name] = c
	}
	var out []wire.DatabaseSchemaChange
	seen := make(map[string]bool, len(prev))
	for _, p := range prev {
		seen[p.Name] = true
		n, ok := byName[p.Name]
		if !ok {
			out = append(out, wire.DatabaseSchemaChange{Column: p.Name, Change: "dropped", From: p.Type})
			continue
		}
		if n.Type != p.Type {
			out = append(out, wire.DatabaseSchemaChange{Column: p.Name, Change: "retyped", From: p.Type, To: n.Type})
		}
		if n.NotNull != p.NotNull {
			out = append(out, wire.DatabaseSchemaChange{Column: p.Name, Change: "nullability", From: nullability(p.NotNull), To: nullability(n.NotNull)})
		}
	}
	for _, n := range now {
		if !seen[n.Name] {
			out = append(out, wire.DatabaseSchemaChange{Column: n.Name, Change: "added", To: n.Type})
		}
	}
	return out
}

func nullability(notNull bool) string {
	if notNull {
		return "NOT NULL"
	}
	return "NULL"
}
