package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/turbolytics/dbhealth/internal/collector"
	"github.com/turbolytics/dbhealth/internal/config"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

// A schema change on a real table reaches the bundle as its diff: the
// collector reads the columns through this client, remembers them, and
// the reading after an ALTER TABLE carries what changed, with the types
// as Postgres names them.
func TestCollect_AnAlterTableIsDiffed(t *testing.T) {
	c := open(t, adminDSN)
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, `CREATE TABLE public.probe (id serial PRIMARY KEY, amount integer, legacy_id text)`)
	assert.NoError(t, err)
	t.Cleanup(func() { _, _ = c.pool.Exec(ctx, `DROP TABLE IF EXISTS public.probe`) })

	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	col := collector.New(
		config.Database{Kind: "postgres", DSN: adminDSN, Name: "app", Cluster: "app"},
		config.Tables{Rows: "estimate", FreshnessIntervalSeconds: 60, RowsExactIntervalSeconds: 3600, Static: []config.StaticTable{{Name: "public.probe"}}},
		config.Probe{IntervalSeconds: 60, TimeoutSeconds: 5}, c, func() time.Time { return now })
	d := col.Collect(ctx)
	assert.Equal(t, 1, len(d.Tables))
	first := d.Tables[0].SchemaHash
	assert.Equal(t, 16, len(first))
	assert.Equal(t, 0, len(d.Tables[0].SchemaChanges))

	_, err = c.pool.Exec(ctx, `ALTER TABLE public.probe ALTER COLUMN amount TYPE numeric(12,2), ALTER COLUMN amount SET NOT NULL, DROP COLUMN legacy_id, ADD COLUMN region text`)
	assert.NoError(t, err)
	now = now.Add(time.Minute)
	d = col.Collect(ctx)
	assert.That(t, d.Tables[0].SchemaHash != first)
	assert.DeepEqual(t, []wire.DatabaseSchemaChange{
		{Column: "amount", Change: "retyped", From: "integer", To: "numeric(12,2)"},
		{Column: "amount", Change: "nullability", From: "NULL", To: "NOT NULL"},
		{Column: "legacy_id", Change: "dropped", From: "text"},
		{Column: "region", Change: "added", To: "text"},
	}, d.Tables[0].SchemaChanges)
}
