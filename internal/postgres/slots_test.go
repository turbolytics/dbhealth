package postgres

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/turbolytics/sql-flow/turbostats/wire"
	"github.com/zeebo/assert"
)

const standbyPort = "5433"

// admin runs one statement as the superuser, for the test's own setup.
func admin(t *testing.T, q string, args ...any) {
	t.Helper()
	c := open(t, adminDSN)
	_, err := c.pool.Exec(context.Background(), q, args...)
	assert.NoError(t, err)
}

// slot makes a slot that the test drops when it ends. Slots outlive every
// session, so one left behind would change what every later test reads.
func slot(t *testing.T, create string) {
	t.Helper()
	admin(t, create)
	name := strings.Split(create, "'")[1]
	t.Cleanup(func() {
		c, err := Open(context.Background(), adminDSN, 5*time.Second)
		assert.NoError(t, err)
		defer c.Close()
		_, err = c.pool.Exec(context.Background(), `SELECT pg_drop_replication_slot($1)`, name)
		assert.NoError(t, err)
	})
}

// writeWAL writes n bytes of WAL, with no table involved. That is what a
// slot sees on a low-activity database: WAL from elsewhere on the server.
// The message is transactional, so its commit flushes it on every version:
// pg_current_wal_lsn() is the write position, and an unflushed record stays
// in the WAL buffers until the WAL writer's next pass.
func writeWAL(t *testing.T, n int) {
	t.Helper()
	admin(t, `SELECT pg_logical_emit_message(true, 'dbhealth', repeat('x', $1::int))`, n)
}

func replication(t *testing.T, dsn string) *wire.DatabaseReplication {
	t.Helper()
	r, err := open(t, dsn).Replication(context.Background())
	assert.NoError(t, err)
	return r
}

func slotNamed(t *testing.T, r *wire.DatabaseReplication, name string) wire.DatabaseReplicationSlot {
	t.Helper()
	assert.NotNil(t, r)
	for _, s := range r.Slots {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no slot %q in %+v", name, r.Slots)
	return wire.DatabaseReplicationSlot{}
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("never: %s", what)
}

// A slot with no consumer holds WAL from the moment it exists. No replica
// is connected, so before slots this database reported no replication at
// all. It reads as a primary, because only a primary reports slots.
func TestReplication_ASlotWithNoConsumerHoldsWAL(t *testing.T) {
	slot(t, `SELECT pg_create_logical_replication_slot('idle', 'pgoutput')`)
	before := slotNamed(t, replication(t, adminDSN), "idle")

	const written = 1 << 20
	writeWAL(t, written)

	r := replication(t, adminDSN)
	assert.Equal(t, "primary", r.Role)
	assert.Equal(t, 0, len(r.Replicas))
	s := slotNamed(t, r, "idle")
	assert.Equal(t, "logical", s.Type)
	assert.Equal(t, "pgoutput", s.Plugin)
	assert.Equal(t, "app", s.Database)
	assert.That(t, !s.Active)
	assert.Equal(t, "reserved", s.WALStatus)
	assert.That(t, *s.RetainedBytes-*before.RetainedBytes >= written)
	assert.That(t, *s.BehindBytes-*before.BehindBytes >= written)
	// The server sets no limit, so nothing says how much room is left.
	assert.Nil(t, s.SafeWALBytes)
}

// The failure this section exists to show. While a consumer streams it is
// in both lists. Once it stops it leaves pg_stat_replication, which is all
// dbhealth read before, and the slot stays, inactive, holding more WAL.
func TestReplication_AStoppedConsumerLeavesReplicasAndStaysInSlots(t *testing.T) {
	ctx := context.Background()
	slot(t, `SELECT pg_create_logical_replication_slot('cdc', 'test_decoding')`)
	done := make(chan error, 1)
	go func() {
		_, _, err := container.Exec(ctx, []string{"pg_recvlogical", "-U", "admin", "-d", "app",
			"--slot", "cdc", "--start", "--no-loop", "-f", "/dev/null"}, tcexec.WithUser("postgres"))
		done <- err
	}()
	eventually(t, "the consumer connects", func() bool {
		r := replication(t, adminDSN)
		return r != nil && len(r.Replicas) == 1 && slotNamed(t, r, "cdc").Active
	})
	assert.Equal(t, "pg_recvlogical", replication(t, adminDSN).Replicas[0].Name)

	admin(t, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'cdc'`)
	assert.NoError(t, <-done)
	eventually(t, "the consumer leaves", func() bool {
		r := replication(t, adminDSN)
		return len(r.Replicas) == 0 && !slotNamed(t, r, "cdc").Active
	})

	before := slotNamed(t, replication(t, adminDSN), "cdc")
	const written = 1 << 20
	writeWAL(t, written)
	after := slotNamed(t, replication(t, adminDSN), "cdc")
	assert.That(t, !after.Active)
	assert.That(t, *after.RetainedBytes-*before.RetainedBytes >= written)
}

// A slot names the connection that holds it, so Control can tell which
// tool holds the WAL. A role with no grants sees both names. Once the
// consumer stops, the slot names nobody: the names were the connection's.
func TestReplication_ASlotNamesTheConnectionThatHoldsIt(t *testing.T) {
	ctx := context.Background()
	slot(t, `SELECT pg_create_logical_replication_slot('held', 'test_decoding')`)
	// Cleanups run last first, so a failed assertion stops the consumer
	// before the slot is dropped: an active slot can't be.
	t.Cleanup(func() {
		admin(t, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'held' AND active`)
	})
	done := make(chan error, 1)
	go func() {
		_, _, err := container.Exec(ctx, []string{"pg_recvlogical", "-U", "admin",
			"-d", "dbname=app application_name=fivetran",
			"--slot", "held", "--start", "--no-loop", "-f", "/dev/null"}, tcexec.WithUser("postgres"))
		done <- err
	}()
	eventually(t, "the consumer connects", func() bool {
		r := replication(t, readerDSN)
		return r != nil && slotNamed(t, r, "held").Active
	})
	s := slotNamed(t, replication(t, readerDSN), "held")
	assert.Equal(t, "fivetran", s.ConsumerApplicationName)
	assert.Equal(t, "admin", s.ConsumerUser)

	admin(t, `SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE slot_name = 'held'`)
	assert.NoError(t, <-done)
	eventually(t, "the consumer leaves", func() bool {
		return !slotNamed(t, replication(t, readerDSN), "held").Active
	})
	s = slotNamed(t, replication(t, readerDSN), "held")
	assert.Equal(t, "", s.ConsumerApplicationName)
	assert.Equal(t, "", s.ConsumerUser)
}

// A server with more slots than a report carries sends the ones holding
// the most WAL, because those fill the disk first. The name decides nothing.
func TestReplication_TheSlotsHoldingTheMostWALAreSent(t *testing.T) {
	physical := `SELECT pg_create_physical_replication_slot('%s', true)`
	slot(t, fmt.Sprintf(physical, "zz_holds_the_most"))
	// A new physical slot starts at the last checkpoint's redo position, so
	// slots made between checkpoints hold the same WAL and tie on name.
	writeWAL(t, 1<<16)
	admin(t, `CHECKPOINT`)
	for i := 0; i < wire.MaxDatabaseReplicationSlots-1; i++ {
		slot(t, fmt.Sprintf(physical, fmt.Sprintf("middle_%02d", i)))
	}
	writeWAL(t, 1<<16)
	admin(t, `CHECKPOINT`)
	slot(t, fmt.Sprintf(physical, "aa_holds_the_least"))

	r := replication(t, adminDSN)
	assert.Equal(t, wire.MaxDatabaseReplicationSlots, len(r.Slots))
	assert.Equal(t, "zz_holds_the_most", r.Slots[0].Name)
	for _, s := range r.Slots {
		assert.That(t, s.Name != "aa_holds_the_least")
	}
	// A physical slot has no plugin, no database and no confirmed position.
	s := r.Slots[0]
	assert.Equal(t, "physical", s.Type)
	assert.Equal(t, "", s.Plugin)
	assert.Equal(t, "", s.Database)
	assert.Nil(t, s.BehindBytes)
	assert.NotNil(t, s.RetainedBytes)
}

// max_slot_wal_keep_size is what stops a slot from filling the disk, and
// safe_wal_bytes is the room left before the server invalidates the slot.
func TestReplication_ALimitSaysHowMuchRoomIsLeft(t *testing.T) {
	setLimit := func(q string, want string) {
		admin(t, q)
		admin(t, `SELECT pg_reload_conf()`)
		eventually(t, "the limit is "+want, func() bool {
			var got string
			assert.NoError(t, open(t, adminDSN).row(context.Background(), `SHOW max_slot_wal_keep_size`, nil, &got))
			return got == want
		})
	}
	setLimit(`ALTER SYSTEM SET max_slot_wal_keep_size = '1GB'`, "1GB")
	t.Cleanup(func() { setLimit(`ALTER SYSTEM RESET max_slot_wal_keep_size`, "-1") })

	slot(t, `SELECT pg_create_logical_replication_slot('limited', 'pgoutput')`)
	s := slotNamed(t, replication(t, adminDSN), "limited")
	assert.NotNil(t, s.SafeWALBytes)
	assert.That(t, *s.SafeWALBytes > 0)
}

// The grants dbhealth asks for do not change what a slot says: the view
// needs none. A role with no grants still sees a stopped connector.
func TestReplication_ARoleWithNoGrantsSeesSlots(t *testing.T) {
	slot(t, `SELECT pg_create_logical_replication_slot('seen', 'pgoutput')`)
	s := slotNamed(t, replication(t, readerDSN), "seen")
	assert.That(t, !s.Active)
	assert.NotNil(t, s.RetainedBytes)
}

// An operator who turned on wal_on_disk asked for the fact, so a role that
// cannot read it gets an error naming the grant, not silence.
func TestResources_WALOnDiskWithoutTheGrantNamesIt(t *testing.T) {
	r, errs := open(t, readerDSN).WithWALOnDisk(true).Resources(context.Background())
	assert.Nil(t, r.WALBytes)
	var got []string
	for _, e := range errs {
		got = append(got, e.Error)
	}
	assert.That(t, strings.Contains(strings.Join(got, "\n"), "pg_ls_waldir"))
	assert.That(t, strings.Contains(strings.Join(got, "\n"), "pg_monitor"))
}

// A role with no grants loses no slot fact and sees no WAL error by default:
// the facts that catch a stopped connector need nothing extra.
func TestResources_NoGrantsStillSeesSlotsWithNoWALError(t *testing.T) {
	r, errs := open(t, readerDSN).Resources(context.Background())
	assert.Nil(t, r.WALBytes)
	for _, e := range errs {
		assert.False(t, strings.Contains(e.Error, "pg_ls_waldir"))
		assert.False(t, strings.Contains(e.Error, "pg_monitor"))
	}
}

// The error tells a role without the grant to take pg_monitor, so that
// grant has to bring the fact back.
func TestResources_PgMonitorBringsWALBytesBack(t *testing.T) {
	admin(t, `CREATE ROLE monitored LOGIN PASSWORD 'monitored'`)
	t.Cleanup(func() {
		c, err := Open(context.Background(), adminDSN, 5*time.Second)
		assert.NoError(t, err)
		defer c.Close()
		_, err = c.pool.Exec(context.Background(), `DROP ROLE monitored`)
		assert.NoError(t, err)
	})
	admin(t, `GRANT pg_monitor TO monitored`)
	r, errs := open(t, strings.Replace(adminDSN, "admin:admin@", "monitored:monitored@", 1)).WithWALOnDisk(true).Resources(context.Background())
	assert.Equal(t, 0, len(errs))
	assert.NotNil(t, r.WALBytes)
	assert.That(t, *r.WALBytes > 0)
}

// The server's WAL position rises with every write. Two readings of it are
// the collector's write rate.
func TestLoad_ReadsTheWALPosition(t *testing.T) {
	c := open(t, adminDSN)
	_, before, errs := c.Load(context.Background())
	assert.Equal(t, 0, len(errs))
	assert.That(t, before.WALBytes > 0)
	const written = 1 << 20
	writeWAL(t, written)
	_, after, errs := c.Load(context.Background())
	assert.Equal(t, 0, len(errs))
	assert.That(t, after.WALBytes-before.WALBytes >= written)
}

// A standby cannot call pg_current_wal_lsn(). The call shares a query with
// the database's counters, so a mistake here costs a replica every load
// rate, and only a real standby shows it. It reports no slots and no write
// rate, and everything it reported before still reads.
func TestAStandby_ReportsNoSlotsAndKeepsItsLoad(t *testing.T) {
	ctx := context.Background()
	open(t, adminDSN) // skips under -short
	inContainer := func(cmd ...string) {
		t.Helper()
		code, out, err := container.Exec(ctx, cmd, tcexec.WithUser("postgres"))
		assert.NoError(t, err)
		if code != 0 {
			got, _ := io.ReadAll(out)
			t.Fatalf("%v exited %d: %s", cmd, code, got)
		}
	}
	inContainer("pg_basebackup", "-U", "admin", "-D", "/tmp/standby", "-R", "-X", "stream", "-c", "fast")
	// The primary's limits came on its command line, so the backup does not
	// carry them, and a hot standby refuses to run below its primary's.
	inContainer("pg_ctl", "-D", "/tmp/standby", "-w", "-l", "/tmp/standby.log",
		"-o", "-p "+standbyPort+" -c unix_socket_directories=/tmp -c max_replication_slots=40 -c max_wal_senders=40", "start")
	t.Cleanup(func() {
		inContainer("pg_ctl", "-D", "/tmp/standby", "-w", "-m", "immediate", "stop")
		inContainer("rm", "-rf", "/tmp/standby")
		eventually(t, "the primary forgets its standby", func() bool {
			return replication(t, adminDSN) == nil
		})
	})
	port, err := container.MappedPort(ctx, standbyPort+"/tcp")
	assert.NoError(t, err)
	host, err := container.Host(ctx)
	assert.NoError(t, err)
	standby := fmt.Sprintf("postgres://admin:admin@%s:%s/app?sslmode=disable", host, port.Port())

	// A slot on the primary reaches a standby only by failover, and this
	// one has none: what the standby reports is its own.
	slot(t, `SELECT pg_create_logical_replication_slot('on_primary', 'pgoutput')`)

	r := replication(t, standby)
	assert.Equal(t, "replica", r.Role)
	assert.Equal(t, 0, len(r.Slots))

	c := open(t, standby)
	_, counters, errs := c.Load(ctx)
	assert.Equal(t, 0, len(errs))
	assert.Equal(t, int64(-1), counters.WALBytes)
	assert.That(t, !counters.At.IsZero())

	res, errs := c.WithWALOnDisk(true).Resources(ctx)
	assert.Equal(t, 0, len(errs))
	assert.NotNil(t, res.WALBytes)
}
