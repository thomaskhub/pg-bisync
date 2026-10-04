package test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thomaskhub/pg-bisync/internal/apply"
	"github.com/thomaskhub/pg-bisync/internal/initcopy"
	"github.com/thomaskhub/pg-bisync/internal/status"
	"github.com/thomaskhub/pg-bisync/internal/worker"
)

// TestFaultPartition: eu frozen for 20 s while both keep writing; converges after the partition.
func TestFaultPartition(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	c.eu.Pause(t)
	for i := 0; i < 50; i++ {
		c.in.Exec(t, "UPDATE project SET name = 'part-' || $1::int WHERE id = 'p1-1'", i)
		time.Sleep(200 * time.Millisecond)
	}
	c.eu.Unpause(t)
	c.eu.Exec(t, "UPDATE project SET name = 'after-partition' WHERE id = 'p6-1'")
	c.waitSync(t, 2*time.Minute)
	c.requireEqual(t)
}

// TestFaultLargeTransaction: 50,000 rows in one transaction arrive atomically (never partly visible).
func TestFaultLargeTransaction(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	done := make(chan struct{})
	var partial []string
	var mu sync.Mutex
	go func() {
		defer close(done)
		deadline := time.Now().Add(2 * time.Minute)
		for time.Now().Before(deadline) {
			n := c.eu.Text(t, "SELECT count(*)::text FROM item WHERE id LIKE 'bulk-%'")
			if n == "50000" {
				return
			}
			if n != "0" {
				mu.Lock()
				partial = append(partial, n)
				mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	t0 := time.Now()
	c.in.Exec(t, "INSERT INTO item (id, project_id, pos) SELECT 'bulk-' || n, 'p1-1', n FROM generate_series(1, 50000) n")
	<-done
	t.Logf("50,000-row transaction visible on eu after %s", time.Since(t0))
	if len(partial) > 0 {
		t.Errorf("partly visible counts on eu: %v", partial[:min(5, len(partial))])
	}
	c.requireEqual(t)
}

// TestFaultTwoWorkers: a second worker for the same node waits on the lock and takes over.
func TestFaultTwoWorkers(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan struct{})
	go func() { defer close(second); _ = worker.Run(ctx, c.cfg["eu"], logger()) }()
	defer func() { cancel(); <-second }()
	time.Sleep(3 * time.Second)
	before := atoi(t, c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status"))
	for i := 0; i < 20; i++ {
		c.in.Exec(t, "UPDATE project SET name = 'two-' || $1::int WHERE id = 'p2-1'", i)
	}
	c.waitSync(t, 30*time.Second)
	if got := atoi(t, c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status")) - before; got != 21 {
		t.Errorf("eu applied %d transactions, want 21 (20 updates + 1 marker, nothing twice)", got)
	}
	// first worker gone: the waiting one takes over
	c.mu.Lock()
	c.cancel["eu"]()
	<-c.done["eu"]
	delete(c.cancel, "eu")
	c.mu.Unlock()
	c.in.Exec(t, "UPDATE project SET name = 'taken-over' WHERE id = 'p2-1'")
	c.waitSync(t, time.Minute)
	if got := c.eu.Text(t, "SELECT name FROM project WHERE id = 'p2-1'"); got != "taken-over" {
		t.Errorf("eu: %q", got)
	}
}

// TestFaultSenderTimeout: applying blocks on a local lock longer than the peer's
// wal_sender_timeout; the worker reconnects and the transaction is applied exactly once.
func TestFaultSenderTimeout(t *testing.T) {
	c := newCluster(t, map[string][]string{"in": {"-c", "wal_sender_timeout=3s"}})
	c.start()
	c.waitSync(t, 30*time.Second)
	ctx := context.Background()
	lock, err := c.eu.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Exec(ctx, "SELECT 1 FROM project WHERE id = 'p3-3' FOR UPDATE"); err != nil {
		t.Fatal(err)
	}
	c.in.Exec(t, "UPDATE project SET name = 'locked' WHERE id = 'p3-3'")
	time.Sleep(10 * time.Second)
	_ = lock.Rollback(ctx)
	c.waitSync(t, time.Minute)
	if got := c.eu.Text(t, "SELECT name FROM project WHERE id = 'p3-3'"); got != "locked" {
		t.Errorf("eu: %q", got)
	}
	c.requireEqual(t)
}

// TestFaultSlotInvalidated: in's slot on eu loses its WAL (max_slot_wal_keep_size) while in is
// down; status shows it, rebuild = empty the synced tables on in + init.
func TestFaultSlotInvalidated(t *testing.T) {
	c := newCluster(t, map[string][]string{"eu": {"-c", "max_slot_wal_keep_size=1MB", "-c", "max_wal_size=32MB", "-c", "min_wal_size=32MB"}})
	c.start()
	c.waitSync(t, 30*time.Second)
	c.mu.Lock()
	c.cancel["in"]()
	<-c.done["in"]
	delete(c.cancel, "in")
	c.mu.Unlock()
	for i := 0; i < 6; i++ {
		c.eu.Exec(t, "INSERT INTO item (id, project_id, pos) SELECT 'wal-' || $1::int || '-' || n, 'p6-1', n FROM generate_series(1, 20000) n", i)
		c.eu.Exec(t, "SELECT pg_switch_wal()")
		c.eu.Exec(t, "CHECKPOINT")
	}
	if ws := c.eu.Text(t, "SELECT wal_status FROM pg_replication_slots WHERE slot_name = 'bisync_in'"); ws != "lost" {
		t.Fatalf("slot wal_status = %q, want lost", ws)
	}
	r, err := status.Run(context.Background(), c.cfg["in"])
	if err != nil || r.SlotOnPeerWAL != "lost" {
		t.Fatalf("status does not show the lost slot: %+v %v", r, err)
	}
	// rebuild in
	c.in.Exec(t, "TRUNCATE owner, owner_login, asset, project, item, access, bisync.tombstone CASCADE")
	if _, err := initcopy.Run(context.Background(), c.cfg["in"], logger()); err != nil {
		t.Fatal(err)
	}
	c.start()
	c.waitSync(t, 2*time.Minute)
	c.requireEqual(t)
}

// TestFaultForeignKeyToUnknownParentStalls: a child whose parent never existed locally is not
// skipped (only deleted parents are): the sync stalls with a clear message.
func TestFaultForeignKeyToUnknownParentStalls(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	divergeSilently(t, c.eu, "DELETE FROM project WHERE id = 'p4-3'") // eu lost p4-3 without tombstone
	c.in.Exec(t, "INSERT INTO item (id, project_id) VALUES ('orphan', 'p4-3')")
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(c.eu.Text(t, "SELECT message FROM bisync.status WHERE node = 'eu'"), "does not exist") {
		if time.Now().After(deadline) {
			t.Fatalf("eu did not stall: %s", c.eu.Text(t, "SELECT state || ' ' || message FROM bisync.status"))
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !strings.Contains(c.eu.Text(t, "SELECT message FROM bisync.status WHERE node = 'eu'"), apply.ErrStalled.Error()) {
		t.Error("stall message missing")
	}
}

// TestFaultDeleteDuringApply: an app delete commits while the worker's upsert of the same row waits
// for its lock. Without a second tombstone check the upsert would re-insert the deleted row on this
// node only (found by the 30-minute chaos run).
func TestFaultDeleteDuringApply(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	ctx := context.Background()
	tx, err := c.in.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "DELETE FROM item WHERE id = 'i5-2-2'"); err != nil {
		t.Fatal(err)
	}
	c.eu.Exec(t, "UPDATE item SET pos = 99 WHERE id = 'i5-2-2'") // applied on in while the delete holds the row
	time.Sleep(2 * time.Second)
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	c.waitSync(t, 30*time.Second)
	for _, n := range both(c) {
		if got := n.Text(t, "SELECT count(*)::text FROM item WHERE id = 'i5-2-2'"); got != "0" {
			t.Errorf("%s: deleted item exists again", n.Name)
		}
	}
	c.requireEqual(t)
}

// TestRestartExactlyOnce: a worker stopped right after applying (before its next status update to
// the peer) resumes from the replication origin: no transaction is applied twice.
func TestRestartExactlyOnce(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	for round := 0; round < 5; round++ {
		before := atoi(t, c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status"))
		for i := 0; i < 20; i++ {
			c.in.Exec(t, "UPDATE project SET name = 'once-' || $1::int WHERE id = 'p3-3'", i)
		}
		deadline := time.Now().Add(30 * time.Second)
		for c.eu.Text(t, "SELECT name FROM project WHERE id = 'p3-3'") != "once-19" {
			if time.Now().After(deadline) {
				t.Fatal("updates did not arrive")
			}
			time.Sleep(10 * time.Millisecond)
		}
		c.stopAll() // immediately: the peer has not seen the last confirmations yet
		c.start()
		c.waitSync(t, 30*time.Second) // +1 marker transaction from in
		if got := atoi(t, c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status")) - before; got != 21 {
			t.Fatalf("round %d: eu applied %d transactions, want 21 (20 updates + 1 marker, none twice)", round, got)
		}
	}
}
