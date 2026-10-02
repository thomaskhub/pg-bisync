package test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestScenarios runs the PoC scenarios S0–S16 on one pair of nodes, in order.
func TestScenarios(t *testing.T) {
	c := newCluster(t)
	c.start()
	syncWait := 30 * time.Second

	t.Run("S0_initial_sync", func(t *testing.T) {
		c.waitSync(t, syncWait)
		c.requireEqual(t)
	})

	t.Run("S1_concurrent_update_later_wins", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "UPDATE project SET name = 'S1-in' WHERE id = 'p1-1'")
		time.Sleep(20 * time.Millisecond)
		c.eu.Exec(t, "UPDATE project SET name = 'S1-eu' WHERE id = 'p1-1'")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if got := n.Text(t, "SELECT name FROM project WHERE id = 'p1-1'"); got != "S1-eu" {
				t.Errorf("%s: name = %q, want S1-eu", n.Name, got)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S2_retry_with_same_id", func(t *testing.T) {
		c.stopAll()
		for _, n := range both(c) {
			n.Exec(t, "INSERT INTO item (id, project_id, asset_id, pos) VALUES ('i-s2', 'p1-2', 'a1', 9) ON CONFLICT (id) DO NOTHING")
		}
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if got := n.Text(t, "SELECT count(*)::text FROM item WHERE id = 'i-s2'"); got != "1" {
				t.Errorf("%s: rows = %s, want 1", n.Name, got)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S3_soft_delete_and_new_link", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "UPDATE asset SET deleted_at = now() WHERE id = 'a2'")
		c.eu.Exec(t, "INSERT INTO item (id, project_id, asset_id) VALUES ('i-s3', 'p6-1', 'a2')")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if n.Text(t, "SELECT (deleted_at IS NOT NULL)::text FROM asset WHERE id = 'a2'") != "true" || n.Text(t, "SELECT id FROM item WHERE id = 'i-s3'") == "" {
				t.Errorf("%s: soft delete or link missing", n.Name)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S3b_hard_delete_vs_new_link_is_a_recorded_conflict", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "DELETE FROM item WHERE asset_id = 'a9'")
		c.in.Exec(t, "DELETE FROM asset WHERE id = 'a9'")
		c.eu.Exec(t, "INSERT INTO item (id, project_id, asset_id) VALUES ('i-s3b', 'p6-2', 'a9')")
		c.start()
		c.waitSync(t, syncWait)
		if got := c.in.Text(t, "SELECT kind FROM bisync.conflict WHERE pk = 'i-s3b'"); got != "parent_deleted" {
			t.Errorf("in: conflict = %q, want parent_deleted", got)
		}
		if got := c.eu.Text(t, "SELECT kind FROM bisync.conflict WHERE pk = 'a9'"); got != "delete_blocked" {
			t.Errorf("eu: conflict = %q, want delete_blocked", got)
		}
		// resolution (delete wins): remove the link and the asset on eu, then both are equal
		c.eu.Exec(t, "DELETE FROM item WHERE id = 'i-s3b'")
		c.eu.Exec(t, "DELETE FROM asset WHERE id = 'a9'")
		c.waitSync(t, syncWait)
		c.requireEqual(t)
	})

	t.Run("S4_update_then_delete_deleted_on_both", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "UPDATE project SET name = 'S4' WHERE id = 'p2-1'")
		time.Sleep(20 * time.Millisecond)
		c.eu.Exec(t, "DELETE FROM project WHERE id = 'p2-1'")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if n.Text(t, "SELECT count(*)::text FROM project WHERE id = 'p2-1'") != "0" || n.Text(t, "SELECT count(*)::text FROM item WHERE project_id = 'p2-1'") != "0" {
				t.Errorf("%s: project or items still there", n.Name)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S4b_delete_then_later_update_delete_wins", func(t *testing.T) {
		c.stopAll()
		c.eu.Exec(t, "DELETE FROM project WHERE id = 'p2-2'")
		time.Sleep(20 * time.Millisecond)
		c.in.Exec(t, "UPDATE project SET name = 'S4b' WHERE id = 'p2-2'")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if n.Text(t, "SELECT count(*)::text FROM project WHERE id = 'p2-2'") != "0" {
				t.Errorf("%s: deleted project came back", n.Name)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S5_admin_and_login_columns_in_separate_tables", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "UPDATE owner SET enabled = false WHERE id = 'o7'")
		c.eu.Exec(t, "UPDATE owner_login SET otp = 'S5', failed = 1 WHERE owner_id = 'o7'")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if n.Text(t, "SELECT enabled::text FROM owner WHERE id = 'o7'") != "false" || n.Text(t, "SELECT otp FROM owner_login WHERE owner_id = 'o7'") != "S5" {
				t.Errorf("%s: admin or login change lost", n.Name)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S6_sync_down_both_write_then_converge", func(t *testing.T) {
		c.stopAll()
		for i := 0; i < 100; i++ {
			c.in.Exec(t, "INSERT INTO item (id, project_id, pos) VALUES ('s6-in-' || $1::int, 'p3-1', $1::int)", i)
			c.eu.Exec(t, "INSERT INTO item (id, project_id, pos) VALUES ('s6-eu-' || $1::int, 'p8-1', $1::int)", i)
		}
		c.start()
		d := c.waitSync(t, syncWait)
		t.Logf("catch-up of 2×100 inserts: %s", d)
		c.requireEqual(t)
	})

	t.Run("S7_migration_on_both_while_paused", func(t *testing.T) {
		c.stopAll()
		for _, n := range both(c) {
			n.Exec(t, "ALTER TABLE project ADD COLUMN s7 text NOT NULL DEFAULT ''")
		}
		c.start()
		c.in.Exec(t, "UPDATE project SET s7 = 'after' WHERE id = 'p4-1'")
		c.waitSync(t, syncWait)
		if got := c.eu.Text(t, "SELECT s7 FROM project WHERE id = 'p4-1'"); got != "after" {
			t.Errorf("eu: s7 = %q", got)
		}
		c.requireEqual(t)
	})

	t.Run("S7b_migration_only_on_one_node_stalls_without_losing_data", func(t *testing.T) {
		c.in.Exec(t, "ALTER TABLE project ADD COLUMN s7b text NOT NULL DEFAULT ''")
		c.in.Exec(t, "UPDATE project SET s7b = 'new' WHERE id = 'p4-2'")
		deadline := time.Now().Add(syncWait)
		for c.eu.Text(t, "SELECT state FROM bisync.status WHERE node = 'eu'") != "stalled" {
			if time.Now().After(deadline) {
				t.Fatal("eu did not stall on the missing column")
			}
			time.Sleep(100 * time.Millisecond)
		}
		if msg := c.eu.Text(t, "SELECT message FROM bisync.status WHERE node = 'eu'"); !strings.Contains(msg, "schema mismatch") {
			t.Errorf("eu status message = %q", msg)
		}
		c.eu.Exec(t, "ALTER TABLE project ADD COLUMN s7b text NOT NULL DEFAULT ''")
		c.waitSync(t, 2*syncWait)
		if got := c.eu.Text(t, "SELECT s7b FROM project WHERE id = 'p4-2'"); got != "new" {
			t.Errorf("eu: s7b = %q, want new (the stalled change must not be skipped)", got)
		}
		c.requireEqual(t)
	})

	t.Run("S9_node_down_and_back", func(t *testing.T) {
		c.eu.Stop(t)
		c.in.Exec(t, "UPDATE project SET name = 'S9' WHERE id = 'p9-1'")
		c.eu.StartAgain(t)
		d := c.waitSync(t, 2*syncWait)
		t.Logf("catch-up after restart: %s", d)
		if got := c.eu.Text(t, "SELECT name FROM project WHERE id = 'p9-1'"); got != "S9" {
			t.Errorf("eu: name = %q", got)
		}
		c.requireEqual(t)
	})

	t.Run("S11_late_commit", func(t *testing.T) {
		ctx := context.Background()
		tx, err := c.in.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, "UPDATE project SET name = 'S11-late' WHERE id = 'p7-2'"); err != nil {
			t.Fatal(err)
		}
		c.in.Exec(t, "UPDATE project SET name = 'S11-early' WHERE id = 'p8-2'")
		c.waitSync(t, syncWait)
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		c.waitSync(t, syncWait)
		if c.eu.Text(t, "SELECT name FROM project WHERE id = 'p7-2'") != "S11-late" || c.eu.Text(t, "SELECT name FROM project WHERE id = 'p8-2'") != "S11-early" {
			t.Error("eu: late or early commit missing")
		}
	})

	t.Run("S12_identical_timestamp_tie_break_node_wins", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "INSERT INTO project (id, owner_id, name, updated_at) VALUES ('p-s12', 'o1', 'S12-in', '2030-01-01')")
		c.eu.Exec(t, "INSERT INTO project (id, owner_id, name, updated_at) VALUES ('p-s12', 'o1', 'S12-eu', '2030-01-01')")
		c.start()
		c.waitSync(t, syncWait)
		want := "S12-" + c.cfg["in"].TieBreakNode
		for _, n := range both(c) {
			if got := n.Text(t, "SELECT name FROM project WHERE id = 'p-s12'"); got != want {
				t.Errorf("%s: name = %q, want %q", n.Name, got, want)
			}
		}
	})

	t.Run("S13_quiet_when_idle", func(t *testing.T) {
		c.waitSync(t, syncWait)
		time.Sleep(2 * time.Second)
		before := c.in.Text(t, "SELECT applied_tx::text FROM bisync.status") + "/" + c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status")
		time.Sleep(3 * time.Second)
		after := c.in.Text(t, "SELECT applied_tx::text FROM bisync.status") + "/" + c.eu.Text(t, "SELECT applied_tx::text FROM bisync.status")
		if before != after {
			t.Errorf("applied transactions changed while idle: %s → %s", before, after)
		}
	})

	t.Run("S14_no_echo", func(t *testing.T) {
		c.waitSync(t, syncWait)
		inBefore := c.in.Text(t, "SELECT applied_tx::text FROM bisync.status")
		c.in.Exec(t, "UPDATE project SET name = 'S14' WHERE id = 'p3-2'")
		c.waitSync(t, syncWait) // one marker transaction from eu reaches in
		inAfter := c.in.Text(t, "SELECT applied_tx::text FROM bisync.status")
		if got := c.eu.Text(t, "SELECT name FROM project WHERE id = 'p3-2'"); got != "S14" {
			t.Errorf("eu: name = %q", got)
		}
		if diff := atoi(t, inAfter) - atoi(t, inBefore); diff != 1 {
			t.Errorf("in applied %d transactions, want 1 (the marker only, no echo of its own change)", diff)
		}
	})

	t.Run("S15_unique_merge_later_row_wins", func(t *testing.T) {
		c.stopAll()
		c.in.Exec(t, "INSERT INTO asset (id, name, hash) VALUES ('a-s15-in', 'in', 'h-s15')")
		c.in.Exec(t, "INSERT INTO item (id, project_id, asset_id) VALUES ('i-s15-in', 'p5-1', 'a-s15-in')")
		time.Sleep(20 * time.Millisecond)
		c.eu.Exec(t, "INSERT INTO asset (id, name, hash) VALUES ('a-s15-eu', 'eu', 'h-s15')")
		c.start()
		c.waitSync(t, syncWait)
		for _, n := range both(c) {
			if got := n.Text(t, "SELECT id FROM asset WHERE hash = 'h-s15'"); got != "a-s15-eu" {
				t.Errorf("%s: kept %q, want the later a-s15-eu", n.Name, got)
			}
			if got := n.Text(t, "SELECT asset_id FROM item WHERE id = 'i-s15-in'"); got != "a-s15-eu" {
				t.Errorf("%s: child of the losing asset points to %q, want the winner a-s15-eu", n.Name, got)
			}
		}
		c.requireEqual(t)
	})

	t.Run("S16_unchanged_toast_value_of_a_row_missing_locally", func(t *testing.T) {
		// random hex does not compress below the TOAST threshold: stored out of line, so an UPDATE
		// that does not touch it sends it as "unchanged"
		c.in.Exec(t, "UPDATE project SET notes = (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 513)) WHERE id = 'p10-1'")
		c.waitSync(t, syncWait)
		// lose the row on eu without a tombstone and without sending the delete back
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, c.eu.DSN)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		for _, s := range []string{
			"SELECT pg_replication_origin_create('test_divergence') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = 'test_divergence')",
			"SELECT pg_replication_origin_session_setup('test_divergence')",
			"DELETE FROM project WHERE id = 'p10-1'",
			"SELECT pg_replication_origin_session_reset()",
		} {
			if _, err := conn.Exec(ctx, s); err != nil {
				t.Fatal(err)
			}
		}
		c.in.Exec(t, "UPDATE project SET name = 'S16' WHERE id = 'p10-1'") // notes unchanged → not sent
		c.waitSync(t, syncWait)
		want := c.in.Text(t, "SELECT name || ':' || md5(notes) FROM project WHERE id = 'p10-1'")
		if got := c.eu.Text(t, "SELECT name || ':' || md5(notes) FROM project WHERE id = 'p10-1'"); got != want {
			t.Errorf("eu: row = %q, want %q (unchanged value fetched from the peer)", got, want)
		}
		if c.in.Text(t, "SELECT (pg_column_size(notes) < length(notes))::text FROM project WHERE id = 'p10-1'") != "true" &&
			c.in.Text(t, "SELECT (pg_column_size(notes) > 2000)::text FROM project WHERE id = 'p10-1'") != "true" {
			t.Error("test data is not toasted: the unchanged-value path was not exercised")
		}
	})
}

func atoi(t *testing.T, s string) int {
	t.Helper()
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			t.Fatalf("not a number: %q", s)
		}
		n = n*10 + int(r-'0')
	}
	return n
}
