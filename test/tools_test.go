package test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/compare"
	"github.com/thomkin/pg-bisync/internal/status"
	"github.com/thomkin/pg-bisync/internal/testpg"
)

// divergeSilently changes a row on n without syncing it (simulates a lost change).
func divergeSilently(t *testing.T, n *testpg.Node, sql string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, n.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	for _, s := range []string{
		"SELECT pg_replication_origin_create('test_divergence') WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = 'test_divergence')",
		"SELECT pg_replication_origin_session_setup('test_divergence')",
		sql,
		"SELECT pg_replication_origin_session_reset()",
	} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCompareRepairStatus(t *testing.T) {
	c := newCluster(t)
	c.start()
	c.waitSync(t, 30*time.Second)
	ctx := context.Background()

	t.Run("T1_status_running", func(t *testing.T) {
		r, err := status.Run(ctx, c.cfg["eu"])
		if err != nil {
			t.Fatal(err)
		}
		if r.State != "running" || !r.SlotOnPeerActive || r.SlotOnPeerLag == nil || r.ClockDiffMillis == nil {
			t.Errorf("status = %+v", r)
		}
	})

	t.Run("T2_compare_equal", func(t *testing.T) {
		res, err := compare.Run(ctx, c.cfg["in"])
		if err != nil || len(res) != 0 {
			t.Fatalf("res = %v, err = %v", res, err)
		}
	})

	t.Run("T3_compare_finds_and_repair_fixes", func(t *testing.T) {
		divergeSilently(t, c.eu, "UPDATE project SET name = 'diverged' WHERE id = 'p5-2'")
		divergeSilently(t, c.eu, "DELETE FROM item WHERE id = 'i4-1-1'")
		res, err := compare.Run(ctx, c.cfg["in"])
		if err != nil {
			t.Fatal(err)
		}
		if len(res["project"].Differing) != 1 || res["project"].Differing[0] != "p5-2" || len(res["item"].Differing) != 1 || res["item"].Differing[0] != "i4-1-1" {
			t.Fatalf("compare = %+v", res)
		}
		if _, err := compare.Repair(ctx, c.cfg["in"], "project", "p5-2", "in"); err != nil {
			t.Fatal(err)
		}
		if msg, err := compare.Repair(ctx, c.cfg["in"], "item", "i4-1-1", "eu"); err != nil || msg != "deleted (missing on eu)" {
			t.Fatalf("repair item: %q %v", msg, err)
		}
		c.waitSync(t, 30*time.Second)
		if res, err := compare.Run(ctx, c.cfg["in"]); err != nil || len(res) != 0 {
			t.Fatalf("after repair: %v %v", res, err)
		}
		if got := c.eu.Text(t, "SELECT name FROM project WHERE id = 'p5-2'"); got == "diverged" {
			t.Error("eu still has the diverged name")
		}
	})

	t.Run("T4_status_stalled", func(t *testing.T) {
		c.in.Exec(t, "ALTER TABLE asset ADD COLUMN t4 text")
		c.in.Exec(t, "UPDATE asset SET t4 = 'x' WHERE id = 'a1'")
		deadline := time.Now().Add(30 * time.Second)
		for {
			r, err := status.Run(ctx, c.cfg["eu"])
			if err != nil {
				t.Fatal(err)
			}
			if r.State == "stalled" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("not stalled: %+v", r)
			}
			time.Sleep(200 * time.Millisecond)
		}
		c.eu.Exec(t, "ALTER TABLE asset ADD COLUMN t4 text")
		c.waitSync(t, time.Minute)
		r, _ := status.Run(ctx, c.cfg["eu"])
		if r.State != "running" {
			t.Errorf("state after fix = %s", r.State)
		}
	})
}
