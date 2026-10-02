package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/initcopy"
	"github.com/thomkin/pg-bisync/internal/schema"
	"github.com/thomkin/pg-bisync/internal/testpg"
)

// TestInit: eu starts empty; init copies in (incl. writes during and after the copy), then both
// directions run.
func TestInit(t *testing.T) {
	testpg.Require(t)
	c := &cluster{cancel: map[string]context.CancelFunc{}, done: map[string]chan struct{}{}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.in = testpg.Start(t, "in", "../examples/schema.sql") }()
	go func() { defer wg.Done(); c.eu = testpg.Start(t, "eu", "../examples/schema.sql") }()
	wg.Wait()
	c.cfg = map[string]config.Config{"in": nodeConfig("in", "eu", c.in.DSN, c.eu.DSN), "eu": nodeConfig("eu", "in", c.eu.DSN, c.in.DSN)}
	t.Cleanup(c.stopAll)
	c.in.Exec(t, seedSQL)
	c.in.Exec(t, `INSERT INTO project (id, owner_id, name) SELECT 'big-' || n, 'o1', 'bulk' FROM generate_series(1, 200000) n`)
	for _, n := range both(c) {
		if _, err := schema.Install(context.Background(), n.Pool, c.cfg[n.Name]); err != nil {
			t.Fatal(err)
		}
	}
	c.in.Exec(t, "DELETE FROM project WHERE id = 'p9-3'") // tombstone on in must reach eu with the copy
	if _, err := initcopy.Run(context.Background(), c.cfg["in"], logger()); err == nil {
		t.Fatal("init on a non-empty node must fail")
	}

	// writes on in while eu copies
	stop := make(chan struct{})
	var writes sync.WaitGroup
	writes.Add(1)
	go func() {
		defer writes.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c.in.Exec(t, "UPDATE project SET name = 'during-' || $1::int WHERE id = 'p1-1'", i)
			time.Sleep(5 * time.Millisecond)
		}
	}()
	rep, err := initcopy.Run(context.Background(), c.cfg["eu"], logger())
	close(stop)
	writes.Wait()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("init: %+v", rep)
	if rep.Rows[`"public"."project"`] < 200000 {
		t.Errorf("copied projects = %d", rep.Rows[`"public"."project"`])
	}
	c.in.Exec(t, "UPDATE project SET name = 'after-init' WHERE id = 'p2-1'")
	c.start()
	c.waitSync(t, time.Minute)
	c.requireEqual(t)
	if got := c.eu.Text(t, "SELECT count(*)::text FROM bisync.tombstone WHERE pk = 'p9-3'"); got != "1" {
		t.Errorf("eu: tombstone of p9-3 not copied")
	}
	// eu → in works as well
	c.eu.Exec(t, "UPDATE project SET name = 'from-eu' WHERE id = 'p3-1'")
	c.waitSync(t, time.Minute)
	if got := c.in.Text(t, "SELECT name FROM project WHERE id = 'p3-1'"); got != "from-eu" {
		t.Errorf("in: name = %q", got)
	}
	c.requireEqual(t)
}
