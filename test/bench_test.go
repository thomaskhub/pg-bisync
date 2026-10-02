package test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thomkin/pg-bisync/internal/schema"
	"github.com/thomkin/pg-bisync/internal/testpg"
)

const benchWriters = 8

// writeRate runs n single-row transactions (insert, then update of the same rows) with 8 writers.
func writeRate(t *testing.T, node *testpg.Node, prefix string, n int) (ins, upd float64) {
	t.Helper()
	run := func(sql string) float64 {
		var wg sync.WaitGroup
		t0 := time.Now()
		for w := 0; w < benchWriters; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := w; i < n; i += benchWriters {
					if _, err := node.Pool.Exec(context.Background(), sql, fmt.Sprintf("%s-%d", prefix, i)); err != nil {
						t.Error(err)
						return
					}
				}
			}(w)
		}
		wg.Wait()
		return float64(n) / time.Since(t0).Seconds()
	}
	ins = run("INSERT INTO project (id, owner_id, name) VALUES ($1, 'o1', 'bench')")
	upd = run("UPDATE project SET name = 'bench2' WHERE id = $1")
	return ins, upd
}

// TestBench measures (R14): write overhead of triggers + publication, apply rate, idle footprint.
func TestBench(t *testing.T) {
	testpg.Require(t)
	if os.Getenv("PGBISYNC_BENCH") != "1" {
		t.Skip("set PGBISYNC_BENCH=1")
	}
	const rows = 20000
	solo := testpg.Start(t, "solo", "../examples/schema.sql")
	solo.Exec(t, seedSQL)
	insPlain, updPlain := writeRate(t, solo, "plain", rows)
	cfg := nodeConfig("solo", "peer", solo.DSN, solo.DSN)
	if _, err := schema.Install(context.Background(), solo.Pool, cfg); err != nil {
		t.Fatal(err)
	}
	insSync, updSync := writeRate(t, solo, "sync", rows)
	t.Logf("write overhead (single-row transactions, %d writers): insert %.0f → %.0f tx/s (%+.1f %%), update %.0f → %.0f tx/s (%+.1f %%)",
		benchWriters, insPlain, insSync, 100*(insSync/insPlain-1), updPlain, updSync, 100*(updSync/updPlain-1))

	c := newCluster(t)
	bin := filepath.Join(t.TempDir(), "pg-bisync")
	if out, err := exec.Command("go", "build", "-ldflags=-s -w", "-o", bin, "../cmd/pg-bisync").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	if fi, err := os.Stat(bin); err == nil {
		t.Logf("binary size: %.1f MB", float64(fi.Size())/1e6)
	}
	var workers []*procWorker
	for _, node := range []string{"in", "eu"} {
		cf := c.cfg[node]
		p := filepath.Join(t.TempDir(), node+".json")
		b, _ := json.Marshal(map[string]any{"node": cf.Node, "peer": cf.Peer, "local_dsn": cf.LocalDSN, "peer_dsn": cf.PeerDSN, "unique_policy": cf.UniquePolicy})
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		w := &procWorker{bin: bin, cfgPath: p}
		w.start(t)
		workers = append(workers, w)
	}
	t.Cleanup(func() {
		for _, w := range workers {
			w.kill()
		}
	})
	c.waitSync(t, 30*time.Second)

	// apply rate: 10,000 single-row transactions on in, time until all are on eu
	t0 := time.Now()
	writeRate(t, c.in, "apply", 5000) // 5,000 inserts + 5,000 updates
	c.waitSync(t, 5*time.Minute)
	t.Logf("apply: 10,000 single-row transactions written and visible on eu after %s (%.0f tx/s end to end)",
		time.Since(t0).Round(time.Millisecond), 10000/time.Since(t0).Seconds())

	// idle footprint of the eu worker
	pid := workers[1].cmd.Process.Pid
	cpu0 := cpuTicks(t, pid)
	time.Sleep(20 * time.Second)
	cpu1 := cpuTicks(t, pid)
	t.Logf("idle worker: RSS %s, CPU %.2f %% over 20 s", rss(t, pid), float64(cpu1-cpu0)/100/20*100)
}

func rss(t *testing.T, pid int) string {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			return strings.TrimSpace(strings.TrimPrefix(l, "VmRSS:"))
		}
	}
	return "?"
}

// cpuTicks returns utime+stime of pid in clock ticks (100 per second on Linux).
func cpuTicks(t *testing.T, pid int) int64 {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal(err)
	}
	f := strings.Fields(string(b)[strings.LastIndex(string(b), ")")+2:])
	u, _ := strconv.ParseInt(f[11], 10, 64)
	s, _ := strconv.ParseInt(f[12], 10, 64)
	return u + s
}
