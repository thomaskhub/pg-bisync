package test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/thomkin/pg-bisync/internal/status"
	"github.com/thomkin/pg-bisync/internal/testpg"
)

// soakSettings come from the environment (see soak/README.md).
type soakSettings struct {
	duration, check time.Duration
	rate            float64 // operations per second, both nodes together
	wan             bool
	cpus, dir       string
}

func soakFromEnv(t *testing.T) soakSettings {
	t.Helper()
	d, err := time.ParseDuration(os.Getenv("PGBISYNC_SOAK"))
	if err != nil || d <= 0 {
		t.Skip("set PGBISYNC_SOAK=<duration>, e.g. 48h")
	}
	s := soakSettings{duration: d, check: time.Hour, rate: 20, wan: os.Getenv("PGBISYNC_SOAK_WAN") != "0", cpus: os.Getenv("PGBISYNC_SOAK_CPUS"),
		dir: os.Getenv("PGBISYNC_SOAK_DIR")}
	if v, err := time.ParseDuration(os.Getenv("PGBISYNC_SOAK_CHECK")); err == nil && v > 0 {
		s.check = v
	}
	s.check = min(s.check, max(d/4, time.Minute))
	if v, err := strconv.ParseFloat(os.Getenv("PGBISYNC_SOAK_RATE"), 64); err == nil && v > 0 {
		s.rate = v
	}
	if s.dir == "" {
		s.dir = filepath.Join("soak-results", time.Now().Format("20060102-150405"))
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		t.Fatal(err)
	}
	return s
}

// journal writes one JSON object per line.
type journal struct {
	mu sync.Mutex
	f  *os.File
}

func (j *journal) write(kind string, fields map[string]any) {
	j.mu.Lock()
	defer j.mu.Unlock()
	fields["time"], fields["kind"] = time.Now().UTC().Format(time.RFC3339), kind
	b, _ := json.Marshal(fields)
	_, _ = j.f.Write(append(b, '\n'))
}

// TestSoak runs the sync for hours or days with a realistic, rate-limited load and periodic
// faults; every checkpoint compares both nodes with the reference model.
func TestSoak(t *testing.T) {
	s := soakFromEnv(t)
	c := newClusterWith(t, testpg.Options{CPUs: s.cpus})
	f, err := os.Create(filepath.Join(s.dir, "soak.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	j := &journal{f: f}
	proxies := map[string]*testpg.Proxy{}
	peer := map[string]string{}
	if s.wan {
		for node, p := range map[string]*testpg.Node{"in": c.eu, "eu": c.in} {
			px, err := testpg.NewProxy(p.HostPort())
			if err != nil {
				t.Fatal(err)
			}
			px.SetLink(130*time.Millisecond, 20*time.Millisecond, 0)
			t.Cleanup(px.Close)
			proxies[node], peer[node] = px, px.DSN(c.cfg[node].PeerDSN)
		}
	}
	workers := startWorkers(t, c, peer, s.dir)
	m := newModel(c.cfg["in"].TieBreakNode)
	seedModel(t, m, c.in)
	const goroutines = 4
	l := &load{m: m, mix: balancedMix, delay: time.Duration(float64(goroutines) / s.rate * float64(time.Second))}
	j.write("start", map[string]any{"duration": s.duration.String(), "check": s.check.String(), "rate": s.rate, "wan": s.wan, "cpus": s.cpus})
	t.Logf("soak: %s, checkpoint every %s, %.0f ops/s, WAN %v, CPUs %q, results in %s", s.duration, s.check, s.rate, s.wan, s.cpus, s.dir)

	ctx, stop := context.WithTimeout(context.Background(), s.duration)
	defer stop()
	var wg sync.WaitGroup
	for _, n := range both(c) {
		for g := 0; g < goroutines/2; g++ {
			wg.Add(1)
			go func(n *testpg.Node, seed uint64) { defer wg.Done(); l.run(ctx, n, seed) }(n, uint64(time.Now().UnixNano())+uint64(g))
		}
	}
	wg.Add(1)
	go func() { defer wg.Done(); soakEvents(ctx, s, c, l, workers, proxies, j) }()

	start := time.Now()
	failed := true // until the last checkpoint passed; the report is written in any case
	defer func() { writeSoakReport(t, s, f.Name(), failed || t.Failed()) }()
	for n := 1; ; n++ {
		sleepCtx(ctx, s.check)
		final := ctx.Err() != nil
		if final {
			wg.Wait()
		}
		if !soakCheckpoint(t, n, start, c, l, workers, j) {
			stop()
			wg.Wait()
			return
		}
		if final {
			failed = false
			return
		}
	}
}

// soakEvents injects faults; each event holds the load's read lock, so checkpoints never overlap one.
func soakEvents(ctx context.Context, s soakSettings, c *cluster, l *load, workers map[string]*procWorker, proxies map[string]*testpg.Proxy, j *journal) {
	r := rand.New(rand.NewPCG(7, 11))
	every := func(d time.Duration) time.Duration { return min(d, max(s.duration/8, 30*time.Second)) }
	next := map[string]time.Time{"kill": time.Now().Add(every(10 * time.Minute)), "cut": time.Now().Add(every(30 * time.Minute)), "restart": time.Now().Add(every(3 * time.Hour))}
	for ctx.Err() == nil {
		sleepCtx(ctx, 5*time.Second)
		l.gate.RLock()
		soakEvent(ctx, r, every, next, c, workers, proxies, j)
		l.gate.RUnlock()
	}
}

func soakEvent(ctx context.Context, r *rand.Rand, every func(time.Duration) time.Duration, next map[string]time.Time, c *cluster,
	workers map[string]*procWorker, proxies map[string]*testpg.Proxy, j *journal) {
	now := time.Now()
	if now.After(next["kill"]) {
		node := []string{"in", "eu"}[r.IntN(2)]
		down := time.Duration(5+r.IntN(25)) * time.Second
		workers[node].kill()
		j.write("event", map[string]any{"event": "worker_kill", "node": node, "down_s": down.Seconds()})
		sleepCtx(ctx, down)
		workers[node].start(nil)
		next["kill"] = now.Add(every(10 * time.Minute))
	}
	if now.After(next["cut"]) && len(proxies) > 0 {
		node := []string{"in", "eu"}[r.IntN(2)]
		d := time.Duration(10+r.IntN(50)) * time.Second
		proxies[node].Cut(d)
		j.write("event", map[string]any{"event": "network_cut", "node": node, "seconds": d.Seconds()})
		next["cut"] = now.Add(every(30 * time.Minute))
	}
	if now.After(next["restart"]) {
		n := both(c)[r.IntN(2)]
		restartContainer(n)
		j.write("event", map[string]any{"event": "db_restart", "node": n.Name})
		next["restart"] = now.Add(every(3 * time.Hour))
	}
}

// soakCheckpoint pauses the load, waits for the sync and compares with the model.
func soakCheckpoint(t *testing.T, n int, start time.Time, c *cluster, l *load, workers map[string]*procWorker, j *journal) bool {
	t.Helper()
	l.gate.Lock()
	defer l.gate.Unlock()
	for _, w := range workers {
		w.start(t)
	}
	c.waitReady(t, 5*time.Minute)
	t0 := time.Now()
	c.waitSync(t, 10*time.Minute)
	syncWait := time.Since(t0)
	problems := checkModel(t, c, l.m)
	ok, failed := l.totals()
	rows, deleted := l.m.size()
	fields := map[string]any{"checkpoint": n, "elapsed_h": time.Since(start).Hours(), "ops_ok": ok, "ops_failed": failed,
		"sync_wait_s": syncWait.Seconds(), "model_rows": rows, "model_deleted": deleted, "problems": problems}
	for name, w := range workers {
		fields["rss_kb_"+name] = rssKB(w.pid())
	}
	for _, node := range both(c) {
		r, _ := status.Run(context.Background(), c.cfg[node.Name])
		fields["conflicts_"+node.Name] = r.OpenConflicts
		fields["lag_from_peer_"+node.Name] = r.SlotOnPeerLag
		fields["db_mb_"+node.Name] = node.Text(t, "SELECT (pg_database_size(current_database()) >> 20)::text")
	}
	j.write("checkpoint", fields)
	t.Logf("checkpoint %d: %.1f h, %d ops, sync wait %s, problems %v", n, time.Since(start).Hours(), ok, syncWait.Round(time.Millisecond), problems)
	l.m.compact()
	if len(problems) > 0 {
		for _, p := range problems {
			t.Error(p)
		}
		return false
	}
	return true
}

// problemCount returns the number of problems recorded at a checkpoint (JSON null or a list).
func problemCount(v any) int {
	if l, ok := v.([]any); ok {
		return len(l)
	}
	return 0
}

func rssKB(pid int) int64 {
	if pid == 0 {
		return 0
	}
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmRSS:") {
			v, _ := strconv.ParseInt(strings.Fields(l)[1], 10, 64)
			return v
		}
	}
	return 0
}

// writeSoakReport summarises the journal into report.md next to it.
func writeSoakReport(t *testing.T, s soakSettings, journalPath string, failed bool) {
	t.Helper()
	b, err := os.ReadFile(journalPath)
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	events := map[string]int{}
	var maxRSS int64
	var last map[string]any
	for _, l := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		var e map[string]any
		if json.Unmarshal([]byte(l), &e) != nil {
			continue
		}
		switch e["kind"] {
		case "event":
			events[fmt.Sprint(e["event"])]++
		case "checkpoint":
			last = e
			for _, k := range []string{"rss_kb_in", "rss_kb_eu"} {
				if v, ok := e[k].(float64); ok && int64(v) > maxRSS {
					maxRSS = int64(v)
				}
			}
			lines = append(lines, fmt.Sprintf("| %v | %.1f | %v | %v | %.1f | %v / %v | %v / %v | %v |", e["checkpoint"], e["elapsed_h"], e["ops_ok"], e["ops_failed"],
				e["sync_wait_s"], e["rss_kb_in"], e["rss_kb_eu"], e["db_mb_in"], e["db_mb_eu"], problemCount(e["problems"])))
		}
	}
	result := "PASS"
	if failed {
		result = "FAIL (containers kept with PGBISYNC_KEEP=1, see soak.jsonl and worker-*.log)"
	}
	report := fmt.Sprintf("# pg-bisync soak report\n\nResult: **%s**\n\nSettings: duration %s, checkpoint every %s, %.0f ops/s, WAN %v, CPUs %q\n\nEvents: %v\n\nMax worker RSS: %d kB\n\n| # | hours | ops ok | ops failed locally | sync wait s | RSS kB in / eu | DB MB in / eu | problems |\n|---|---|---|---|---|---|---|---|\n%s\n\nLast checkpoint: %v\n",
		result, s.duration, s.check, s.rate, s.wan, s.cpus, events, maxRSS, strings.Join(lines, "\n"), last)
	if err := os.WriteFile(filepath.Join(s.dir, "report.md"), []byte(report), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("report: %s", filepath.Join(s.dir, "report.md"))
}
