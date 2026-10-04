package test

import (
	"context"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/thomaskhub/pg-bisync/internal/testpg"
)

type chaosOptions struct {
	minutes int
	hard    bool                     // more node restarts
	proxies map[string]*testpg.Proxy // node → proxy to its peer (WAN simulation)
}

func envMinutes(def int) int {
	if m, _ := strconv.Atoi(os.Getenv("PGBISYNC_CHAOS_MIN")); m > 0 {
		return m
	}
	return def
}

// runChaos: random concurrent writes on both nodes while workers are killed, nodes restarted
// and (WAN) connections cut; at the end both nodes must equal the reference model.
func runChaos(t *testing.T, c *cluster, o chaosOptions) {
	t.Helper()
	peer := map[string]string{}
	for node, p := range o.proxies {
		peer[node] = p.DSN(c.cfg[node].PeerDSN)
	}
	workers := startWorkers(t, c, peer, "")
	m := newModel(c.cfg["in"].TieBreakNode)
	seedModel(t, m, c.in)
	l := &load{m: m, mix: defaultMix, delay: 10 * time.Millisecond}

	ctx, stop := context.WithTimeout(context.Background(), time.Duration(o.minutes)*time.Minute)
	defer stop()
	var wg sync.WaitGroup
	for _, n := range both(c) {
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(n *testpg.Node, seed uint64) { defer wg.Done(); l.run(ctx, n, seed) }(n, uint64(time.Now().UnixNano())+uint64(g))
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewPCG(1, 2))
		restarted := false
		start := time.Now()
		for {
			sleepCtx(ctx, time.Duration(15+r.IntN(20))*time.Second)
			if ctx.Err() != nil {
				return
			}
			node := []string{"in", "eu"}[r.IntN(2)]
			workers[node].kill()
			if p := o.proxies[[]string{"in", "eu"}[r.IntN(2)]]; p != nil && r.IntN(2) == 0 {
				p.Cut(time.Duration(2+r.IntN(10)) * time.Second)
			}
			if (o.hard && r.IntN(10) < 3) || (!restarted && time.Since(start) > time.Duration(o.minutes)*time.Minute/2) {
				restartContainer(both(c)[r.IntN(2)])
				restarted = true
			}
			sleepCtx(ctx, time.Duration(3+r.IntN(8))*time.Second)
			workers[node].start(t)
		}
	}()
	wg.Wait()
	for _, w := range workers {
		w.start(t)
	}
	c.waitSync(t, 5*time.Minute)
	time.Sleep(2 * time.Second)
	ok, failed := l.totals()
	t.Logf("operations: %d ok, %d failed locally (FK races, restarts)", ok, failed)
	for _, p := range checkModel(t, c, m) {
		t.Error(p)
	}
	for _, n := range both(c) {
		t.Logf("%s conflicts: %s", n.Name, n.Text(t, "SELECT coalesce(string_agg(kind || '=' || n, ' '), 'none') FROM (SELECT kind, count(*)::text n FROM bisync.conflict GROUP BY kind) x"))
	}
}

func TestChaos(t *testing.T) {
	c := newCluster(t)
	runChaos(t, c, chaosOptions{minutes: envMinutes(2), hard: os.Getenv("PGBISYNC_CHAOS_HARD") == "1"})
}

// TestChaosWAN: chaos over a simulated India–EU link (≈130 ms each way ± 20 ms, a random drop of
// each connection every ~2 min, cuts of 2–12 s) with Postgres limited to half a CPU.
func TestChaosWAN(t *testing.T) {
	c := newClusterWith(t, testpg.Options{CPUs: "0.5"})
	proxies := map[string]*testpg.Proxy{}
	for node, peer := range map[string]*testpg.Node{"in": c.eu, "eu": c.in} {
		p, err := testpg.NewProxy(peer.HostPort())
		if err != nil {
			t.Fatal(err)
		}
		p.SetLink(130*time.Millisecond, 20*time.Millisecond, 2*time.Minute)
		t.Cleanup(p.Close)
		proxies[node] = p
	}
	runChaos(t, c, chaosOptions{minutes: envMinutes(5), proxies: proxies})
}
