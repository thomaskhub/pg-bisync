package test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/schema"
	"github.com/thomkin/pg-bisync/internal/stream"
	"github.com/thomkin/pg-bisync/internal/testpg"
	"github.com/thomkin/pg-bisync/internal/worker"
)

var tables = []string{"owner", "owner_login", "asset", "project", "item", "access"}

// seedSQL is loaded on both nodes before the sync starts (identical rows and timestamps).
const seedSQL = `
INSERT INTO owner (id, name, email, updated_at)
  SELECT 'o' || n, 'Owner ' || n, 'o' || n || '@example.invalid', '2026-01-01' FROM generate_series(1, 10) n;
INSERT INTO owner_login (owner_id, updated_at) SELECT 'o' || n, '2026-01-01' FROM generate_series(1, 10) n;
INSERT INTO asset (id, name, hash, updated_at) SELECT 'a' || n, 'asset ' || n, 'h' || n, '2026-01-01' FROM generate_series(1, 10) n;
INSERT INTO project (id, owner_id, name, updated_at)
  SELECT 'p' || n || '-' || k, 'o' || n, 'project ' || k, '2026-01-01' FROM generate_series(1, 10) n, generate_series(1, 3) k;
INSERT INTO item (id, project_id, asset_id, pos, updated_at)
  SELECT 'i' || n || '-' || k || '-' || i, 'p' || n || '-' || k, 'a' || i, i, '2026-01-01'
  FROM generate_series(1, 10) n, generate_series(1, 3) k, generate_series(1, 3) i;
INSERT INTO access (id, owner_id, project_id, updated_at)
  SELECT 'x' || n, 'o' || n, 'p' || ((n % 10) + 1) || '-1', '2026-01-01' FROM generate_series(1, 10) n;
`

type cluster struct {
	in, eu   *testpg.Node
	cfg      map[string]config.Config
	mu       sync.Mutex
	cancel   map[string]context.CancelFunc
	done     map[string]chan struct{}
	markerNo int
}

func newCluster(t *testing.T, pgArgs ...map[string][]string) *cluster {
	t.Helper()
	testpg.Require(t)
	extra := map[string][]string{}
	if len(pgArgs) > 0 {
		extra = pgArgs[0]
	}
	c := &cluster{cancel: map[string]context.CancelFunc{}, done: map[string]chan struct{}{}}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.in = testpg.Start(t, "in", "../examples/schema.sql", extra["in"]...) }()
	go func() { defer wg.Done(); c.eu = testpg.Start(t, "eu", "../examples/schema.sql", extra["eu"]...) }()
	wg.Wait()
	c.cfg = map[string]config.Config{"in": nodeConfig("in", "eu", c.in.DSN, c.eu.DSN), "eu": nodeConfig("eu", "in", c.eu.DSN, c.in.DSN)}
	for _, n := range []*testpg.Node{c.in, c.eu} {
		n.Exec(t, seedSQL)
		if _, err := schema.Install(context.Background(), n.Pool, c.cfg[n.Name]); err != nil {
			t.Fatal(err)
		}
	}
	// slots exist before anything is written: changes from before a slot are never streamed
	for _, node := range []string{"in", "eu"} {
		cfg := c.cfg[node]
		r := stream.Reader{PeerDSN: cfg.PeerDSN, Slot: cfg.SlotName(), Publication: cfg.Publication}
		if err := r.EnsureSlot(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(c.stopAll)
	return c
}

func nodeConfig(node, peer, local, remote string) config.Config {
	c := config.Config{Node: node, Peer: peer, LocalDSN: local, PeerDSN: remote,
		UniquePolicy: map[string]string{"asset": config.UniqueMerge}, StatusInterval: time.Second, RetryMax: 2 * time.Second}
	c.SetDefaults()
	return c
}

func logger() *slog.Logger {
	if os.Getenv("PGBISYNC_LOG") == "1" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// start runs the workers of both nodes in this process.
func (c *cluster) start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range []string{"in", "eu"} {
		if c.cancel[node] != nil {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		c.cancel[node], c.done[node] = cancel, done
		go func(cfg config.Config) {
			defer close(done)
			_ = worker.Run(ctx, cfg, logger().With("node", cfg.Node))
		}(c.cfg[node])
	}
}

func (c *cluster) stopAll() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for node, cancel := range c.cancel {
		cancel()
		<-c.done[node]
		delete(c.cancel, node)
	}
}

// waitSync writes a marker on each node and waits until both are visible on the other node.
func (c *cluster) waitSync(t *testing.T, timeout time.Duration) time.Duration {
	t.Helper()
	c.markerNo++
	t0 := time.Now()
	m := map[string]string{"in": fmt.Sprintf("m-in-%d-%d", c.markerNo, t0.UnixNano()), "eu": fmt.Sprintf("m-eu-%d-%d", c.markerNo, t0.UnixNano())}
	for _, n := range both(c) {
		// retried: a node may just be restarting
		for i := 0; ; i++ {
			_, err := n.Pool.Exec(context.Background(), "INSERT INTO owner (id, name, email) VALUES ($1, 'marker', $1)", m[n.Name])
			if err == nil {
				break
			}
			if i == 60 {
				t.Fatalf("%s: marker: %v", n.Name, err)
			}
			time.Sleep(time.Second)
		}
	}
	for time.Since(t0) < timeout {
		if c.eu.Text(t, "SELECT id FROM owner WHERE id = $1", m["in"]) != "" && c.in.Text(t, "SELECT id FROM owner WHERE id = $1", m["eu"]) != "" {
			return time.Since(t0)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no sync within %s (status in: %s / eu: %s)", timeout, c.in.Text(t, "SELECT state || ' ' || message FROM bisync.status"), c.eu.Text(t, "SELECT state || ' ' || message FROM bisync.status"))
	return 0
}

// diff returns the tables whose content differs between the nodes.
func (c *cluster) diff(t *testing.T) []string {
	t.Helper()
	a, b := c.in.Checksum(t, tables...), c.eu.Checksum(t, tables...)
	var out []string
	for _, tbl := range tables {
		if a[tbl] != b[tbl] {
			out = append(out, tbl)
		}
	}
	return out
}

func (c *cluster) requireEqual(t *testing.T) {
	t.Helper()
	if d := c.diff(t); len(d) > 0 {
		t.Fatalf("nodes differ in %v", d)
	}
}

func both(c *cluster) []*testpg.Node { return []*testpg.Node{c.in, c.eu} }

func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
