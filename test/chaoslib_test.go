package test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/testpg"
)

// ---------------------------------------------------------------------------------------------
// Reference model (compact: constant memory per row). Rules: delete wins; otherwise the version
// with the latest updated_at wins (identical: tie-break node); an item whose project is gone is
// gone. The last few versions are kept per row for diagnostics only.

const historyKept = 4

type version struct {
	at   time.Time
	node string
	row  map[string]any
}

type modelRow struct {
	best    version
	history []version
}

type model struct {
	mu       sync.Mutex
	tieBreak string
	rows     map[string]*modelRow // "table/pk"
	deleted  map[string]bool
	ids      map[string][]string // table → ids that may still exist (random picks)
}

func newModel(tieBreak string) *model {
	return &model{tieBreak: tieBreak, rows: map[string]*modelRow{}, deleted: map[string]bool{}, ids: map[string][]string{}}
}

func keyColumn(table string) string {
	if table == "owner_login" {
		return "owner_id"
	}
	return "id"
}

func (m *model) add(table, node string, row map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := row[keyColumn(table)].(string)
	key := table + "/" + id
	at, err := time.Parse(time.RFC3339Nano, row["updated_at"].(string))
	if err != nil {
		panic(err)
	}
	v := version{at: at, node: node, row: row}
	r, ok := m.rows[key]
	if !ok {
		m.rows[key] = &modelRow{best: v, history: []version{v}}
		m.ids[table] = append(m.ids[table], id)
		return
	}
	if v.at.After(r.best.at) || (v.at.Equal(r.best.at) && v.node == m.tieBreak) {
		r.best = v
	}
	r.history = append(r.history, v)
	if len(r.history) > historyKept {
		r.history = r.history[1:]
	}
}

func (m *model) del(table, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted[table+"/"+id] = true
}

// pick returns a random id of table, preferring rows that are not deleted.
func (m *model) pick(r *rand.Rand, table string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.ids[table]
	for tries := 0; tries < 5 && len(ids) > 0; tries++ {
		i := r.IntN(len(ids))
		if !m.deleted[table+"/"+ids[i]] {
			return ids[i]
		}
		ids[i] = ids[len(ids)-1] // forget deleted ids (their row/tombstone stays in the model)
		ids = ids[:len(ids)-1]
	}
	m.ids[table] = ids
	if len(ids) == 0 {
		return ""
	}
	return ids[r.IntN(len(ids))]
}

// compact drops deleted rows' versions (the deleted marker stays): keeps memory bounded on soaks.
func (m *model) compact() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key := range m.deleted {
		delete(m.rows, key)
	}
}

func (m *model) expected() map[string]map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]map[string]string{"project": {}, "item": {}, "owner_login": {}}
	alive := func(key string) bool { _, ok := m.rows[key]; return ok && !m.deleted[key] }
	for key, r := range m.rows {
		if m.deleted[key] {
			continue
		}
		table, id, _ := strings.Cut(key, "/")
		if table == "item" && !alive("project/"+r.best.row["project_id"].(string)) {
			continue // parent gone
		}
		b, _ := json.Marshal(r.best.row)
		out[table][id] = string(b)
	}
	return out
}

func (m *model) explain(t *testing.T, table, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := table + "/" + id
	t.Logf("  model %s deleted=%v", key, m.deleted[key])
	if r, ok := m.rows[key]; ok {
		for _, v := range r.history {
			t.Logf("    version %s by %s: %v", v.at.Format(time.RFC3339Nano), v.node, v.row)
		}
		if table == "item" {
			pid, _ := r.best.row["project_id"].(string)
			t.Logf("  model project/%s deleted=%v", pid, m.deleted["project/"+pid])
		}
	}
}

func (m *model) size() (rows, deleted int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.rows), len(m.deleted)
}

// actual reads the rows of a node in the model's JSON shape.
func actual(t *testing.T, n *testpg.Node, table string) map[string]string {
	t.Helper()
	rows, err := n.Pool.Query(context.Background(), fmt.Sprintf(`SELECT %s, to_jsonb(x) FROM %s x`, keyColumn(table), table))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	var id string
	var row map[string]any
	_, err = pgx.ForEachRow(rows, []any{&id, &row}, func() error {
		row["updated_at"] = normTime(row["updated_at"].(string))
		b, _ := json.Marshal(row)
		out[id] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func normTime(s string) string {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return ts.UTC().Format(time.RFC3339Nano)
}

// seedModel loads the current rows of a node as the model's starting state.
func seedModel(t *testing.T, m *model, n *testpg.Node) {
	t.Helper()
	for _, tbl := range []string{"project", "item", "owner_login"} {
		for _, js := range actual(t, n, tbl) {
			var row map[string]any
			_ = json.Unmarshal([]byte(js), &row)
			m.add(tbl, "seed", row)
		}
	}
}

// checkModel compares both nodes with the model and with each other; returns the problems.
func checkModel(t *testing.T, c *cluster, m *model) []string {
	t.Helper()
	exp := m.expected()
	var problems []string
	for _, tbl := range []string{"project", "item", "owner_login"} {
		for _, n := range both(c) {
			got := actual(t, n, tbl)
			miss, extra, wrong := 0, 0, 0
			for id, want := range exp[tbl] {
				g, ok := got[id]
				switch {
				case !ok:
					miss++
				case g != want:
					wrong++
					if wrong <= 3 {
						t.Logf("%s %s %s:\n got  %s\n want %s", n.Name, tbl, id, g, want)
						m.explain(t, tbl, id)
					}
				}
			}
			for id, row := range got {
				if _, ok := exp[tbl][id]; ok {
					continue
				}
				extra++
				if extra <= 3 {
					t.Logf("%s extra %s %s: %s", n.Name, tbl, id, row)
					m.explain(t, tbl, id)
				}
			}
			if miss+extra+wrong > 0 {
				problems = append(problems, fmt.Sprintf("%s %s vs model: %d missing, %d extra, %d wrong (expected %d)", n.Name, tbl, miss, extra, wrong, len(exp[tbl])))
			}
		}
	}
	if d := c.diff(t); len(d) > 0 {
		problems = append(problems, fmt.Sprintf("nodes differ in %v", d))
	}
	return problems
}

// ---------------------------------------------------------------------------------------------
// Workers as processes (killable with SIGKILL)

type procWorker struct {
	bin, cfgPath, logPath string
	mu                    sync.Mutex
	cmd                   *exec.Cmd
}

func (w *procWorker) start(t *testing.T) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cmd != nil {
		return
	}
	cmd := exec.Command(w.bin, "run", "-config", w.cfgPath)
	if w.logPath != "" {
		if f, err := os.OpenFile(w.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644); err == nil {
			cmd.Stderr = f
		}
	} else if os.Getenv("PGBISYNC_LOG") == "1" {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		if t != nil {
			t.Error(err)
		}
		return
	}
	w.cmd = cmd
}

func (w *procWorker) kill() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cmd == nil {
		return
	}
	_ = w.cmd.Process.Signal(syscall.SIGKILL)
	_ = w.cmd.Wait()
	w.cmd = nil
}

func (w *procWorker) pid() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cmd == nil {
		return 0
	}
	return w.cmd.Process.Pid
}

// binary returns the pg-bisync binary: PGBISYNC_BIN or a fresh build.
func binary(t *testing.T) string {
	t.Helper()
	if b := os.Getenv("PGBISYNC_BIN"); b != "" {
		return b
	}
	bin := filepath.Join(t.TempDir(), "pg-bisync")
	if out, err := exec.Command("go", "build", "-o", bin, "../cmd/pg-bisync").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

// startWorkers writes node configs (peer DSN through a proxy if given) and starts both workers.
func startWorkers(t *testing.T, c *cluster, peerDSN map[string]string, logDir string) map[string]*procWorker {
	t.Helper()
	bin := binary(t)
	workers := map[string]*procWorker{}
	for _, node := range []string{"in", "eu"} {
		cfg := c.cfg[node]
		peer := cfg.PeerDSN
		if peerDSN != nil && peerDSN[node] != "" {
			peer = peerDSN[node]
		}
		p := filepath.Join(t.TempDir(), node+".json")
		b, _ := json.Marshal(map[string]any{"node": cfg.Node, "peer": cfg.Peer, "local_dsn": cfg.LocalDSN, "peer_dsn": peer, "unique_policy": cfg.UniquePolicy})
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		w := &procWorker{bin: bin, cfgPath: p}
		if logDir != "" {
			w.logPath = filepath.Join(logDir, "worker-"+node+".log")
		}
		w.start(t)
		workers[node] = w
	}
	t.Cleanup(func() {
		for _, w := range workers {
			w.kill()
		}
	})
	return workers
}

// ---------------------------------------------------------------------------------------------
// Load

// opMix is the share (percent) of each operation; the rest is "login".
type opMix struct{ create, rename, notes, deleteProject, createItem, moveItem, deleteItem int }

var defaultMix = opMix{create: 15, rename: 25, notes: 10, deleteProject: 3, createItem: 17, moveItem: 15, deleteItem: 5}

// balancedMix creates and deletes about as many rows as it creates (soak: bounded table size).
var balancedMix = opMix{create: 8, rename: 25, notes: 10, deleteProject: 6, createItem: 15, moveItem: 20, deleteItem: 10}

type load struct {
	m      *model
	mix    opMix
	delay  time.Duration // pause between operations per goroutine
	gate   sync.RWMutex  // checkpoints take the write lock: no operation in flight
	ops    sync.Map
	failed sync.Map
}

func (l *load) totals() (ok, failed int64) {
	l.ops.Range(func(_, v any) bool { ok += atomic.LoadInt64(v.(*int64)); return true })
	l.failed.Range(func(_, v any) bool { failed += atomic.LoadInt64(v.(*int64)); return true })
	return ok, failed
}

// run executes random operations on n until ctx ends.
func (l *load) run(ctx context.Context, n *testpg.Node, seed uint64) {
	r := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	home := []string{"o1", "o2", "o3", "o4", "o5"}
	if n.Name == "eu" {
		home = []string{"o6", "o7", "o8", "o9", "o10"}
	}
	for i := 0; ctx.Err() == nil; i++ {
		l.gate.RLock()
		name, err := l.one(n, r, home, i)
		l.gate.RUnlock()
		k := &l.ops
		if err != nil {
			k = &l.failed // FK race with a delete or the node restarting: not part of the model
		}
		v, _ := k.LoadOrStore(name, new(int64))
		atomic.AddInt64(v.(*int64), 1)
		sleepCtx(ctx, l.delay+time.Duration(r.IntN(int(l.delay/2+1))))
	}
}

func (l *load) one(n *testpg.Node, r *rand.Rand, home []string, i int) (string, error) {
	mx, op := l.mix, r.IntN(100)
	m := l.m
	at := 0
	next := func(share int) bool { at += share; return op < at }
	switch {
	case next(mx.create):
		id := fmt.Sprintf("c-%s-%d-%d", n.Name, r.Uint32(), i)
		return "create_project", record(n, m, "project", `INSERT INTO project (id, owner_id, name) VALUES ($1, $2, 'new') RETURNING to_jsonb(project.*)`, id, home[r.IntN(len(home))])
	case next(mx.rename):
		return "rename_project", record(n, m, "project", `UPDATE project SET name = $1 WHERE id = $2 RETURNING to_jsonb(project.*)`, fmt.Sprintf("r-%s-%d", n.Name, i), m.pick(r, "project"))
	case next(mx.notes):
		return "notes", record(n, m, "project", `UPDATE project SET notes = $1 WHERE id = $2 RETURNING to_jsonb(project.*)`, fmt.Sprintf("n-%d", r.IntN(1000)), m.pick(r, "project"))
	case next(mx.deleteProject):
		id := m.pick(r, "project")
		tag, err := n.Pool.Exec(context.Background(), `DELETE FROM project WHERE id = $1`, id)
		if err == nil && tag.RowsAffected() == 1 {
			m.del("project", id)
		}
		return "delete_project", err
	case next(mx.createItem):
		id := fmt.Sprintf("ci-%s-%d-%d", n.Name, r.Uint32(), i)
		return "create_item", record(n, m, "item", `INSERT INTO item (id, project_id, pos) VALUES ($1, $2, $3) RETURNING to_jsonb(item.*)`, id, m.pick(r, "project"), r.IntN(100))
	case next(mx.moveItem):
		return "move_item", record(n, m, "item", `UPDATE item SET pos = $1 WHERE id = $2 RETURNING to_jsonb(item.*)`, r.IntN(1000), m.pick(r, "item"))
	case next(mx.deleteItem):
		id := m.pick(r, "item")
		tag, err := n.Pool.Exec(context.Background(), `DELETE FROM item WHERE id = $1`, id)
		if err == nil && tag.RowsAffected() == 1 {
			m.del("item", id)
		}
		return "delete_item", err
	default:
		return "login", record(n, m, "owner_login", `UPDATE owner_login SET failed = failed + 1 WHERE owner_id = $1 RETURNING to_jsonb(owner_login.*)`, home[r.IntN(len(home))])
	}
}

// record runs a statement returning to_jsonb(row) and adds the version to the model.
func record(n *testpg.Node, m *model, table, sql string, args ...any) error {
	var row map[string]any
	err := n.Pool.QueryRow(context.Background(), sql, args...).Scan(&row)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	row["updated_at"] = normTime(row["updated_at"].(string))
	m.add(table, n.Name, row)
	return nil
}

func sleepCtx(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// restartContainer restarts a node's container without waiting (safe from any goroutine).
func restartContainer(n *testpg.Node) {
	engine := os.Getenv("PGBISYNC_CONTAINER")
	if engine == "" {
		engine = "podman"
		if _, err := exec.LookPath("podman"); err != nil {
			engine = "docker"
		}
	}
	_ = exec.Command(engine, "restart", "-t", "2", n.Container).Run()
}
