package test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/testpg"
)

// version is one row version written by a successful operation (from RETURNING).
type version struct {
	at   time.Time
	node string
	row  map[string]any
}

// model is the expected final state: per (table, pk) all written versions and whether the row was
// deleted anywhere. Rules: delete wins; otherwise the version with the latest updated_at wins
// (identical: tie-break node); an item whose project is gone is gone.
type model struct {
	mu       sync.Mutex
	versions map[string][]version // "table/pk"
	deleted  map[string]bool
	ids      map[string][]string // table → known ids (for random picks)
}

func newModel() *model {
	return &model{versions: map[string][]version{}, deleted: map[string]bool{}, ids: map[string][]string{}}
}

func (m *model) add(table, node string, row map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idKey := "id"
	if table == "owner_login" {
		idKey = "owner_id"
	}
	id := row[idKey].(string)
	key := table + "/" + id
	at, err := time.Parse(time.RFC3339Nano, row["updated_at"].(string))
	if err != nil {
		panic(err)
	}
	if len(m.versions[key]) == 0 {
		m.ids[table] = append(m.ids[table], id)
	}
	m.versions[key] = append(m.versions[key], version{at: at, node: node, row: row})
}

func (m *model) del(table, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted[table+"/"+id] = true
}

func (m *model) pick(r *rand.Rand, table string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := m.ids[table]
	if len(ids) == 0 {
		return ""
	}
	return ids[r.IntN(len(ids))]
}

// explain logs the recorded history of one row.
func (m *model) explain(t *testing.T, table, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t.Logf("  model %s/%s deleted=%v", table, id, m.deleted[table+"/"+id])
	for _, v := range m.versions[table+"/"+id] {
		t.Logf("    version %s by %s: %v", v.at.Format(time.RFC3339Nano), v.node, v.row)
	}
	if table == "item" && len(m.versions[table+"/"+id]) > 0 {
		pid, _ := m.versions[table+"/"+id][0].row["project_id"].(string)
		t.Logf("  model project/%s deleted=%v versions=%d", pid, m.deleted["project/"+pid], len(m.versions["project/"+pid]))
	}
}

// expected returns table → pk → row JSON of the expected final state.
func (m *model) expected(tieBreak string) map[string]map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	win := map[string]version{}
	for key, vs := range m.versions {
		if m.deleted[key] {
			continue
		}
		best := vs[0]
		for _, v := range vs[1:] {
			if v.at.After(best.at) || (v.at.Equal(best.at) && v.node == tieBreak) {
				best = v
			}
		}
		win[key] = best
	}
	out := map[string]map[string]string{"project": {}, "item": {}, "owner_login": {}}
	for key, v := range win {
		var table, id string
		for i := range key {
			if key[i] == '/' {
				table, id = key[:i], key[i+1:]
				break
			}
		}
		if table == "item" {
			if _, ok := win["project/"+v.row["project_id"].(string)]; !ok {
				continue // parent gone
			}
		}
		b, _ := json.Marshal(v.row)
		out[table][id] = string(b)
	}
	return out
}

// actual reads the rows of a node in the same JSON shape.
func actual(t *testing.T, n *testpg.Node, table, key string) map[string]string {
	t.Helper()
	rows, err := n.Pool.Query(context.Background(), fmt.Sprintf(`SELECT %s, to_jsonb(x) FROM %s x`, key, table))
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

// procWorker runs the pg-bisync binary for one node (killable with SIGKILL).
type procWorker struct {
	bin, cfgPath string
	mu           sync.Mutex
	cmd          *exec.Cmd
}

func (w *procWorker) start(t *testing.T) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cmd != nil {
		return
	}
	cmd := exec.Command(w.bin, "run", "-config", w.cfgPath)
	if os.Getenv("PGBISYNC_LOG") == "1" {
		cmd.Stderr = os.Stderr
	}
	if err := cmd.Start(); err != nil {
		t.Error(err)
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

func TestChaos(t *testing.T) {
	testpg.Require(t)
	minutes, _ := strconv.Atoi(os.Getenv("PGBISYNC_CHAOS_MIN"))
	if minutes == 0 {
		minutes = 2
	}
	c := newCluster(t)
	bin := filepath.Join(t.TempDir(), "pg-bisync")
	if out, err := exec.Command("go", "build", "-o", bin, "../cmd/pg-bisync").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	workers := map[string]*procWorker{}
	for _, node := range []string{"in", "eu"} {
		cfg := c.cfg[node]
		p := filepath.Join(t.TempDir(), node+".json")
		b, _ := json.Marshal(map[string]any{"node": cfg.Node, "peer": cfg.Peer, "local_dsn": cfg.LocalDSN, "peer_dsn": cfg.PeerDSN,
			"unique_policy": cfg.UniquePolicy})
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		workers[node] = &procWorker{bin: bin, cfgPath: p}
		workers[node].start(t)
	}
	t.Cleanup(func() {
		for _, w := range workers {
			w.kill()
		}
	})

	m := newModel()
	// the seed is the starting state of the model
	for _, tbl := range []string{"project", "item", "owner_login"} {
		key := "id"
		if tbl == "owner_login" {
			key = "owner_id"
		}
		for _, js := range actual(t, c.in, tbl, key) {
			var row map[string]any
			_ = json.Unmarshal([]byte(js), &row)
			m.add(tbl, "seed", row)
		}
	}

	ctx, stop := context.WithTimeout(context.Background(), time.Duration(minutes)*time.Minute)
	defer stop()
	var wg sync.WaitGroup
	var ops, failed sync.Map
	for _, n := range both(c) {
		for g := 0; g < 2; g++ {
			wg.Add(1)
			go func(n *testpg.Node, seed uint64) {
				defer wg.Done()
				chaosOps(ctx, t, n, m, rand.New(rand.NewPCG(seed, seed)), &ops, &failed)
			}(n, uint64(time.Now().UnixNano())+uint64(g))
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		r := rand.New(rand.NewPCG(1, 2))
		restarted := false
		start := time.Now()
		for ctx.Err() == nil {
			sleep(ctx, time.Duration(15+r.IntN(20))*time.Second)
			if ctx.Err() != nil {
				break
			}
			node := []string{"in", "eu"}[r.IntN(2)]
			workers[node].kill()
			// restart a database: once in the middle, and with PGBISYNC_CHAOS_HARD=1 in 30 % of the cycles
			hard := os.Getenv("PGBISYNC_CHAOS_HARD") == "1" && r.IntN(10) < 3
			if hard || (!restarted && time.Since(start) > time.Duration(minutes)*time.Minute/2) {
				ctr := map[string]string{"in": c.in.Container, "eu": c.eu.Container}[[]string{"in", "eu"}[r.IntN(2)]]
				exec.Command("podman", "restart", "-t", "2", ctr).Run() //nolint:errcheck
				restarted = true
			}
			sleep(ctx, time.Duration(3+r.IntN(8))*time.Second)
			workers[node].start(t)
		}
	}()
	wg.Wait()
	for _, w := range workers {
		w.start(t)
	}
	c.waitSync(t, 3*time.Minute)
	time.Sleep(2 * time.Second)

	var total int
	ops.Range(func(k, v any) bool { total += v.(int); return true })
	t.Logf("operations: %d", total)
	failed.Range(func(k, v any) bool { t.Logf("local op errors %v: %d", k, v); return true })

	exp := m.expected(c.cfg["in"].TieBreakNode)
	for _, tbl := range []string{"project", "item", "owner_login"} {
		key := "id"
		if tbl == "owner_login" {
			key = "owner_id"
		}
		for _, n := range both(c) {
			got := actual(t, n, tbl, key)
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
					}
				}
			}
			for id, row := range got {
				if _, ok := exp[tbl][id]; !ok {
					extra++
					if extra <= 3 {
						t.Logf("%s extra %s %s: %s", n.Name, tbl, id, row)
						m.explain(t, tbl, id)
						for _, o := range both(c) {
							t.Logf("  %s tombstone: %q conflicts: %q", o.Name,
								o.Text(t, "SELECT deleted_at::text || coalesce(' replaced_by=' || replaced_by, '') FROM bisync.tombstone WHERE tbl = $1 AND pk = $2", tbl, id),
								o.Text(t, "SELECT string_agg(kind || ' ' || detail, '; ') FROM bisync.conflict WHERE pk = $1", id))
							if tbl == "item" {
								p := o.Text(t, "SELECT project_id FROM item WHERE id = $1", id)
								t.Logf("  %s item row: %q project %q exists=%q tombstone=%q", o.Name, o.Text(t, "SELECT to_jsonb(item)::text FROM item WHERE id = $1", id), p,
									o.Text(t, "SELECT count(*)::text FROM project WHERE id = $1", p), o.Text(t, "SELECT deleted_at::text FROM bisync.tombstone WHERE tbl = 'project' AND pk = $1", p))
							}
						}
					}
				}
			}
			if miss+extra+wrong > 0 {
				t.Errorf("%s %s vs model: %d missing, %d extra, %d wrong (expected %d rows)", n.Name, tbl, miss, extra, wrong, len(exp[tbl]))
			}
		}
	}
	c.requireEqual(t)
	for _, n := range both(c) {
		t.Logf("%s conflicts: %s", n.Name, n.Text(t, "SELECT coalesce(string_agg(kind || '=' || n, ' '), 'none') FROM (SELECT kind, count(*)::text n FROM bisync.conflict GROUP BY kind) x"))
	}
}

// deleteShare is the share of project deletes in percent (3, or 9 with PGBISYNC_CHAOS_HARD=1).
func deleteShare() int {
	if os.Getenv("PGBISYNC_CHAOS_HARD") == "1" {
		return 9
	}
	return 3
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// chaosOps runs random operations on one node until ctx ends and records them in the model.
func chaosOps(ctx context.Context, t *testing.T, n *testpg.Node, m *model, r *rand.Rand, ops, failed *sync.Map) {
	home := []string{"o1", "o2", "o3", "o4", "o5"}
	if n.Name == "eu" {
		home = []string{"o6", "o7", "o8", "o9", "o10"}
	}
	count := func(m *sync.Map, k string) {
		v, _ := m.LoadOrStore(k, 0)
		m.Store(k, v.(int)+1)
	}
	for i := 0; ctx.Err() == nil; i++ {
		op := r.IntN(100)
		var err error
		var name string
		switch {
		case op < 15:
			name = "create_project"
			id := fmt.Sprintf("c-%s-%d-%d", n.Name, r.Uint32(), i)
			err = one(n, m, "project", `INSERT INTO project (id, owner_id, name) VALUES ($1, $2, 'new') RETURNING to_jsonb(project.*)`, id, home[r.IntN(len(home))])
		case op < 40:
			name = "rename_project"
			err = one(n, m, "project", `UPDATE project SET name = $1 WHERE id = $2 RETURNING to_jsonb(project.*)`, fmt.Sprintf("r-%s-%d", n.Name, i), m.pick(r, "project"))
		case op < 50:
			name = "notes"
			err = one(n, m, "project", `UPDATE project SET notes = $1 WHERE id = $2 RETURNING to_jsonb(project.*)`, fmt.Sprintf("n-%d", r.IntN(1000)), m.pick(r, "project"))
		case op < 50+deleteShare():
			name = "delete_project"
			id := m.pick(r, "project")
			var tag int64
			tag, err = exec1(n, `DELETE FROM project WHERE id = $1`, id)
			if err == nil && tag == 1 {
				m.del("project", id)
			}
		case op < 70:
			name = "create_item"
			id := fmt.Sprintf("ci-%s-%d-%d", n.Name, r.Uint32(), i)
			err = one(n, m, "item", `INSERT INTO item (id, project_id, pos) VALUES ($1, $2, $3) RETURNING to_jsonb(item.*)`, id, m.pick(r, "project"), r.IntN(100))
		case op < 85:
			name = "move_item"
			err = one(n, m, "item", `UPDATE item SET pos = $1 WHERE id = $2 RETURNING to_jsonb(item.*)`, r.IntN(1000), m.pick(r, "item"))
		case op < 90:
			name = "delete_item"
			id := m.pick(r, "item")
			var tag int64
			tag, err = exec1(n, `DELETE FROM item WHERE id = $1`, id)
			if err == nil && tag == 1 {
				m.del("item", id)
			}
		default:
			name = "login"
			err = one(n, m, "owner_login", `UPDATE owner_login SET failed = failed + 1 WHERE owner_id = $1 RETURNING to_jsonb(owner_login.*)`, home[r.IntN(len(home))])
		}
		if err != nil {
			count(failed, name) // FK race with a delete or the node restarting: not part of the model
		} else {
			count(ops, name)
		}
		time.Sleep(time.Duration(5+r.IntN(20)) * time.Millisecond)
	}
}

func exec1(n *testpg.Node, sql string, args ...any) (int64, error) {
	tag, err := n.Pool.Exec(context.Background(), sql, args...)
	return tag.RowsAffected(), err
}

// one runs a statement returning to_jsonb(row) and records the version (0 rows: nothing to record).
func one(n *testpg.Node, m *model, table, sql string, args ...any) error {
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
