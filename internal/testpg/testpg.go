// Package testpg starts two throw-away PostgreSQL 17 containers for integration tests
// (podman or docker, see PGBISYNC_CONTAINER). Tests using it run only with PGBISYNC_IT=1.
package testpg

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	image        = "docker.io/library/postgres:17"
	password     = "bisync"
	readyTimeout = 60 * time.Second
)

// Node is one database container.
type Node struct {
	Name      string
	Container string
	DSN       string
	Pool      *pgxpool.Pool
	owner     *testing.T // test that started the node: owns the pool cleanup
}

// Require skips the test unless integration tests are enabled.
func Require(t *testing.T) {
	t.Helper()
	if os.Getenv("PGBISYNC_IT") != "1" {
		t.Skip("integration test: set PGBISYNC_IT=1 (needs podman or docker)")
	}
}

func engine() string {
	if e := os.Getenv("PGBISYNC_CONTAINER"); e != "" {
		return e
	}
	if _, err := exec.LookPath("podman"); err == nil {
		return "podman"
	}
	return "docker"
}

func run(t *testing.T, args ...string) string {
	t.Helper()
	out, err := exec.Command(engine(), args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", engine(), strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// Start starts a node with logical replication enabled and the given schema file loaded.
func Start(t *testing.T, name, schemaFile string, pgArgs ...string) *Node {
	t.Helper()
	ctr := fmt.Sprintf("pgbisync-test-%s-%d", name, time.Now().UnixNano())
	args := []string{"run", "-d", "--name", ctr, "-e", "POSTGRES_PASSWORD=" + password, "-e", "POSTGRES_DB=bisync",
		"-p", "127.0.0.1::5432", image, "-c", "wal_level=logical", "-c", "max_replication_slots=10", "-c", "max_wal_senders=10"}
	run(t, append(args, pgArgs...)...)
	t.Cleanup(func() {
		if t.Failed() && os.Getenv("PGBISYNC_KEEP") == "1" {
			t.Logf("keeping container %s (PGBISYNC_KEEP=1)", ctr)
			return
		}
		_ = exec.Command(engine(), "rm", "-f", ctr).Run()
	})
	port := run(t, "port", ctr, "5432/tcp")
	port = port[strings.LastIndex(port, ":")+1:]
	n := &Node{Name: name, Container: ctr, DSN: fmt.Sprintf("postgres://postgres:%s@127.0.0.1:%s/bisync?sslmode=disable", password, port), owner: t}
	t.Cleanup(func() {
		if n.Pool != nil {
			n.Pool.Close()
		}
	})
	n.waitReady(t)
	if schemaFile != "" {
		b, err := os.ReadFile(schemaFile)
		if err != nil {
			t.Fatal(err)
		}
		n.Exec(t, string(b))
	}
	return n
}

func (n *Node) waitReady(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := pgx.Connect(ctx, n.DSN)
		if err == nil {
			err = conn.Ping(ctx)
			_ = conn.Close(ctx)
		}
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s not ready: %v", n.Name, err)
		}
		time.Sleep(300 * time.Millisecond)
	}
	if n.Pool != nil {
		n.Pool.Close()
	}
	pool, err := pgxpool.New(context.Background(), n.DSN)
	if err != nil {
		t.Fatal(err)
	}
	n.Pool = pool
}

// Stop stops the container (keeps its data).
func (n *Node) Stop(t *testing.T) { t.Helper(); run(t, "stop", "-t", "2", n.Container) }

// StartAgain starts a stopped container and waits until it accepts connections.
func (n *Node) StartAgain(t *testing.T) {
	t.Helper()
	run(t, "start", n.Container)
	n.waitReady(t)
}

// Pause freezes the container (network partition from the outside view).
func (n *Node) Pause(t *testing.T) { t.Helper(); run(t, "pause", n.Container) }

// Unpause resumes a paused container.
func (n *Node) Unpause(t *testing.T) { t.Helper(); run(t, "unpause", n.Container) }

// Exec runs SQL (multiple statements allowed without args) and fails the test on error.
func (n *Node) Exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := n.Pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v\nSQL: %s", n.Name, err, sql)
	}
}

// Text returns the first column of the first row as text ("" for no row / NULL).
func (n *Node) Text(t *testing.T, sql string, args ...any) string {
	t.Helper()
	var s *string
	err := n.Pool.QueryRow(context.Background(), sql, args...).Scan(&s)
	if err == pgx.ErrNoRows || (err == nil && s == nil) {
		return ""
	}
	if err != nil {
		t.Fatalf("%s: %v\nSQL: %s", n.Name, err, sql)
	}
	return *s
}

// Checksum returns an md5 over all rows of the given tables (ordered by primary key "id" or the
// first column), for equality checks between nodes.
func (n *Node) Checksum(t *testing.T, tables ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, tbl := range tables {
		out[tbl] = n.Text(t, fmt.Sprintf(`SELECT md5(coalesce(string_agg(x::text, '|' ORDER BY x::text), '')) FROM %s x`, pgx.Identifier{tbl}.Sanitize()))
	}
	return out
}
