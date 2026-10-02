package test

import (
	"context"
	"strings"
	"testing"
	"time"
)

// grantSQL gives role bisync exactly the rights documented in the README.
const grantSQL = `
CREATE ROLE bisync LOGIN REPLICATION PASSWORD 'bisync';
CREATE ROLE app LOGIN PASSWORD 'app';
GRANT USAGE ON SCHEMA public, bisync TO bisync, app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO bisync, app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bisync TO bisync;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bisync TO bisync;
GRANT EXECUTE ON FUNCTION pg_replication_origin_create(text), pg_replication_origin_session_setup(text),
  pg_replication_origin_session_reset(), pg_replication_origin_xact_setup(pg_lsn, timestamptz),
  pg_replication_origin_progress(text, boolean), pg_replication_origin_drop(text) TO bisync;
`

// TestLeastPrivilege: the worker runs as a non-superuser role with only the documented rights,
// and the application writes as a plain role (no rights on schema bisync).
func TestLeastPrivilege(t *testing.T) {
	c := newCluster(t)
	for _, n := range both(c) {
		n.Exec(t, grantSQL)
	}
	sub := func(dsn, user string) string { return strings.Replace(dsn, "postgres:bisync@", user+"@", 1) }
	for node, cfg := range c.cfg {
		cfg.LocalDSN = sub(cfg.LocalDSN, "bisync:bisync")
		cfg.PeerDSN = sub(cfg.PeerDSN, "bisync:bisync")
		c.cfg[node] = cfg
	}
	c.start()
	c.waitSync(t, 30*time.Second)
	ctx := context.Background()
	app := map[string]string{"in": sub(c.in.DSN, "app:app"), "eu": sub(c.eu.DSN, "app:app")}
	for _, n := range both(c) {
		conn := connect(t, app[n.Name])
		defer conn.Close(ctx)
		for _, s := range []string{
			"UPDATE project SET name = 'priv-" + n.Name + "' WHERE id = 'p" + map[string]string{"in": "1", "eu": "6"}[n.Name] + "-1'",
			"DELETE FROM item WHERE id = 'i" + map[string]string{"in": "2", "eu": "7"}[n.Name] + "-1-1'",
			"INSERT INTO project (id, owner_id, name) VALUES ('priv-" + n.Name + "', 'o1', 'x')",
		} {
			if _, err := conn.Exec(ctx, s); err != nil {
				t.Fatalf("app on %s: %s: %v", n.Name, s, err)
			}
		}
	}
	c.waitSync(t, 30*time.Second)
	c.requireEqual(t)
	for _, n := range both(c) {
		if got := n.Text(t, "SELECT state FROM bisync.status"); got != "running" {
			t.Errorf("%s: state %q: %s", n.Name, got, n.Text(t, "SELECT message FROM bisync.status"))
		}
	}
}
