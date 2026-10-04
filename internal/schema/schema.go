// Package schema installs and removes the pg-bisync objects in one database: schema bisync
// (tombstones, conflicts, status), the publication of the synced tables and two small triggers
// per table (last-change stamp on UPDATE, tombstone on DELETE).
package schema

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/thomaskhub/pg-bisync/internal/config"
)

// DB is the subset of pgx used here (implemented by *pgx.Conn, pgx.Tx and *pgxpool.Pool).
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Both triggers do nothing while a replication origin is set up: rows applied by the worker keep
// the peer's timestamp, and the worker writes tombstones for applied deletes itself.
const baseSQL = `
CREATE SCHEMA IF NOT EXISTS bisync;
CREATE TABLE IF NOT EXISTS bisync.tombstone (
  tbl text NOT NULL,
  pk text NOT NULL,
  deleted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (tbl, pk)
);
-- unique-key merge: the losing row is replaced by the winner; references are redirected to it
ALTER TABLE bisync.tombstone ADD COLUMN IF NOT EXISTS replaced_by text;
CREATE TABLE IF NOT EXISTS bisync.conflict (
  id bigserial PRIMARY KEY,
  at timestamptz NOT NULL DEFAULT clock_timestamp(),
  kind text NOT NULL,
  tbl text NOT NULL,
  pk text NOT NULL,
  detail text NOT NULL DEFAULT '',
  resolved boolean NOT NULL DEFAULT false
);
CREATE TABLE IF NOT EXISTS bisync.status (
  node text PRIMARY KEY,
  state text NOT NULL,
  message text NOT NULL DEFAULT '',
  last_apply timestamptz,
  last_lsn pg_lsn,
  applied_tx bigint NOT NULL DEFAULT 0,
  updated timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- SECURITY DEFINER: the triggers run for every application role, which needs no rights on schema
-- bisync or the replication origin functions; search_path is fixed so the definer rights cannot be
-- redirected to other objects
CREATE OR REPLACE FUNCTION bisync.tomb() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF NOT pg_replication_origin_session_is_setup() THEN
    INSERT INTO bisync.tombstone (tbl, pk) VALUES (TG_TABLE_NAME, to_jsonb(OLD) ->> TG_ARGV[0])
    ON CONFLICT (tbl, pk) DO UPDATE SET deleted_at = excluded.deleted_at;
  END IF;
  RETURN OLD;
END $$;
`

// stampSQL sets the last-change column. The column name is written into the function (not passed
// as an argument): assigning a single field keeps large (TOAST) values untouched, so an UPDATE of
// other columns neither rewrites them nor sends them in the change stream.
const stampSQL = `CREATE OR REPLACE FUNCTION bisync.stamp() RETURNS trigger LANGUAGE plpgsql
  SECURITY DEFINER SET search_path = pg_catalog, pg_temp AS $$
BEGIN
  IF NOT pg_replication_origin_session_is_setup() THEN
    NEW.%s := clock_timestamp();
  END IF;
  RETURN NEW;
END $$`

// Table is a synced table with its single-column primary key.
type Table struct {
	Name string
	PK   string
}

// Tables returns the synced tables: cfg.Tables or every table of cfg.Schema with a single-column
// primary key and the updated column. Other tables are skipped (they are not synced).
func Tables(ctx context.Context, db DB, cfg config.Config) ([]Table, error) {
	rows, err := db.Query(ctx, `
		SELECT c.relname, a.attname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = i.indkey[0]
		WHERE i.indisprimary AND i.indnatts = 1 AND n.nspname = $1 AND c.relkind = 'r'
		  AND EXISTS (SELECT 1 FROM pg_attribute u
		              WHERE u.attrelid = c.oid AND u.attname = $2 AND NOT u.attisdropped)
		ORDER BY c.relname`, cfg.Schema, cfg.UpdatedColumn)
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	all, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Table])
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	if len(cfg.Tables) == 0 {
		return all, nil
	}
	byName := make(map[string]Table, len(all))
	for _, t := range all {
		byName[t.Name] = t
	}
	out := make([]Table, 0, len(cfg.Tables))
	for _, name := range cfg.Tables {
		t, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("table %s.%s: missing, no single-column primary key, or no %s column", cfg.Schema, name, cfg.UpdatedColumn)
		}
		out = append(out, t)
	}
	return out, nil
}

// Install creates (or updates) schema bisync, the triggers and the publication. Idempotent.
func Install(ctx context.Context, db DB, cfg config.Config) ([]Table, error) {
	if _, err := db.Exec(ctx, baseSQL); err != nil {
		return nil, fmt.Errorf("install base objects: %w", err)
	}
	if _, err := db.Exec(ctx, fmt.Sprintf(stampSQL, pgx.Identifier{cfg.UpdatedColumn}.Sanitize())); err != nil {
		return nil, fmt.Errorf("install stamp function: %w", err)
	}
	tables, err := Tables(ctx, db, cfg)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no table in schema %s qualifies for sync", cfg.Schema)
	}
	if err := checkMergeForeignKeys(ctx, db, cfg, tables); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		qt := pgx.Identifier{cfg.Schema, t.Name}.Sanitize()
		stmts := []string{
			fmt.Sprintf("DROP TRIGGER IF EXISTS bisync_stamp ON %s", qt),
			fmt.Sprintf("CREATE TRIGGER bisync_stamp BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION bisync.stamp()", qt),
			fmt.Sprintf("DROP TRIGGER IF EXISTS bisync_tomb ON %s", qt),
			fmt.Sprintf("CREATE TRIGGER bisync_tomb AFTER DELETE ON %s FOR EACH ROW EXECUTE FUNCTION bisync.tomb(%s)", qt, quoteLiteral(t.PK)),
		}
		for _, s := range stmts {
			if _, err := db.Exec(ctx, s); err != nil {
				return nil, fmt.Errorf("triggers on %s: %w", qt, err)
			}
		}
		names = append(names, qt)
	}
	pub := pgx.Identifier{cfg.Publication}.Sanitize()
	var exists bool
	if err := queryRow(ctx, db, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", []any{cfg.Publication}, &exists); err != nil {
		return nil, err
	}
	list := strings.Join(names, ", ")
	stmt := fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')", pub, list)
	if exists {
		stmt = fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", pub, list)
	}
	if _, err := db.Exec(ctx, stmt); err != nil {
		return nil, fmt.Errorf("publication: %w", err)
	}
	return tables, nil
}

// checkMergeForeignKeys: a unique-key merge redirects references from the losing row to the
// winner inside one transaction, which needs the referencing foreign keys to be DEFERRABLE.
func checkMergeForeignKeys(ctx context.Context, db DB, cfg config.Config, tables []Table) error {
	for _, t := range tables {
		if cfg.UniquePolicyFor(t.Name) != config.UniqueMerge {
			continue
		}
		rows, err := db.Query(ctx, `
			SELECT k.conname FROM pg_constraint k
			JOIN pg_class r ON r.oid = k.confrelid JOIN pg_namespace n ON n.oid = r.relnamespace
			WHERE k.contype = 'f' AND NOT k.condeferrable AND n.nspname = $1 AND r.relname = $2`, cfg.Schema, t.Name)
		if err != nil {
			return fmt.Errorf("check foreign keys to %s: %w", t.Name, err)
		}
		bad, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("check foreign keys to %s: %w", t.Name, err)
		}
		if len(bad) > 0 {
			return fmt.Errorf("unique_policy merge on %s: foreign keys %v must be DEFERRABLE", t.Name, bad)
		}
	}
	return nil
}

// Uninstall removes triggers, publication, the replication origin and schema bisync.
func Uninstall(ctx context.Context, db DB, cfg config.Config) error {
	tables, err := Tables(ctx, db, cfg)
	if err != nil {
		return err
	}
	for _, t := range tables {
		qt := pgx.Identifier{cfg.Schema, t.Name}.Sanitize()
		for _, trg := range []string{"bisync_stamp", "bisync_tomb"} {
			if _, err := db.Exec(ctx, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s", trg, qt)); err != nil {
				return fmt.Errorf("drop trigger on %s: %w", qt, err)
			}
		}
	}
	stmts := []string{
		fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pgx.Identifier{cfg.Publication}.Sanitize()),
		"DROP SCHEMA IF EXISTS bisync CASCADE",
	}
	for _, s := range stmts {
		if _, err := db.Exec(ctx, s); err != nil {
			return fmt.Errorf("uninstall: %w", err)
		}
	}
	if _, err := db.Exec(ctx, `SELECT pg_replication_origin_drop(roname) FROM pg_replication_origin WHERE roname = $1`, cfg.OriginName()); err != nil {
		return fmt.Errorf("drop replication origin: %w", err)
	}
	return nil
}

func queryRow(ctx context.Context, db DB, sql string, args []any, dest ...any) error {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return pgx.ErrNoRows
	}
	if err := rows.Scan(dest...); err != nil {
		return err
	}
	return rows.Err()
}

func quoteLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
