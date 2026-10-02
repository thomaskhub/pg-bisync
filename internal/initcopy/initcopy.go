// Package initcopy makes an empty node a copy of its peer: it creates the slot this node reads
// from with an exported snapshot, copies every synced table (and the peer's tombstones) inside
// that snapshot and sets the local replication origin to the slot's starting point, so the
// stream continues exactly after the copy.
package initcopy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/schema"
	"github.com/thomkin/pg-bisync/internal/stream"
)

// Report summarises a copy.
type Report struct {
	Rows     map[string]int64 `json:"rows"`
	StartLSN string           `json:"start_lsn"`
	Duration string           `json:"duration"`
}

// Run copies the peer into the (empty) local database.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) (Report, error) {
	t0 := time.Now()
	rep := Report{Rows: map[string]int64{}}
	local, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return rep, fmt.Errorf("connect local: %w", err)
	}
	defer local.Close(context.Background())
	tables, err := schema.Tables(ctx, local, cfg)
	if err != nil {
		return rep, err
	}
	if err := requireEmpty(ctx, local, cfg, tables); err != nil {
		return rep, err
	}
	ordered, err := fkOrder(ctx, local, cfg.Schema, tables)
	if err != nil {
		return rep, err
	}
	// the peer reads from this node: its slot must exist before anything is written locally
	if err := (stream.Reader{PeerDSN: cfg.LocalDSN, Slot: "bisync_" + cfg.Peer}).EnsureSlot(ctx); err != nil {
		return rep, fmt.Errorf("slot for the peer on this node: %w", err)
	}
	repl, err := stream.Connect(ctx, cfg.PeerDSN)
	if err != nil {
		return rep, err
	}
	defer repl.Close(context.Background())
	if exists, err := stream.SlotExists(ctx, repl, cfg.SlotName()); err != nil {
		return rep, err
	} else if exists {
		log.Info("dropping existing slot on the peer", "slot", cfg.SlotName())
		if err := pglogrepl.DropReplicationSlot(ctx, repl, cfg.SlotName(), pglogrepl.DropReplicationSlotOptions{}); err != nil {
			return rep, fmt.Errorf("drop slot %s: %w", cfg.SlotName(), err)
		}
	}
	slot, err := pglogrepl.CreateReplicationSlot(ctx, repl, cfg.SlotName(), "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "EXPORT_SNAPSHOT"})
	if err != nil {
		return rep, fmt.Errorf("create slot %s: %w", cfg.SlotName(), err)
	}
	rep.StartLSN = slot.ConsistentPoint
	if err := copyAll(ctx, cfg, local, slot, ordered, &rep, log); err != nil {
		return rep, err
	}
	rep.Duration = time.Since(t0).Round(time.Millisecond).String()
	return rep, nil
}

func copyAll(ctx context.Context, cfg config.Config, local *pgx.Conn, slot pglogrepl.CreateReplicationSlotResult, tables []string, rep *Report, log *slog.Logger) error {
	peer, err := pgx.Connect(ctx, cfg.PeerDSN)
	if err != nil {
		return fmt.Errorf("connect peer: %w", err)
	}
	defer peer.Close(context.Background())
	if _, err := peer.Exec(ctx, "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"); err != nil {
		return fmt.Errorf("peer snapshot transaction: %w", err)
	}
	if _, err := peer.Exec(ctx, fmt.Sprintf("SET TRANSACTION SNAPSHOT '%s'", slot.SnapshotName)); err != nil {
		return fmt.Errorf("use exported snapshot: %w", err)
	}
	// copied rows carry the origin: the peer's stream from this node never sends them back
	for _, s := range []string{
		"SELECT pg_replication_origin_create($1) WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = $1)",
		"SELECT pg_replication_origin_session_setup($1)",
	} {
		if _, err := local.Exec(ctx, s, cfg.OriginName()); err != nil {
			return fmt.Errorf("replication origin: %w", err)
		}
	}
	defer func() { _, _ = local.Exec(context.Background(), "SELECT pg_replication_origin_session_reset()") }()
	tx, err := local.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, "SET CONSTRAINTS ALL DEFERRED"); err != nil {
		return err
	}
	all := append(tables, "") // "" = bisync.tombstone
	for _, t := range all {
		qt := pgx.Identifier{cfg.Schema, t}.Sanitize()
		if t == "" {
			qt = "bisync.tombstone"
		}
		n, err := copyTable(ctx, peer, local, qt)
		if err != nil {
			return err
		}
		rep.Rows[qt] = n
		log.Info("copied", "table", qt, "rows", n)
	}
	// stored with the copied rows at commit: the stream starts exactly after the snapshot
	if _, err := tx.Exec(ctx, "SELECT pg_replication_origin_xact_setup($1::text::pg_lsn, clock_timestamp())", slot.ConsistentPoint); err != nil {
		return fmt.Errorf("set origin to %s: %w", slot.ConsistentPoint, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit copy: %w", err)
	}
	_, _ = peer.Exec(ctx, "COMMIT")
	return nil
}

// copyTable streams COPY TO STDOUT on the peer into COPY FROM STDIN locally.
func copyTable(ctx context.Context, peer, local *pgx.Conn, qt string) (int64, error) {
	r, w := io.Pipe()
	errc := make(chan error, 1)
	go func() {
		_, err := peer.PgConn().CopyTo(ctx, w, "COPY "+qt+" TO STDOUT")
		_ = w.CloseWithError(err)
		errc <- err
	}()
	tag, err := local.PgConn().CopyFrom(ctx, r, "COPY "+qt+" FROM STDIN")
	_ = r.Close()
	if perr := <-errc; perr != nil {
		return 0, fmt.Errorf("copy %s from peer: %w", qt, perr)
	}
	if err != nil {
		return 0, fmt.Errorf("copy %s into local: %w", qt, err)
	}
	return tag.RowsAffected(), nil
}

func requireEmpty(ctx context.Context, conn *pgx.Conn, cfg config.Config, tables []schema.Table) error {
	for _, t := range tables {
		var any bool
		q := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s)", pgx.Identifier{cfg.Schema, t.Name}.Sanitize())
		if err := conn.QueryRow(ctx, q).Scan(&any); err != nil {
			return fmt.Errorf("check %s: %w", t.Name, err)
		}
		if any {
			return fmt.Errorf("init needs empty tables, %s has rows", t.Name)
		}
	}
	return nil
}

// fkOrder sorts the tables so that referenced tables come first (self references are fine).
func fkOrder(ctx context.Context, conn *pgx.Conn, schemaName string, tables []schema.Table) ([]string, error) {
	in := map[string]bool{}
	for _, t := range tables {
		in[t.Name] = true
	}
	rows, err := conn.Query(ctx, `
		SELECT c.relname, r.relname FROM pg_constraint k
		JOIN pg_class c ON c.oid = k.conrelid JOIN pg_class r ON r.oid = k.confrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE k.contype = 'f' AND n.nspname = $1 AND c.oid <> r.oid`, schemaName)
	if err != nil {
		return nil, fmt.Errorf("foreign keys: %w", err)
	}
	deps := map[string][]string{}
	var child, parent string
	if _, err := pgx.ForEachRow(rows, []any{&child, &parent}, func() error {
		if in[child] && in[parent] {
			deps[child] = append(deps[child], parent)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("foreign keys: %w", err)
	}
	names := make([]string, 0, len(tables))
	for _, t := range tables {
		names = append(names, t.Name)
	}
	sort.Strings(names)
	var out []string
	state := map[string]int{} // 1 visiting, 2 done
	var visit func(string) error
	visit = func(n string) error {
		switch state[n] {
		case 1:
			return fmt.Errorf("foreign key cycle involving %s: init cannot order the copy", n)
		case 2:
			return nil
		}
		state[n] = 1
		for _, p := range deps[n] {
			if err := visit(p); err != nil {
				return err
			}
		}
		state[n] = 2
		out = append(out, n)
		return nil
	}
	for _, n := range names {
		if err := visit(n); err != nil {
			return nil, err
		}
	}
	return out, nil
}
