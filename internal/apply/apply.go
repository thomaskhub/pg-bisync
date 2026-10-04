// Package apply writes the peer's transactions into the local database with the pg-bisync rules:
// last change wins on the updated column (identical timestamps: tie-break node), deletes win
// (tombstones), foreign keys and cascades stay active. Every applied transaction carries the local
// replication origin, which (a) keeps the peer's stream from sending it back and (b) stores the
// applied position atomically with the data.
package apply

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/thomaskhub/pg-bisync/internal/config"
	"github.com/thomaskhub/pg-bisync/internal/stream"
)

// Conflict kinds recorded in bisync.conflict. Recorded conflicts do not stop the sync.
const (
	ConflictParentDeleted = "parent_deleted" // child refers to a row deleted on this node: child dropped
	ConflictDeleteBlocked = "delete_blocked" // delete blocked by a local child (RESTRICT): needs repair
	ConflictUniqueLost    = "unique_lost"    // lost a unique-key merge: row dropped on both nodes
)

const (
	sqlStateForeignKey = "23503"
	sqlStateUnique     = "23505"
	lockWait           = 2 * time.Second
)

// ErrStalled wraps errors that stop applying (nothing is skipped; the transaction is retried).
var ErrStalled = errors.New("apply stalled")

// Applier applies peer transactions on one dedicated local connection.
type Applier struct {
	cfg  config.Config
	conn *pgx.Conn
	peer *pgx.Conn // only for unchanged TOAST values of rows missing locally
	cat  *catalog
	tx   pgx.Tx
	log  *slog.Logger

	Applied   int64 // applied transactions since start
	Conflicts int64 // recorded conflicts since start
}

// New connects to the local database, takes the per-node advisory lock (one active worker) and
// sets up the replication origin session.
func New(ctx context.Context, cfg config.Config, log *slog.Logger) (*Applier, error) {
	conn, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return nil, fmt.Errorf("connect local: %w", err)
	}
	a := &Applier{cfg: cfg, conn: conn, log: log}
	if err := a.setup(ctx); err != nil {
		_ = conn.Close(context.Background())
		return nil, err
	}
	return a, nil
}

func (a *Applier) setup(ctx context.Context) error {
	for {
		var ok bool
		if err := a.conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('bisync:' || $1))", a.cfg.OriginName()).Scan(&ok); err != nil {
			return fmt.Errorf("advisory lock: %w", err)
		}
		if ok {
			break
		}
		a.log.Info("another worker holds the lock, waiting", "origin", a.cfg.OriginName())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(lockWait):
		}
	}
	// synchronous_commit off is safe here (like Postgres' own apply worker): the origin position is
	// committed with the data, so after a crash the lost transactions are simply streamed again
	if _, err := a.conn.Exec(ctx, "SET synchronous_commit = off"); err != nil {
		return fmt.Errorf("set synchronous_commit: %w", err)
	}
	stmts := []string{
		"SELECT pg_replication_origin_create($1) WHERE NOT EXISTS (SELECT 1 FROM pg_replication_origin WHERE roname = $1)",
		"SELECT pg_replication_origin_session_setup($1)",
	}
	for _, s := range stmts {
		if _, err := a.conn.Exec(ctx, s, a.cfg.OriginName()); err != nil {
			return fmt.Errorf("replication origin %s: %w", a.cfg.OriginName(), err)
		}
	}
	return a.reloadCatalog(ctx)
}

func (a *Applier) reloadCatalog(ctx context.Context) error {
	cat, err := loadCatalog(ctx, a.conn, a.cfg.Schema)
	if err != nil {
		return err
	}
	a.cat = cat
	return nil
}

// Close ends the session (releases the advisory lock and the origin session).
func (a *Applier) Close(ctx context.Context) {
	a.Abort(ctx)
	_ = a.conn.Close(ctx)
	if a.peer != nil {
		_ = a.peer.Close(ctx)
	}
}

// StartLSN is the position after the last applied peer transaction (0 if none yet).
func (a *Applier) StartLSN(ctx context.Context) (pglogrepl.LSN, error) {
	var s *string
	if err := a.conn.QueryRow(ctx, "SELECT pg_replication_origin_progress($1, true)::text", a.cfg.OriginName()).Scan(&s); err != nil {
		return 0, fmt.Errorf("origin progress: %w", err)
	}
	if s == nil {
		return 0, nil
	}
	return pglogrepl.ParseLSN(*s)
}

// Abort rolls back an open transaction (after a stream error).
func (a *Applier) Abort(ctx context.Context) {
	if a.tx != nil {
		_ = a.tx.Rollback(ctx)
		a.tx = nil
	}
}

// Begin starts the local transaction for one peer transaction.
func (a *Applier) Begin(ctx context.Context, _ *pglogrepl.BeginMessage) error {
	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%w: begin: %w", ErrStalled, err)
	}
	a.tx = tx
	return nil
}

// Commit records the peer position in the origin and commits.
func (a *Applier) Commit(ctx context.Context, m *pglogrepl.CommitMessage) (pglogrepl.LSN, error) {
	if a.tx == nil {
		return 0, fmt.Errorf("%w: commit without begin", ErrStalled)
	}
	b := &pgx.Batch{}
	b.Queue("SELECT pg_replication_origin_xact_setup($1::text::pg_lsn, $2)", m.TransactionEndLSN.String(), m.CommitTime)
	b.Queue(`INSERT INTO bisync.status (node, state, message, last_apply, last_lsn, applied_tx, updated)
		VALUES ($1, 'running', '', clock_timestamp(), $2::text::pg_lsn, 1, clock_timestamp())
		ON CONFLICT (node) DO UPDATE SET state = 'running', message = '', last_apply = excluded.last_apply,
		  last_lsn = excluded.last_lsn, applied_tx = bisync.status.applied_tx + 1, updated = excluded.updated`,
		a.cfg.Node, m.TransactionEndLSN.String())
	if err := a.tx.SendBatch(ctx, b).Close(); err != nil {
		a.Abort(ctx)
		return 0, fmt.Errorf("%w: commit bookkeeping: %w", ErrStalled, err)
	}
	if err := a.tx.Commit(ctx); err != nil {
		a.tx = nil
		return 0, fmt.Errorf("%w: commit: %w", ErrStalled, err)
	}
	a.tx = nil
	a.Applied++
	return m.TransactionEndLSN, nil
}

// Change applies one row change inside the current transaction.
func (a *Applier) Change(ctx context.Context, c stream.Change) error {
	if a.tx == nil {
		return fmt.Errorf("%w: change outside a transaction", ErrStalled)
	}
	if c.Rel.Schema != a.cfg.Schema {
		return fmt.Errorf("%w: change for schema %s, configured %s", ErrStalled, c.Rel.Schema, a.cfg.Schema)
	}
	pk, err := c.KeyText()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrStalled, err)
	}
	if err := a.checkColumns(ctx, c); err != nil {
		return err
	}
	if c.Op == stream.Delete {
		return a.applyDelete(ctx, c, pk)
	}
	return a.applyUpsert(ctx, c, pk, true)
}

// checkColumns makes sure every column of the change exists locally (reloads the catalog once).
func (a *Applier) checkColumns(ctx context.Context, c stream.Change) error {
	missing := func() string {
		cols := a.cat.types[c.Rel.Name]
		if cols == nil {
			return "table " + c.Rel.Name
		}
		for _, col := range c.Rel.Columns {
			if _, ok := cols[col.Name]; !ok {
				return "column " + c.Rel.Name + "." + col.Name
			}
		}
		return ""
	}
	if missing() == "" {
		return nil
	}
	if err := a.reloadCatalog(ctx); err != nil {
		return fmt.Errorf("%w: %w", ErrStalled, err)
	}
	if m := missing(); m != "" {
		return fmt.Errorf("%w: schema mismatch, %s missing locally (run the migration on both nodes)", ErrStalled, m)
	}
	return nil
}

func (a *Applier) applyDelete(ctx context.Context, c stream.Change, pk string) error {
	t := a.tableIdent(c.Rel.Name)
	pkCol := a.cat.pk[c.Rel.Name]
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s = $1::text::%s", t, quoteIdent(pkCol), a.cat.types[c.Rel.Name][pkCol])
	err := a.inSavepoint(ctx, func(b *pgx.Batch) {
		b.Queue(stmt, pk)
		b.Queue(tombstoneSQL, c.Rel.Name, pk)
	})
	var pe *pgconn.PgError
	if errors.As(err, &pe) && pe.Code == sqlStateForeignKey {
		return a.conflict(ctx, ConflictDeleteBlocked, c.Rel.Name, pk, pe.Message)
	}
	if err != nil {
		return fmt.Errorf("%w: delete %s %s: %w", ErrStalled, c.Rel.Name, pk, err)
	}
	return nil
}

const tombstoneSQL = `INSERT INTO bisync.tombstone (tbl, pk) VALUES ($1, $2)
	ON CONFLICT (tbl, pk) DO UPDATE SET deleted_at = greatest(bisync.tombstone.deleted_at, excluded.deleted_at)`

// applyUpsert inserts or updates the row unless a tombstone exists; on an existing row only a
// later change (or the tie-break peer on an identical timestamp) overwrites it.
func (a *Applier) applyUpsert(ctx context.Context, c stream.Change, pk string, retryUnique bool) error {
	vals, err := a.fillUnchanged(ctx, c, pk)
	if err != nil {
		return err
	}
	if vals == nil {
		return nil // row gone on the peer as well; its delete follows in the stream
	}
	return a.applyValues(ctx, c, vals, pk, retryUnique)
}

func (a *Applier) applyValues(ctx context.Context, c stream.Change, vals []stream.Value, pk string, retryUnique bool) error {
	stmt, args := a.upsertSQL(c, vals, pk)
	// The tombstone check inside the upsert uses the statement's snapshot. If a local delete of the
	// row commits while the upsert waits for the row lock, the upsert re-inserts the row. A second
	// statement (new snapshot) sees that tombstone and removes the row again: delete wins.
	pkCol := a.cat.pk[c.Rel.Name]
	recheck := fmt.Sprintf(`DELETE FROM %s WHERE %s = $1::text::%s
		AND EXISTS (SELECT 1 FROM bisync.tombstone WHERE tbl = $2 AND pk = $3)`,
		a.tableIdent(c.Rel.Name), quoteIdent(pkCol), a.cat.types[c.Rel.Name][pkCol])
	err := a.inSavepoint(ctx, func(b *pgx.Batch) {
		b.Queue(stmt, args...)
		b.Queue(recheck, pk, c.Rel.Name, pk)
	})
	if err == nil {
		return nil
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return fmt.Errorf("%w: upsert %s %s: %w", ErrStalled, c.Rel.Name, pk, err)
	}
	switch pe.Code {
	case sqlStateForeignKey:
		return a.foreignKeyConflict(ctx, c, vals, pk, pe)
	case sqlStateUnique:
		if retryUnique {
			return a.uniqueConflict(ctx, c, vals, pk, pe)
		}
	}
	return fmt.Errorf("%w: upsert %s %s: %w", ErrStalled, c.Rel.Name, pk, err)
}

func (a *Applier) upsertSQL(c stream.Change, vals []stream.Value, pk string) (string, []any) {
	types := a.cat.types[c.Rel.Name]
	pkCol := a.cat.pk[c.Rel.Name]
	var cols, params, sets []string
	var args []any
	for i, col := range c.Rel.Columns {
		v := vals[i]
		if v.Kind == stream.ValueUnchanged {
			continue
		}
		args = append(args, textArg(v))
		cols = append(cols, quoteIdent(col.Name))
		params = append(params, fmt.Sprintf("$%d::text::%s", len(args), types[col.Name]))
		if col.Name != pkCol {
			sets = append(sets, fmt.Sprintf("%s = excluded.%s", quoteIdent(col.Name), quoteIdent(col.Name)))
		}
	}
	args = append(args, c.Rel.Name, pk)
	nTbl, nPK := len(args)-1, len(args)
	cmp := "<"
	if a.cfg.PeerWinsTie() {
		cmp = "<="
	}
	upd := quoteIdent(a.cfg.UpdatedColumn)
	t := a.tableIdent(c.Rel.Name)
	stmt := fmt.Sprintf(`INSERT INTO %s AS t (%s) SELECT %s
		WHERE NOT EXISTS (SELECT 1 FROM bisync.tombstone WHERE tbl = $%d AND pk = $%d)
		ON CONFLICT (%s) DO UPDATE SET %s WHERE t.%s %s excluded.%s`,
		t, strings.Join(cols, ", "), strings.Join(params, ", "), nTbl, nPK,
		quoteIdent(pkCol), strings.Join(sets, ", "), upd, cmp, upd)
	if len(sets) == 0 {
		stmt = fmt.Sprintf(`INSERT INTO %s AS t (%s) SELECT %s
			WHERE NOT EXISTS (SELECT 1 FROM bisync.tombstone WHERE tbl = $%d AND pk = $%d)
			ON CONFLICT (%s) DO NOTHING`, t, strings.Join(cols, ", "), strings.Join(params, ", "), nTbl, nPK, quoteIdent(pkCol))
	}
	return stmt, args
}

// fillUnchanged returns the values with unchanged TOAST columns. If the row exists locally they
// stay "unchanged" (not written); if not, they are read from the peer. nil = row gone on the peer.
func (a *Applier) fillUnchanged(ctx context.Context, c stream.Change, pk string) ([]stream.Value, error) {
	vals := c.New
	var unchanged []int
	for i, v := range vals {
		if v.Kind == stream.ValueUnchanged {
			unchanged = append(unchanged, i)
		}
	}
	if len(unchanged) == 0 {
		return vals, nil
	}
	pkCol := a.cat.pk[c.Rel.Name]
	pkType := a.cat.types[c.Rel.Name][pkCol]
	var exists bool
	q := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s WHERE %s = $1::text::%s)", a.tableIdent(c.Rel.Name), quoteIdent(pkCol), pkType)
	if err := a.tx.QueryRow(ctx, q, pk).Scan(&exists); err != nil {
		return nil, fmt.Errorf("%w: check row %s %s: %w", ErrStalled, c.Rel.Name, pk, err)
	}
	if exists {
		return vals, nil
	}
	if err := a.connectPeer(ctx); err != nil {
		return nil, err
	}
	sel := make([]string, len(unchanged))
	for i, idx := range unchanged {
		sel[i] = quoteIdent(c.Rel.Columns[idx].Name) + "::text"
	}
	q = fmt.Sprintf("SELECT %s FROM %s WHERE %s = $1::text::%s", strings.Join(sel, ", "), a.tableIdent(c.Rel.Name), quoteIdent(pkCol), pkType)
	got := make([]*string, len(unchanged))
	dest := make([]any, len(unchanged))
	for i := range got {
		dest[i] = &got[i]
	}
	err := a.peer.QueryRow(ctx, q, pk).Scan(dest...)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: fetch unchanged values of %s %s from peer: %w", ErrStalled, c.Rel.Name, pk, err)
	}
	out := append([]stream.Value(nil), vals...)
	for i, idx := range unchanged {
		out[idx] = stream.Value{Kind: stream.ValueNull}
		if got[i] != nil {
			out[idx] = stream.Value{Kind: stream.ValueText, Text: *got[i]}
		}
	}
	return out, nil
}

func (a *Applier) connectPeer(ctx context.Context) error {
	if a.peer != nil && !a.peer.IsClosed() {
		return nil
	}
	p, err := pgx.Connect(ctx, a.cfg.PeerDSN)
	if err != nil {
		return fmt.Errorf("%w: connect peer: %w", ErrStalled, err)
	}
	a.peer = p
	return nil
}

// foreignKeyConflict: a child whose parent was merged into another row is redirected to the
// winner; a child whose parent was deleted on this node is dropped (delete wins); any other
// missing parent stops the sync.
func (a *Applier) foreignKeyConflict(ctx context.Context, c stream.Change, vals []stream.Value, pk string, pe *pgconn.PgError) error {
	fk, ok := a.cat.fks[pe.ConstraintName]
	if !ok {
		return fmt.Errorf("%w: foreign key %s of %s %s: %s", ErrStalled, pe.ConstraintName, c.Rel.Name, pk, pe.Message)
	}
	ref := valueOf(c.Rel, vals, fk.Column)
	var replacedBy *string
	err := a.tx.QueryRow(ctx, "SELECT replaced_by FROM bisync.tombstone WHERE tbl = $1 AND pk = $2", fk.RefTable, ref).Scan(&replacedBy)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: %s %s refers to %s %s which does not exist: %s", ErrStalled, c.Rel.Name, pk, fk.RefTable, ref, pe.Message)
	}
	if err != nil {
		return fmt.Errorf("%w: tombstone lookup: %w", ErrStalled, err)
	}
	if replacedBy != nil {
		redirected := withValue(c.Rel, vals, fk.Column, *replacedBy)
		return a.applyValues(ctx, c, redirected, pk, false)
	}
	if err := a.exec(ctx, tombstoneSQL, c.Rel.Name, pk); err != nil {
		return err
	}
	return a.conflict(ctx, ConflictParentDeleted, c.Rel.Name, pk, fmt.Sprintf("parent %s %s deleted", fk.RefTable, ref))
}

// uniqueConflict applies the table's unique-key policy for a constraint other than the PK.
// merge: the row with the later change wins; the loser is replaced by the winner on both nodes
// (references are redirected, the loser gets a tombstone with replaced_by).
func (a *Applier) uniqueConflict(ctx context.Context, c stream.Change, vals []stream.Value, pk string, pe *pgconn.PgError) error {
	uk, ok := a.cat.uniques[pe.ConstraintName]
	if !ok || a.cfg.UniquePolicyFor(c.Rel.Name) != config.UniqueMerge {
		return fmt.Errorf("%w: unique %s on %s %s (policy %s): %s", ErrStalled, pe.ConstraintName, c.Rel.Name, pk, a.cfg.UniquePolicyFor(c.Rel.Name), pe.Message)
	}
	pkCol := a.cat.pk[c.Rel.Name]
	types := a.cat.types[c.Rel.Name]
	conds := make([]string, len(uk.Columns))
	args := make([]any, 0, len(uk.Columns)+1)
	for i, col := range uk.Columns {
		args = append(args, valueOf(c.Rel, vals, col))
		conds[i] = fmt.Sprintf("%s = $%d::text::%s", quoteIdent(col), i+1, types[col])
	}
	upd := quoteIdent(a.cfg.UpdatedColumn)
	args = append(args, valueOf(c.Rel, vals, a.cfg.UpdatedColumn))
	// sign of (incoming - local) computed by Postgres: no timestamp parsing in Go
	var localPK string
	var cmp int
	q := fmt.Sprintf(`SELECT %s::text, CASE WHEN %s < $%d::text::timestamptz THEN 1 WHEN %s = $%d::text::timestamptz THEN 0 ELSE -1 END
		FROM %s WHERE %s`, quoteIdent(pkCol), upd, len(args), upd, len(args), a.tableIdent(c.Rel.Name), strings.Join(conds, " AND "))
	if err := a.tx.QueryRow(ctx, q, args...).Scan(&localPK, &cmp); err != nil {
		return fmt.Errorf("%w: unique merge lookup %s: %w", ErrStalled, c.Rel.Name, err)
	}
	winner, loser := localPK, pk
	if cmp > 0 || (cmp == 0 && a.cfg.PeerWinsTie()) {
		winner, loser = pk, localPK
		if err := a.replaceLocal(ctx, c.Rel.Name, localPK, pk); err != nil {
			return err
		}
		if err := a.applyValues(ctx, c, vals, pk, false); err != nil {
			return err
		}
	}
	if err := a.exec(ctx, tombstoneReplacedSQL, c.Rel.Name, loser, winner); err != nil {
		return err
	}
	return a.conflict(ctx, ConflictUniqueLost, c.Rel.Name, loser, fmt.Sprintf("%s: merged into %s", pe.ConstraintName, winner))
}

const tombstoneReplacedSQL = `INSERT INTO bisync.tombstone (tbl, pk, replaced_by) VALUES ($1, $2, $3)
	ON CONFLICT (tbl, pk) DO UPDATE SET replaced_by = excluded.replaced_by, deleted_at = excluded.deleted_at`

// replaceLocal moves every reference from the local loser row to the (not yet inserted) winner and
// deletes the loser. The referencing foreign keys must be DEFERRABLE: they are checked at commit,
// when the winner exists.
func (a *Applier) replaceLocal(ctx context.Context, table, loserPK, winnerPK string) error {
	pkCol := a.cat.pk[table]
	pkType := a.cat.types[table][pkCol]
	for _, fk := range a.cat.fks {
		if fk.RefTable != table || fk.RefColumn != pkCol {
			continue
		}
		if !fk.Deferrable {
			return fmt.Errorf("%w: unique merge on %s needs foreign key %s to be DEFERRABLE", ErrStalled, table, fk.Name)
		}
		stmts := []string{
			fmt.Sprintf("SET CONSTRAINTS %s DEFERRED", pgx.Identifier{a.cfg.Schema, fk.Name}.Sanitize()),
			fmt.Sprintf("UPDATE %s SET %s = $1::text::%s WHERE %s = $2::text::%s",
				a.tableIdent(fk.Table), quoteIdent(fk.Column), pkType, quoteIdent(fk.Column), pkType),
		}
		if err := a.exec(ctx, stmts[0]); err != nil {
			return err
		}
		if err := a.exec(ctx, stmts[1], winnerPK, loserPK); err != nil {
			return err
		}
	}
	del := fmt.Sprintf("DELETE FROM %s WHERE %s = $1::text::%s", a.tableIdent(table), quoteIdent(pkCol), pkType)
	return a.exec(ctx, del, loserPK)
}

func (a *Applier) conflict(ctx context.Context, kind, table, pk, detail string) error {
	a.Conflicts++
	a.log.Warn("conflict", "kind", kind, "table", table, "pk", pk, "detail", detail)
	return a.exec(ctx, "INSERT INTO bisync.conflict (kind, tbl, pk, detail) VALUES ($1, $2, $3, $4)", kind, table, pk, detail)
}

func (a *Applier) exec(ctx context.Context, sql string, args ...any) error {
	if _, err := a.tx.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("%w: %w", ErrStalled, err)
	}
	return nil
}

// inSavepoint runs the queued statements in one round trip; on error the savepoint is rolled
// back so the transaction stays usable, and the first statement error is returned.
func (a *Applier) inSavepoint(ctx context.Context, queue func(b *pgx.Batch)) error {
	b := &pgx.Batch{}
	b.Queue("SAVEPOINT bisync_change")
	queue(b)
	b.Queue("RELEASE SAVEPOINT bisync_change")
	br := a.tx.SendBatch(ctx, b)
	var first error
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil && first == nil {
			first = err
		}
	}
	if err := br.Close(); err != nil && first == nil {
		first = err
	}
	if first == nil {
		return nil
	}
	if _, err := a.tx.Exec(ctx, "ROLLBACK TO SAVEPOINT bisync_change"); err != nil {
		return fmt.Errorf("%w: rollback to savepoint after %w: %w", ErrStalled, first, err)
	}
	return first
}

func (a *Applier) tableIdent(name string) string {
	return pgx.Identifier{a.cfg.Schema, name}.Sanitize()
}

func quoteIdent(s string) string { return pgx.Identifier{s}.Sanitize() }

func textArg(v stream.Value) any {
	if v.Kind == stream.ValueNull {
		return nil
	}
	return v.Text
}

// withValue returns a copy of vals with col set to text v.
func withValue(rel *stream.Relation, vals []stream.Value, col, v string) []stream.Value {
	out := append([]stream.Value(nil), vals...)
	for i, c := range rel.Columns {
		if c.Name == col {
			out[i] = stream.Value{Kind: stream.ValueText, Text: v}
		}
	}
	return out
}

func valueOf(rel *stream.Relation, vals []stream.Value, col string) string {
	for i, c := range rel.Columns {
		if c.Name == col && i < len(vals) {
			return vals[i].Text
		}
	}
	return ""
}
