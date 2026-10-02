// Package stream reads the committed transactions of the peer from a logical replication slot
// (core plugin pgoutput) and hands them, change by change, to a Handler.
package stream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Op is the kind of a row change.
type Op byte

// Row change kinds.
const (
	Insert Op = 'I'
	Update Op = 'U'
	Delete Op = 'D'
)

// Value kinds of a column value in a change.
const (
	ValueNull      = 'n' // SQL NULL
	ValueUnchanged = 'u' // unchanged TOAST value: not sent, keep the local value
	ValueText      = 't' // text representation in Text
)

// Value is one column value of a change.
type Value struct {
	Kind byte
	Text string
}

// Column of a relation.
type Column struct {
	Name    string
	TypeOID uint32
	Key     bool
}

// Relation describes a published table as sent by the peer.
type Relation struct {
	ID      uint32
	Schema  string
	Name    string
	Columns []Column
}

// KeyIndex returns the index of the (single) key column, or -1.
func (r *Relation) KeyIndex() int {
	for i, c := range r.Columns {
		if c.Key {
			return i
		}
	}
	return -1
}

// Change is one row change. Key holds the old key (deletes, key updates), New the new row.
type Change struct {
	Op  Op
	Rel *Relation
	Key []Value
	New []Value
}

// KeyText returns the primary key value of the change as text.
func (c Change) KeyText() (string, error) {
	i := c.Rel.KeyIndex()
	if i < 0 {
		return "", fmt.Errorf("table %s.%s: no key column in replica identity", c.Rel.Schema, c.Rel.Name)
	}
	vals := c.New
	if c.Op == Delete || (c.Key != nil && len(c.Key) > i && c.Key[i].Kind == ValueText) {
		vals = c.Key
	}
	if len(vals) <= i || vals[i].Kind != ValueText {
		return "", fmt.Errorf("table %s.%s: key value missing", c.Rel.Schema, c.Rel.Name)
	}
	return vals[i].Text, nil
}

// Handler applies one transaction at a time. Commit returns the LSN that is durably applied.
type Handler interface {
	Begin(ctx context.Context, m *pglogrepl.BeginMessage) error
	Change(ctx context.Context, c Change) error
	Commit(ctx context.Context, m *pglogrepl.CommitMessage) (pglogrepl.LSN, error)
}

// Reader streams from one slot on the peer.
type Reader struct {
	PeerDSN        string
	Slot           string
	Publication    string
	StatusInterval time.Duration
}

// EnsureSlot creates the slot on the peer if it does not exist.
func (r Reader) EnsureSlot(ctx context.Context) error {
	conn, err := Connect(ctx, r.PeerDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	exists, err := SlotExists(ctx, conn, r.Slot)
	if err != nil || exists {
		return err
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, r.Slot, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication, SnapshotAction: "NOEXPORT_SNAPSHOT"})
	if err != nil {
		return fmt.Errorf("create slot %s: %w", r.Slot, err)
	}
	return nil
}

// Connect opens a replication connection (replication=database) to dsn.
func Connect(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse peer dsn: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect peer (replication): %w", err)
	}
	return conn, nil
}

// SlotExists reports whether the slot exists on the server of conn.
func SlotExists(ctx context.Context, conn *pgconn.PgConn, slot string) (bool, error) {
	res, err := conn.Exec(ctx, fmt.Sprintf("SELECT 1 FROM pg_replication_slots WHERE slot_name = '%s'", slot)).ReadAll()
	if err != nil {
		return false, fmt.Errorf("check slot %s: %w", slot, err)
	}
	return len(res) > 0 && len(res[0].Rows) > 0, nil
}

// Run streams from start (0 = where the slot is) until ctx ends or an error occurs.
func (r Reader) Run(ctx context.Context, start pglogrepl.LSN, h Handler) error {
	conn, err := Connect(ctx, r.PeerDSN)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	args := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", r.Publication), "origin 'none'"}
	if err := pglogrepl.StartReplication(ctx, conn, r.Slot, start, pglogrepl.StartReplicationOptions{PluginArgs: args}); err != nil {
		return fmt.Errorf("start replication on %s: %w", r.Slot, err)
	}
	s := session{conn: conn, h: h, confirmed: start, relations: map[uint32]*Relation{}, interval: r.StatusInterval}
	return s.loop(ctx)
}

type session struct {
	conn      *pgconn.PgConn
	h         Handler
	confirmed pglogrepl.LSN // last LSN the peer may release
	inTx      bool
	relations map[uint32]*Relation
	interval  time.Duration
}

func (s *session) loop(ctx context.Context) error {
	next := time.Now().Add(s.interval)
	for {
		if time.Now().After(next) {
			if err := s.sendStatus(ctx); err != nil {
				return err
			}
			next = time.Now().Add(s.interval)
		}
		rctx, cancel := context.WithDeadline(ctx, next)
		raw, err := s.conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("peer: %s", pgconn.ErrorResponseToPgError(msg))
		case *pgproto3.CopyData:
			if err := s.copyData(ctx, msg.Data); err != nil {
				return err
			}
		}
	}
}

func (s *session) copyData(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("keepalive: %w", err)
		}
		// between transactions everything received is applied: the peer may release its WAL
		if !s.inTx && ka.ServerWALEnd > s.confirmed {
			s.confirmed = ka.ServerWALEnd
		}
		if ka.ReplyRequested {
			return s.sendStatus(ctx)
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return fmt.Errorf("xlogdata: %w", err)
		}
		return s.logical(ctx, xld.WALData)
	}
	return nil
}

func (s *session) logical(ctx context.Context, data []byte) error {
	m, err := pglogrepl.Parse(data)
	if err != nil {
		return fmt.Errorf("decode pgoutput: %w", err)
	}
	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[m.RelationID] = toRelation(m)
	case *pglogrepl.BeginMessage:
		s.inTx = true
		return s.h.Begin(ctx, m)
	case *pglogrepl.CommitMessage:
		lsn, err := s.h.Commit(ctx, m)
		if err != nil {
			return err
		}
		s.inTx = false
		if lsn > s.confirmed {
			s.confirmed = lsn
		}
	case *pglogrepl.InsertMessage:
		return s.change(ctx, Insert, m.RelationID, nil, m.Tuple)
	case *pglogrepl.UpdateMessage:
		return s.change(ctx, Update, m.RelationID, m.OldTuple, m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return s.change(ctx, Delete, m.RelationID, m.OldTuple, nil)
	case *pglogrepl.TruncateMessage:
		return errors.New("TRUNCATE on a synced table is not supported")
	}
	return nil
}

func (s *session) change(ctx context.Context, op Op, relID uint32, oldT, newT *pglogrepl.TupleData) error {
	rel, ok := s.relations[relID]
	if !ok {
		return fmt.Errorf("change for unknown relation %d", relID)
	}
	return s.h.Change(ctx, Change{Op: op, Rel: rel, Key: toValues(oldT), New: toValues(newT)})
}

func (s *session) sendStatus(ctx context.Context) error {
	err := pglogrepl.SendStandbyStatusUpdate(ctx, s.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.confirmed})
	if err != nil {
		return fmt.Errorf("status update: %w", err)
	}
	return nil
}

func toRelation(m *pglogrepl.RelationMessage) *Relation {
	r := &Relation{ID: m.RelationID, Schema: m.Namespace, Name: m.RelationName, Columns: make([]Column, len(m.Columns))}
	for i, c := range m.Columns {
		r.Columns[i] = Column{Name: c.Name, TypeOID: c.DataType, Key: c.Flags&1 == 1}
	}
	return r
}

func toValues(t *pglogrepl.TupleData) []Value {
	if t == nil {
		return nil
	}
	out := make([]Value, len(t.Columns))
	for i, c := range t.Columns {
		out[i] = Value{Kind: c.DataType, Text: string(c.Data)}
	}
	return out
}
