// Package status reports the sync state of one node (local state, slot lag on both sides, clock
// difference to the peer).
package status

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/config"
)

// Report is the JSON output of "status".
type Report struct {
	Node              string   `json:"node"`
	State             string   `json:"state"`
	Message           string   `json:"message,omitempty"`
	LastApply         *string  `json:"last_apply,omitempty"`
	AppliedTx         int64    `json:"applied_tx"`
	OpenConflicts     int64    `json:"open_conflicts"`
	SlotOnPeerActive  bool     `json:"slot_on_peer_active"`
	SlotOnPeerLag     *int64   `json:"slot_on_peer_lag_bytes"`
	SlotOnPeerWAL     string   `json:"slot_on_peer_wal_status"`
	ClockDiffMillis   *float64 `json:"clock_diff_ms,omitempty"`
	BacklogForPeerLag *int64   `json:"backlog_for_peer_bytes"`
}

// Run collects the status of the node in cfg.
func Run(ctx context.Context, cfg config.Config) (Report, error) {
	r := Report{Node: cfg.Node, State: "unknown"}
	local, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return r, fmt.Errorf("connect local: %w", err)
	}
	defer local.Close(context.Background())
	err = local.QueryRow(ctx, `SELECT state, message, last_apply::text, applied_tx FROM bisync.status WHERE node = $1`, cfg.Node).
		Scan(&r.State, &r.Message, &r.LastApply, &r.AppliedTx)
	if err != nil && err != pgx.ErrNoRows {
		return r, fmt.Errorf("read status: %w", err)
	}
	if err := local.QueryRow(ctx, "SELECT count(*) FROM bisync.conflict WHERE NOT resolved").Scan(&r.OpenConflicts); err != nil {
		return r, fmt.Errorf("read conflicts: %w", err)
	}
	_ = local.QueryRow(ctx, `SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint FROM pg_replication_slots WHERE slot_name = $1`,
		"bisync_"+cfg.Peer).Scan(&r.BacklogForPeerLag)
	peer, err := pgx.Connect(ctx, cfg.PeerDSN)
	if err != nil {
		r.Message = "peer unreachable: " + err.Error()
		return r, nil
	}
	defer peer.Close(context.Background())
	_ = peer.QueryRow(ctx, `SELECT active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, coalesce(wal_status, '')
		FROM pg_replication_slots WHERE slot_name = $1`, cfg.SlotName()).Scan(&r.SlotOnPeerActive, &r.SlotOnPeerLag, &r.SlotOnPeerWAL)
	var peerNow time.Time
	t0 := time.Now()
	if err := peer.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&peerNow); err == nil {
		rtt := time.Since(t0)
		var localNow time.Time
		if err := local.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&localNow); err == nil {
			d := float64(peerNow.Add(rtt/2).Sub(localNow).Microseconds()) / 1000
			r.ClockDiffMillis = &d
		}
	}
	return r, nil
}
