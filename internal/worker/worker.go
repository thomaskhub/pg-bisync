// Package worker runs the sync loop of one node: stream the peer's changes and apply them locally,
// reconnecting with backoff after errors.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/apply"
	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/stream"
)

const firstBackoff = time.Second

// Run syncs until ctx is cancelled. Errors never skip data: the failed transaction is retried.
func Run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	reader := stream.Reader{PeerDSN: cfg.PeerDSN, Slot: cfg.SlotName(), Publication: cfg.Publication, StatusInterval: cfg.StatusInterval}
	backoff := firstBackoff
	for {
		err := once(ctx, cfg, reader, log)
		if ctx.Err() != nil {
			return nil
		}
		log.Error("sync stopped, retrying", "err", err, "in", backoff)
		setStatus(cfg, "stalled", err.Error())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, cfg.RetryMax)
		if err == nil {
			backoff = firstBackoff
		}
	}
}

func once(ctx context.Context, cfg config.Config, reader stream.Reader, log *slog.Logger) error {
	if err := reader.EnsureSlot(ctx); err != nil {
		return err
	}
	a, err := apply.New(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer a.Close(context.Background())
	start, err := a.StartLSN(ctx)
	if err != nil {
		return err
	}
	log.Info("streaming", "peer", cfg.Peer, "slot", cfg.SlotName(), "from", start)
	setStatus(cfg, "running", "")
	err = reader.Run(ctx, start, a)
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	return fmt.Errorf("stream from %s: %w", cfg.Peer, err)
}

// setStatus records the worker state on its own short connection (the apply session may be gone).
func setStatus(cfg config.Config, state, msg string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return
	}
	defer conn.Close(ctx)
	_, _ = conn.Exec(ctx, `INSERT INTO bisync.status (node, state, message) VALUES ($1, $2, $3)
		ON CONFLICT (node) DO UPDATE SET state = excluded.state, message = excluded.message, updated = clock_timestamp()`,
		cfg.Node, state, msg)
}
