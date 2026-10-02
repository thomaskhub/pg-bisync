package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"

	"github.com/thomkin/pg-bisync/internal/compare"
	"github.com/thomkin/pg-bisync/internal/config"
	"github.com/thomkin/pg-bisync/internal/initcopy"
	"github.com/thomkin/pg-bisync/internal/schema"
	statuspkg "github.com/thomkin/pg-bisync/internal/status"
	"github.com/thomkin/pg-bisync/internal/stream"
	"github.com/thomkin/pg-bisync/internal/worker"
)

func install(ctx context.Context, cfg config.Config) (any, error) {
	conn, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return nil, fmt.Errorf("connect local: %w", err)
	}
	defer conn.Close(context.Background())
	tables, err := schema.Install(ctx, conn, cfg)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(tables))
	for i, t := range tables {
		names[i] = t.Name
	}
	return map[string]any{"node": cfg.Node, "tables": names}, nil
}

func uninstall(ctx context.Context, cfg config.Config) error {
	conn, err := pgx.Connect(ctx, cfg.LocalDSN)
	if err != nil {
		return fmt.Errorf("connect local: %w", err)
	}
	defer conn.Close(context.Background())
	if err := schema.Uninstall(ctx, conn, cfg); err != nil {
		return err
	}
	repl, err := stream.Connect(ctx, cfg.PeerDSN)
	if err != nil {
		return err
	}
	defer repl.Close(context.Background())
	exists, err := stream.SlotExists(ctx, repl, cfg.SlotName())
	if err != nil || !exists {
		return err
	}
	return pglogrepl.DropReplicationSlot(ctx, repl, cfg.SlotName(), pglogrepl.DropReplicationSlotOptions{})
}

func run(ctx context.Context, cfg config.Config, log *slog.Logger) error {
	return worker.Run(ctx, cfg, log)
}

func initCopy(ctx context.Context, cfg config.Config, log *slog.Logger) (any, error) {
	return initcopy.Run(ctx, cfg, log)
}

func status(ctx context.Context, cfg config.Config) (any, error) {
	return statuspkg.Run(ctx, cfg)
}

func compareCmd(ctx context.Context, cfg config.Config) (any, bool, error) {
	res, err := compare.Run(ctx, cfg)
	return res, len(res) > 0, err
}

func repair(ctx context.Context, cfg config.Config, table, id, winner string) (any, error) {
	if table == "" || id == "" || winner == "" {
		return nil, errors.New("repair needs -table, -id and -winner")
	}
	msg, err := compare.Repair(ctx, cfg, table, id, winner)
	return map[string]string{"result": msg}, err
}
