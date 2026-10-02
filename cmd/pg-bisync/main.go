// Command pg-bisync keeps two PostgreSQL databases in sync in both directions.
//
//	pg-bisync <install|uninstall|init|run|status|compare|repair> -config node.json [flags]
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/thomkin/pg-bisync/internal/config"
)

const usage = `usage: pg-bisync <command> -config node.json [flags]

commands:
  install    create schema bisync, triggers and publication in the local database
  uninstall  remove them again (and this node's slot on the peer)
  init       first copy of all synced tables from the peer (local tables must be empty)
  run        stream the peer's changes and apply them locally (until SIGINT/SIGTERM)
  status     sync state as JSON (state, lag, backlog, conflicts, clock difference)
  watch      check the status regularly and alert (Telegram / webhook) until SIGINT/SIGTERM
  compare    compare both databases per table (exit 1 on differences)
  repair     re-send one row from the winning node: -table t -id x -winner node
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	path := fs.String("config", "pg-bisync.json", "config file of this node")
	table := fs.String("table", "", "repair: table")
	id := fs.String("id", "", "repair: primary key value")
	winner := fs.String("winner", "", "repair: node whose version wins")
	_ = fs.Parse(os.Args[2:])

	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	cfg, err := config.Load(*path)
	if err != nil {
		fail(log, err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var out any
	switch cmd {
	case "install":
		out, err = install(ctx, cfg)
	case "uninstall":
		err = uninstall(ctx, cfg)
	case "init":
		out, err = initCopy(ctx, cfg, log)
	case "run":
		err = run(ctx, cfg, log)
	case "status":
		out, err = status(ctx, cfg)
	case "watch":
		err = watchCmd(ctx, cfg, log)
	case "compare":
		var diff bool
		out, diff, err = compareCmd(ctx, cfg)
		if err == nil && diff {
			printJSON(out)
			os.Exit(1)
		}
	case "repair":
		out, err = repair(ctx, cfg, *table, *id, *winner)
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fail(log, err)
	}
	if out != nil {
		printJSON(out)
	}
}

func printJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func fail(log *slog.Logger, err error) {
	log.Error("failed", "err", err)
	if errors.Is(err, context.Canceled) {
		os.Exit(130)
	}
	os.Exit(1)
}
