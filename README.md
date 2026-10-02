# pg-bisync

Two-way (active-active) sync between two PostgreSQL databases on **plain, unpatched PostgreSQL**.
Each node keeps working on its own database; pg-bisync copies every committed change to the other
node within milliseconds and resolves conflicts by fixed rules. One small Go binary per node,
~17 MB RAM, no extension, no proxy in the request path.

Status: feature-complete for two nodes, tested (see [Testing](#testing)); not yet used in production.

## How it works

```
app (node in) ─ read/write ─→ PostgreSQL in ──WAL stream──→ pg-bisync (on eu) ─ apply ─→ PostgreSQL eu
app (node eu) ─ read/write ─→ PostgreSQL eu ──WAL stream──→ pg-bisync (on in) ─ apply ─→ PostgreSQL in
```

- Changes are read from the peer's write-ahead log through a logical replication slot (core plugin
  `pgoutput`): no polling, no outbox table, nothing extra written by the application.
- Each peer transaction is applied **as one local transaction**, in commit order.
- Applied changes carry a replication origin: they are not sent back (no echo), and the applied
  position is stored **atomically with the data** — after a crash the worker continues exactly where
  it stopped, nothing lost, nothing applied twice.
- Two tiny triggers per table: `updated_at` stamp on UPDATE and a tombstone on DELETE.

## Rules

| Situation                                              | Result                                                                                                                                                        |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Same row changed on both nodes                         | the later `updated_at` wins (whole row); identical timestamps → `tie_break_node` wins                                                                         |
| Row deleted on one node, changed on the other          | **delete wins** (tombstone), regardless of time                                                                                                               |
| Child arrives for a parent deleted on this node        | child dropped, recorded as conflict `parent_deleted`                                                                                                          |
| Delete blocked by a local child (`ON DELETE RESTRICT`) | recorded as `delete_blocked`, needs `repair` (use soft delete for referenced rows)                                                                            |
| Same unique value (not the PK) created on both nodes   | `unique_policy`: `stop` (default) or `merge`: the later row wins, references to the loser are redirected to the winner (referencing FKs must be `DEFERRABLE`) |
| Anything else (missing column, check violation, …)     | the worker **stops applying and retries**; nothing is skipped; `status` shows `stalled`                                                                       |

## Requirements

- PostgreSQL ≥ 17 on both nodes, `wal_level = logical` (restart), `max_replication_slots` and
  `max_wal_senders` ≥ 2, and `max_slot_wal_keep_size` set (limits WAL kept for a stopped node).
- Every synced table: single-column primary key (UUID or text ids; no sequence ids shared across
  nodes), `updated_at timestamptz` (default `clock_timestamp()`), no primary-key updates, no TRUNCATE.
- Clocks synced (chrony/NTP); `status` shows the difference to the peer.
- A role on each node with `REPLICATION`, rights on the synced tables and schema `bisync`, and:
  ```sql
  GRANT EXECUTE ON FUNCTION pg_replication_origin_create(text), pg_replication_origin_session_setup(text),
    pg_replication_origin_session_reset(), pg_replication_origin_xact_setup(pg_lsn, timestamptz),
    pg_replication_origin_progress(text, boolean), pg_replication_origin_drop(text) TO bisync;
  ```
  plus a `replication` line in `pg_hba.conf` for the peer.
- `install` runs as the owner of the tables (or a superuser). Application roles need no extra rights: the two trigger functions are `SECURITY DEFINER` with a fixed `search_path`.

## Setup

```sh
go build -o pg-bisync ./cmd/pg-bisync                 # or: go install github.com/thomkin/pg-bisync/cmd/pg-bisync@latest
pg-bisync install -config in.json                     # on both nodes: schema bisync, triggers, publication
pg-bisync install -config eu.json
pg-bisync init -config eu.json                        # empty node only: copy everything from the peer
pg-bisync run -config in.json                         # on both nodes (systemd: examples/pg-bisync.service)
pg-bisync run -config eu.json
```

`init` copies inside the snapshot of the new slot, so writes on the peer during the copy are not
lost. Config: [`examples/node-in.json`](examples/node-in.json); `PGBISYNC_LOCAL_DSN` /
`PGBISYNC_PEER_DSN` override the DSNs (keep passwords out of the file).

## Operations

| Task                                                     | How                                                                                                                                                                                                                                     |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Health                                                   | `pg-bisync status`: state (`running`/`stalled` + message), last apply, open conflicts, slot lag on both sides, WAL status, clock difference. Alert on `stalled`, growing lag, `wal_status = lost`, conflicts, clock difference > 200 ms |
| Find differences                                         | `pg-bisync compare` (exit 1 on differences, lists the rows) — run it regularly                                                                                                                                                          |
| Fix a row                                                | `pg-bisync repair -table t -id x -winner in`                                                                                                                                                                                            |
| Schema migration                                         | maintenance window: stop the apps, wait until `status` shows no lag, run the migration on **both** nodes, start again. A change on one node only stalls the other worker until the migration is there too (nothing lost)                |
| Node offline longer than `max_slot_wal_keep_size` allows | its slot is `lost`: stop it, empty its synced tables (`TRUNCATE … , bisync.tombstone`), `init`, `run`. Its own unsynced changes are lost                                                                                                |
| Restore a node from backup                               | treat as rebuild (`init`): the peer may have released WAL the restored node still needs                                                                                                                                                 |
| Second worker by mistake                                 | harmless: an advisory lock lets only one apply; the other waits and takes over                                                                                                                                                          |

## Alerts

`pg-bisync watch -config node.json` checks this node every 30 s and sends one message when a
problem starts and one when it is resolved:

| Alert                | When                                                               |
| -------------------- | ------------------------------------------------------------------ |
| sync stalled         | `stalled` for longer than the grace period (default 2 min)         |
| worker not connected | slot on the peer inactive for longer than the grace period         |
| peer unreachable     | for longer than the grace period                                   |
| slot lost            | the peer released WAL this node still needs: rebuild (immediately) |
| lag                  | slot lag over `lag_mb` (default 64 MB) on either side              |
| clock                | clock difference to the peer over `clock_ms` (default 200 ms)      |
| conflicts            | new rows in `bisync.conflict`                                      |

Channels: Telegram (`alerts.telegram_chat_id` + `PGBISYNC_TELEGRAM_TOKEN`) and a generic webhook
(`PGBISYNC_WEBHOOK_URL`, posts `{"text": "…"}`). A new channel is one more implementation of
`notify.Notifier`. Run `watch` next to `run` (second systemd unit).

```json
"alerts": { "telegram_chat_id": "-100123", "interval_seconds": 30, "grace_seconds": 120, "lag_mb": 64, "clock_ms": 200 }
```

## Limits

Two nodes; one schema; DDL is not replicated; no TRUNCATE; no primary-key updates; row-level last
change wins (two columns of one row changed on different nodes → the later row wins completely:
keep data with different writers in different tables).

## Testing

```sh
go test ./...                                          # unit tests
PGBISYNC_IT=1 go test -timeout 30m ./...               # + integration tests (podman or docker, postgres:17)
PGBISYNC_IT=1 PGBISYNC_CHAOS_MIN=30 go test -run TestChaos -timeout 50m ./test/
PGBISYNC_IT=1 PGBISYNC_BENCH=1 go test -run TestBench ./test/
```

- Scenarios: concurrent updates, retries, deletes vs. updates, unique merge, late commits, tie-break,
  echo, migrations, node restart, unchanged TOAST values.
- Chaos: random concurrent writes on both nodes, workers killed with SIGKILL, a node restarted; both
  nodes are checked against a reference model of the expected final state.
- Faults: partition, 50,000-row transaction (never partly visible), two workers, sender timeout,
  lost slot + rebuild, unknown parent, delete during apply, restart exactly once, least privilege.
- WAN: `TestChaosWAN` runs the chaos test through a proxy that simulates a wide-area link
  (130 ms ± 20 ms each way, random drops, cuts) with each database limited to half a CPU.
- Soak: the kit in [`soak/`](soak/README.md) runs for hours or days on any Linux machine with docker
  (no Go needed) and checks both databases against the reference model every hour.

Measured on a laptop (Postgres 17 in podman): write overhead of triggers + publication within
noise; apply ~2,800 single-row transactions/s end to end; idle worker 17 MB RSS, 0 % CPU; binary
10 MB.

## License

MIT
