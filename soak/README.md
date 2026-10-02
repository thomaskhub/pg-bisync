# pg-bisync soak kit

Runs pg-bisync for hours or days on any Linux machine with **docker or podman** (no Go needed):
two PostgreSQL 17 containers, two sync workers, a simulated wide-area link (130 ms ± 20 ms each
way) and a steady load on both nodes with faults (worker killed every ~10 min, network cut every
~30 min, database restart every ~3 h). Every checkpoint (default hourly) pauses the load, waits for
the sync and compares both databases with a reference model of the expected final state.

## Build (on a machine with Go)

```sh
soak/bundle.sh            # or: soak/bundle.sh arm64
# → dist/pg-bisync-soak-amd64.tar.gz
```

## Run (on the test machine)

```sh
tar xzf pg-bisync-soak-amd64.tar.gz && cd pg-bisync-soak-amd64
./run.sh 2m                                # smoke test first
nohup ./run.sh 48h > soak.out 2>&1 &       # or inside tmux
tail -f soak.out
```

Options: `--rate 20` (operations per second, both nodes), `--cpus 0.5` (limit each database, like
a small VM), `--no-wan`, `--check 1h`.

## Results

`results/<timestamp>/`:

- `report.md`: result, settings, events, checkpoints (operations, sync wait, worker memory, database size)
- `soak.jsonl`: one line per event and checkpoint
- `worker-in.log`, `worker-eu.log`: worker logs
- `test.log`: test output

On a failed checkpoint the run stops and the containers are kept for analysis
(`docker ps -a --filter name=pgbisync-test`; remove with `docker rm -f $(docker ps -aq --filter name=pgbisync-test)`).
