#!/usr/bin/env bash
# Runs the pg-bisync soak test: two PostgreSQL 17 containers, two sync workers, a simulated WAN
# link and a steady load with faults; checks both databases against a reference model.
#   ./run.sh <duration> [--rate N] [--cpus 0.5] [--no-wan] [--check 1h]
# Example: nohup ./run.sh 48h > soak.out 2>&1 &   (or inside tmux)
set -euo pipefail
cd "$(dirname "$0")"
DURATION="${1:?usage: ./run.sh <duration, e.g. 48h> [--rate N] [--cpus 0.5] [--no-wan] [--check 1h]}"
shift
RATE=20 CPUS="" WAN=1 CHECK=1h
while [ $# -gt 0 ]; do
  case "$1" in
    --rate) RATE="$2"; shift 2 ;;
    --cpus) CPUS="$2"; shift 2 ;;
    --no-wan) WAN=0; shift ;;
    --check) CHECK="$2"; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
ENGINE="${PGBISYNC_CONTAINER:-}"
if [ -z "$ENGINE" ]; then
  if command -v docker >/dev/null; then ENGINE=docker; elif command -v podman >/dev/null; then ENGINE=podman; else
    echo "needs docker or podman" >&2; exit 1
  fi
fi
"$ENGINE" info >/dev/null 2>&1 || { echo "$ENGINE is not usable by this user (docker group / daemon running?)" >&2; exit 1; }
"$ENGINE" pull -q docker.io/library/postgres:17 >/dev/null
RESULTS="$PWD/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RESULTS"
echo "soak $DURATION (rate $RATE/s, WAN $WAN, CPUs '${CPUS:-none}', checkpoint $CHECK) → $RESULTS"
cd test
set +e
PGBISYNC_IT=1 PGBISYNC_KEEP=1 PGBISYNC_CONTAINER="$ENGINE" PGBISYNC_BIN="$PWD/../pg-bisync" \
  PGBISYNC_SOAK="$DURATION" PGBISYNC_SOAK_RATE="$RATE" PGBISYNC_SOAK_CPUS="$CPUS" PGBISYNC_SOAK_WAN="$WAN" \
  PGBISYNC_SOAK_CHECK="$CHECK" PGBISYNC_SOAK_DIR="$RESULTS" \
  ./soak.test -test.run '^TestSoak$' -test.v -test.timeout 0 2>&1 | tee "$RESULTS/test.log"
RC=${PIPESTATUS[0]}
set -e
echo
echo "report: $RESULTS/report.md"
[ "$RC" -eq 0 ] && echo "RESULT: PASS" || echo "RESULT: FAIL (exit $RC) — containers kept: $ENGINE ps -a --filter name=pgbisync-test"
exit "$RC"
