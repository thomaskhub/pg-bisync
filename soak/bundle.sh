#!/usr/bin/env bash
# Builds a self-contained soak kit for another Linux machine (needs only docker or podman there).
#   soak/bundle.sh [amd64|arm64]   → dist/pg-bisync-soak-<arch>.tar.gz
set -euo pipefail
ARCH="${1:-$(go env GOARCH)}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/dist/pg-bisync-soak-$ARCH"
rm -rf "$OUT"
mkdir -p "$OUT/test" "$OUT/examples"
cd "$ROOT"
export GOOS=linux GOARCH="$ARCH" CGO_ENABLED=0
go build -trimpath -ldflags="-s -w" -o "$OUT/pg-bisync" ./cmd/pg-bisync
go test -c -trimpath -o "$OUT/test/soak.test" ./test
cp examples/schema.sql "$OUT/examples/"
cp soak/run.sh soak/README.md "$OUT/"
tar -C "$ROOT/dist" -czf "$ROOT/dist/pg-bisync-soak-$ARCH.tar.gz" "pg-bisync-soak-$ARCH"
echo "dist/pg-bisync-soak-$ARCH.tar.gz"
