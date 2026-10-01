#!/usr/bin/env bash
# Reproduce the on-sale stampede against a running instance.
#
#   ./burst.sh <BASE_URL> [extra flags for cmd/burst]
#   ADMIN_TOKEN=... ./burst.sh https://your-live-url
#
# Needs Go (uses `go run`), or run the prebuilt binary from the image:
#   docker run --rm --entrypoint /burst <image> -url <BASE_URL> -admin-token <token>
set -euo pipefail

BASE_URL="${1:-${BASE_URL:-http://localhost:8787}}"
shift || true
ADMIN_TOKEN="${ADMIN_TOKEN:-admin-dev-token}"

cd "$(dirname "$0")"
# Lots of concurrent sockets; raise the soft fd limit if we can.
ulimit -n 65535 2>/dev/null || true
exec go run ./cmd/burst -url "$BASE_URL" -admin-token "$ADMIN_TOKEN" "$@"
