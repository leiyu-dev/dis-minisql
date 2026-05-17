#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

action="${1:-health}"
case "$action" in
  health)
    pretty_http GET "http://127.0.0.1:$COORD_PORT/admin/health"
    ;;
  rebalance)
    pretty_http POST "http://127.0.0.1:$COORD_PORT/admin/rebalance" '{}'
    ;;
  snapshot)
    pretty_http POST "http://127.0.0.1:$COORD_PORT/admin/snapshot" '{}'
    ;;
  *)
    echo "Usage: $0 [health|rebalance|snapshot]" >&2
    exit 1
    ;;
esac
