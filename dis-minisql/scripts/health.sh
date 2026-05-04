#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

target="${1:-coordinator}"
case "$target" in
  coordinator)
    pretty_http GET "http://127.0.0.1:$COORD_PORT/admin/health"
    ;;
  nodes)
    pretty_http GET "http://127.0.0.1:$COORD_PORT/nodes"
    ;;
  node-a|node-b|node-c|node-d)
    port="$(node_port_for "$target")"
    pretty_http GET "http://127.0.0.1:$port/health"
    ;;
  *)
    echo "Usage: $0 [coordinator|nodes|node-a|node-b|node-c|node-d]" >&2
    exit 1
    ;;
esac
