#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

force="${1:-}"

for name in coordinator node-d node-c node-b node-a zk; do
  pid_file="$(pid_file_for "$name")"
  if [[ ! -f "$pid_file" ]]; then
    continue
  fi
  pid="$(tr -d '\n' < "$pid_file")"
  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    continue
  fi
  if [[ "$force" == "--force" ]]; then
    kill -9 "$pid" 2>/dev/null || true
  else
    kill "$pid" 2>/dev/null || true
  fi
done

echo "Stopped known manual test processes. Runtime kept at: $RUNTIME_DIR"
