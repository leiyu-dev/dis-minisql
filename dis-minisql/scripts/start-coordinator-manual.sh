#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

config="${1:-$CLUSTER3_CONFIG}"
if [[ ! -f "$config" ]]; then
  echo "Missing cluster config: $config" >&2
  exit 1
fi

ensure_runtime_dirs
pid_file="$(pid_file_for coordinator)"
if [[ -f "$pid_file" ]] && kill -0 "$(tr -d '\n' < "$pid_file")" 2>/dev/null; then
  echo "Coordinator already running with pid $(tr -d '\n' < "$pid_file")"
  exit 0
fi

require_port_free "127.0.0.1" "$COORD_PORT" "Coordinator"

cd "$ROOT_DIR"
java -jar "$JAR_PATH" coordinator "$config" \
  > "$RUNTIME_DIR/coordinator.log" 2>&1 &
echo $! > "$pid_file"

wait_for_http "http://127.0.0.1:$COORD_PORT/nodes" "Coordinator" 25
