#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <node-a|node-b|node-c|node-d> [cluster-config]" >&2
  exit 1
fi

node_id="$1"
config="${2:-$CLUSTER3_CONFIG}"
if [[ "$node_id" == "node-d" && $# -lt 2 ]]; then
  config="$CLUSTER4_CONFIG"
fi

if [[ ! -f "$config" ]]; then
  echo "Missing cluster config: $config" >&2
  exit 1
fi

ensure_runtime_dirs
pid_file="$(pid_file_for "$node_id")"
if [[ -f "$pid_file" ]] && kill -0 "$(tr -d '\n' < "$pid_file")" 2>/dev/null; then
  echo "$node_id already running with pid $(tr -d '\n' < "$pid_file")"
  exit 0
fi

port="$(node_port_for "$node_id")"
require_port_free "127.0.0.1" "$port" "$node_id"

cd "$ROOT_DIR"
java -jar "$JAR_PATH" datanode "$config" "$node_id" \
  > "$RUNTIME_DIR/$node_id.log" 2>&1 &
echo $! > "$pid_file"

wait_for_http "http://127.0.0.1:$port/health" "$node_id" 25
