#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

ensure_runtime_dirs

if [[ ! -f "$ZOO_CFG" ]]; then
  echo "Missing $ZOO_CFG. Run scripts/prepare-manual-runtime.sh first." >&2
  exit 1
fi

pid_file="$(pid_file_for zk)"
if [[ -f "$pid_file" ]] && kill -0 "$(tr -d '\n' < "$pid_file")" 2>/dev/null; then
  echo "ZooKeeper already running with pid $(tr -d '\n' < "$pid_file")"
  exit 0
fi

require_port_free "$ZK_HOST" "$ZK_PORT" "ZooKeeper"

"$ROOT_DIR/apache-zookeeper-3.9.5-bin/bin/zkServer.sh" start-foreground "$ZOO_CFG" \
  > "$RUNTIME_DIR/zk.log" 2>&1 &
echo $! > "$pid_file"

wait_for_port "$ZK_HOST" "$ZK_PORT" "ZooKeeper" 20
