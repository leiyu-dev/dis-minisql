#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

if [[ "${CLEAN_RUNTIME:-1}" == "1" ]]; then
  if [[ "$RUNTIME_DIR" == "/" || "$RUNTIME_DIR" == "$ROOT_DIR" ]]; then
    echo "Refuse to clean unsafe runtime directory: $RUNTIME_DIR" >&2
    exit 1
  fi
  rm -rf "$RUNTIME_DIR"
fi

ensure_runtime_dirs

{
  printf 'tickTime=2000\n'
  printf 'initLimit=10\n'
  printf 'syncLimit=5\n'
  printf 'dataDir=%s/zkdata\n' "$RUNTIME_DIR"
  printf 'clientPort=%s\n' "$ZK_PORT"
  printf 'admin.enableServer=false\n'
} > "$ZOO_CFG"

python3 - "$CLUSTER3_CONFIG" "$CLUSTER4_CONFIG" <<PY
import json
import sys

cluster3_path, cluster4_path = sys.argv[1], sys.argv[2]

base = {
    "clusterName": "$CLUSTER_NAME",
    "zkConnect": "$ZK_HOST:$ZK_PORT",
    "sessionTimeoutMs": int("$SESSION_TIMEOUT_MS"),
    "shardCount": int("$SHARD_COUNT"),
    "replicationFactor": int("$REPLICATION_FACTOR"),
    "defaultDatabase": "$DEFAULT_DATABASE",
    "coordinatorHost": "$COORD_HOST",
    "coordinatorPort": int("$COORD_PORT"),
    "minisqlBinary": "$MINISQL_BINARY",
}

nodes3 = [
    {"nodeId": "node-a", "host": "$NODE_A_HOST", "port": int("$NODE_A_PORT"), "dataDir": "$RUNTIME_DIR/node-a"},
    {"nodeId": "node-b", "host": "$NODE_B_HOST", "port": int("$NODE_B_PORT"), "dataDir": "$RUNTIME_DIR/node-b"},
    {"nodeId": "node-c", "host": "$NODE_C_HOST", "port": int("$NODE_C_PORT"), "dataDir": "$RUNTIME_DIR/node-c"},
]
nodes4 = nodes3 + [
    {"nodeId": "node-d", "host": "$NODE_D_HOST", "port": int("$NODE_D_PORT"), "dataDir": "$RUNTIME_DIR/node-d"},
]

for path, nodes in [(cluster3_path, nodes3), (cluster4_path, nodes4)]:
    config = dict(base)
    config["nodes"] = nodes
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)
        f.write("\\n")
PY

python3 -m json.tool "$CLUSTER3_CONFIG" >/dev/null
python3 -m json.tool "$CLUSTER4_CONFIG" >/dev/null

echo "Prepared manual runtime: $RUNTIME_DIR"
echo "ZooKeeper config: $ZOO_CFG"
echo "3-node cluster config: $CLUSTER3_CONFIG"
echo "4-node cluster config: $CLUSTER4_CONFIG"
