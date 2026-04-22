#!/usr/bin/env bash
# ============================================================
# Start a DataNode
#
# Usage:
#   ./start-datanode.sh <node-id> <port> [zk-connect]
#
# Defaults:
#   node-id     = node1
#   port        = 9001
#   zk-connect  = localhost:2181
#
# Example (3-node cluster on one machine):
#   ./start-datanode.sh node1 9001
#   ./start-datanode.sh node2 9002
#   ./start-datanode.sh node3 9003
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

NODE_ID="${1:-node1}"
PORT="${2:-9001}"
ZK="${3:-localhost:2181}"

# Path to the compiled minisql binary
MINISQL_BIN="${SCRIPT_DIR}/minisql/build/minisql"
if [ ! -f "$MINISQL_BIN" ]; then
    echo "ERROR: minisql binary not found at $MINISQL_BIN"
    echo "       Please build it first: cd minisql && mkdir build && cd build && cmake .. && make"
    exit 1
fi

# Path to the DataNode fat-jar
JAR="${SCRIPT_DIR}/datanode/target/datanode-1.0.0-jar-with-dependencies.jar"
if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found at $JAR.  Run: mvn package -pl datanode -am"
    exit 1
fi

DATA_DIR="${SCRIPT_DIR}/data"
mkdir -p "$DATA_DIR/$NODE_ID"
mkdir -p "${SCRIPT_DIR}/logs"

echo "Starting DataNode: id=$NODE_ID  port=$PORT  zk=$ZK"
exec java -jar "$JAR" \
    --node-id="$NODE_ID" \
    --host=localhost \
    --port="$PORT" \
    --zk="$ZK" \
    --minisql="$MINISQL_BIN" \
    --data-dir="$DATA_DIR" \
    --replication-factor=2
