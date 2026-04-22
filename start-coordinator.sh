#!/usr/bin/env bash
# ============================================================
# Start the Coordinator
#
# Usage:
#   ./start-coordinator.sh [port] [zk-connect] [replication-factor]
#
# Defaults:
#   port                = 8080
#   zk-connect          = localhost:2181
#   replication-factor  = 2
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

PORT="${1:-8080}"
ZK="${2:-localhost:2181}"
REPL="${3:-2}"

JAR="${SCRIPT_DIR}/coordinator/target/coordinator-1.0.0-jar-with-dependencies.jar"
if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found at $JAR.  Run: mvn package -pl coordinator -am"
    exit 1
fi

mkdir -p "${SCRIPT_DIR}/logs"

echo "Starting Coordinator: port=$PORT  zk=$ZK  replication-factor=$REPL"
exec java -jar "$JAR" \
    --host=0.0.0.0 \
    --port="$PORT" \
    --zk="$ZK" \
    --replication-factor="$REPL"
