#!/usr/bin/env bash
# ============================================================
# Start the interactive SQL client
#
# Usage:
#   ./start-client.sh [coordinator-host] [coordinator-port]
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
HOST="${1:-localhost}"
PORT="${2:-8080}"

JAR="${SCRIPT_DIR}/client/target/client-1.0.0-jar-with-dependencies.jar"
if [ ! -f "$JAR" ]; then
    echo "ERROR: JAR not found at $JAR.  Run: mvn package -pl client -am"
    exit 1
fi

exec java -jar "$JAR" --host="$HOST" --port="$PORT"
