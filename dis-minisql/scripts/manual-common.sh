#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ROOT_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
CONFIG_ENV="${DIS_MINISQL_MANUAL_ENV:-$PROJECT_DIR/config/manual-test.env}"

if [[ ! -f "$CONFIG_ENV" ]]; then
  echo "Missing manual test config: $CONFIG_ENV" >&2
  exit 1
fi

# shellcheck source=../config/manual-test.env
source "$CONFIG_ENV"

RUNTIME_DIR="$(python3 -c 'import os, sys; print(os.path.abspath(os.path.expanduser(sys.argv[1])))' "$RUNTIME_DIR")"
CLUSTER3_CONFIG="$RUNTIME_DIR/cluster3.json"
CLUSTER4_CONFIG="$RUNTIME_DIR/cluster4.json"
ZOO_CFG="$RUNTIME_DIR/zoo.cfg"

ensure_runtime_dirs() {
  mkdir -p "$RUNTIME_DIR/pids" "$RUNTIME_DIR/zkdata" \
    "$RUNTIME_DIR/node-a" "$RUNTIME_DIR/node-b" "$RUNTIME_DIR/node-c" "$RUNTIME_DIR/node-d"
}

pid_file_for() {
  printf '%s/pids/%s.pid\n' "$RUNTIME_DIR" "$1"
}

node_port_for() {
  case "$1" in
    node-a) printf '%s\n' "$NODE_A_PORT" ;;
    node-b) printf '%s\n' "$NODE_B_PORT" ;;
    node-c) printf '%s\n' "$NODE_C_PORT" ;;
    node-d) printf '%s\n' "$NODE_D_PORT" ;;
    *) echo "Unknown node id: $1" >&2; exit 1 ;;
  esac
}

wait_for_port() {
  local host="$1"
  local port="$2"
  local label="$3"
  local timeout="${4:-25}"
  python3 - "$host" "$port" "$label" "$timeout" <<'PY'
import socket
import sys
import time

host, port, label, timeout = sys.argv[1], int(sys.argv[2]), sys.argv[3], float(sys.argv[4])
deadline = time.time() + timeout
while time.time() < deadline:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            print(f"{label} is ready on {host}:{port}")
            break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit(f"{label} did not open on {host}:{port}")
PY
}

require_port_free() {
  local host="$1"
  local port="$2"
  local label="$3"
  python3 - "$host" "$port" "$label" <<'PY'
import socket
import sys

host, port, label = sys.argv[1], int(sys.argv[2]), sys.argv[3]
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sock.settimeout(0.5)
    if sock.connect_ex((host, port)) == 0:
        raise SystemExit(f"{label} port is already in use: {host}:{port}")
PY
}

wait_for_http() {
  local url="$1"
  local label="$2"
  local timeout="${3:-25}"
  python3 - "$url" "$label" "$timeout" <<'PY'
import sys
import time
import urllib.request

url, label, timeout = sys.argv[1], sys.argv[2], float(sys.argv[3])
deadline = time.time() + timeout
last_error = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            response.read()
            print(f"{label} is ready: {url}")
            break
    except Exception as exc:
        last_error = exc
        time.sleep(0.3)
else:
    raise SystemExit(f"{label} is not ready: {url}: {last_error}")
PY
}

pretty_http() {
  local method="$1"
  local url="$2"
  local payload="${3:-}"
  python3 - "$method" "$url" "$payload" <<'PY'
import json
import sys
import urllib.request

method, url, payload = sys.argv[1], sys.argv[2], sys.argv[3]
data = payload.encode("utf-8") if payload else None
request = urllib.request.Request(url, data=data, method=method)
request.add_header("Content-Type", "application/json")
with urllib.request.urlopen(request, timeout=20) as response:
    text = response.read().decode("utf-8")
try:
    print(json.dumps(json.loads(text), indent=2, ensure_ascii=False))
except json.JSONDecodeError:
    print(text)
PY
}

require_build_outputs() {
  test -x "$MINISQL_BINARY"
  test -f "$JAR_PATH"
  test -x "$ROOT_DIR/apache-zookeeper-3.9.5-bin/bin/zkServer.sh"
}
