#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

step() {
  printf '\n==> %s\n' "$1"
}

cleanup() {
  if [[ "${KEEP_RUNNING:-0}" == "1" ]]; then
    echo "KEEP_RUNNING=1, leaving processes running. Runtime: $RUNTIME_DIR"
  else
    "$SCRIPT_DIR/stop-manual-test.sh" >/dev/null 2>&1 || true
    echo "Processes stopped. Runtime and logs kept at: $RUNTIME_DIR"
  fi
}
trap cleanup EXIT

assert_json_field_contains() {
  local response="$1"
  local field="$2"
  local needle="$3"
  local message="$4"
  RESPONSE="$response" python3 - "$field" "$needle" "$message" <<'PY'
import json
import os
import sys

field, needle, message = sys.argv[1], sys.argv[2], sys.argv[3]
body = json.loads(os.environ["RESPONSE"])
if not body.get("ok"):
    raise SystemExit(f"FAIL: {message}: ok is not true: {body}")
if needle and needle not in str(body.get(field, "")):
    raise SystemExit(f"FAIL: {message}: {field} does not contain {needle!r}: {body}")
print(f"PASS: {message}")
PY
}

assert_rebalance_includes_node_d() {
  local response="$1"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["RESPONSE"])
if not any("node-d" in shard.get("replicas", []) for shard in body):
    raise SystemExit(f"FAIL: rebalance result does not include node-d: {body}")
print("PASS: dynamic rebalance includes node-d")
PY
}

assert_health() {
  local response="$1"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["RESPONSE"])
if len(body.get("nodes", [])) < 3 or len(body.get("shards", [])) != 3:
    raise SystemExit(f"FAIL: unexpected health response: {body}")
print("PASS: admin health returns nodes and shards")
PY
}

assert_snapshot() {
  local response="$1"
  RESPONSE="$response" python3 - <<'PY'
import json
import os

body = json.loads(os.environ["RESPONSE"])
if len(body) < 3:
    raise SystemExit(f"FAIL: snapshot returned fewer than 3 nodes: {body}")
print("PASS: admin snapshot triggers on live nodes")
PY
}

json_compact() {
  python3 -c 'import json, sys; print(json.dumps(json.load(sys.stdin), ensure_ascii=False))'
}

run_sql() {
  local statement="$1"
  echo "SQL> $statement" >&2
  "$SCRIPT_DIR/sql.sh" "$statement" | json_compact
}

run_admin() {
  "$SCRIPT_DIR/admin.sh" "$1" | json_compact
}

step "Stop any previous manual test processes"
"$SCRIPT_DIR/stop-manual-test.sh" --force >/dev/null 2>&1 || true

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  step "Build MiniSQL C++ main"
  cmake -S "$ROOT_DIR/minisql" -B "$ROOT_DIR/minisql/build"
  cmake --build "$ROOT_DIR/minisql/build" --target main -j2

  step "Build Java distributed layer"
  mvn -q -f "$PROJECT_DIR/pom.xml" package
fi

step "Check required build outputs"
require_build_outputs

step "Prepare runtime configs"
"$SCRIPT_DIR/prepare-manual-runtime.sh"

step "Start ZooKeeper"
"$SCRIPT_DIR/start-zookeeper-manual.sh"

step "Initialize ZooKeeper metadata"
"$SCRIPT_DIR/init-zk-manual.sh" "$CLUSTER3_CONFIG"

step "Start three initial DataNodes"
"$SCRIPT_DIR/start-node-manual.sh" node-a "$CLUSTER3_CONFIG"
"$SCRIPT_DIR/start-node-manual.sh" node-b "$CLUSTER3_CONFIG"
"$SCRIPT_DIR/start-node-manual.sh" node-c "$CLUSTER3_CONFIG"

step "Start Coordinator"
"$SCRIPT_DIR/start-coordinator-manual.sh" "$CLUSTER3_CONFIG"

step "Create database and tables"
response="$(run_sql "create database dist;")"
assert_json_field_contains "$response" "route" "" "create database broadcast succeeds"
response="$(run_sql "create table t (id int unique, age int, primary key(id));")"
assert_json_field_contains "$response" "route" "" "create table t succeeds"
response="$(run_sql "create table o (id int unique, user_id int, amount int, primary key(id));")"
assert_json_field_contains "$response" "route" "" "create table o succeeds"

step "Run quorum writes"
for statement in \
  "insert into t values (1, 10);" \
  "insert into t values (2, 20);" \
  "insert into t values (3, 30);" \
  "insert into o values (101, 1, 7);" \
  "insert into o values (102, 2, 9);"
do
  response="$(run_sql "$statement")"
  assert_json_field_contains "$response" "route" "raft write" "raft quorum write: $statement"
done

step "Verify distributed queries"
response="$(run_sql "select count(*) from t;")"
assert_json_field_contains "$response" "mergedOutput" "3" "coordinator count aggregation works"
response="$(run_sql "select * from t order by age desc;")"
assert_json_field_contains "$response" "mergedOutput" "" "coordinator order by merge works"
response="$(run_sql "select * from t join o on t.id = o.user_id;")"
assert_json_field_contains "$response" "mergedOutput" "101" "coordinator hash join works"

step "Stop node-a to simulate one DataNode failure"
node_a_pid_file="$(pid_file_for node-a)"
if [[ -f "$node_a_pid_file" ]]; then
  kill "$(tr -d '\n' < "$node_a_pid_file")" 2>/dev/null || true
fi
sleep 1

step "Verify write continues after one DataNode stops"
response="$(run_sql "insert into t values (4, 40);")"
assert_json_field_contains "$response" "route" "" "write continues after one DataNode stops"

step "Start node-d with four-node config"
"$SCRIPT_DIR/start-node-manual.sh" node-d "$CLUSTER4_CONFIG"

step "Trigger rebalance"
response="$(run_admin rebalance)"
assert_rebalance_includes_node_d "$response"

step "Check admin health"
response="$(run_admin health)"
assert_health "$response"

step "Trigger snapshots"
response="$(run_admin snapshot)"
assert_snapshot "$response"

step "Verify final count after failover and rebalance"
response="$(run_sql "select count(*) from t;")"
assert_json_field_contains "$response" "mergedOutput" "4" "final count after failover and rebalance works"

step "Manual E2E test completed"
echo "PASS: full manual distributed MiniSQL test completed"
echo "Runtime and logs: $RUNTIME_DIR"
