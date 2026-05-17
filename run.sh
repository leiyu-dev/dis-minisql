#!/usr/bin/env bash
set -euo pipefail

wait_for_zookeeper() {
  python3 - <<'PY'
import socket
import time

deadline = time.time() + 30
while time.time() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", 12181), timeout=0.5):
            print("ZooKeeper is ready on 127.0.0.1:12181")
            break
    except OSError:
        time.sleep(0.2)
else:
    raise SystemExit("ZooKeeper did not open on 127.0.0.1:12181")
PY
}

wait_for_datanodes() {
  python3 - <<'PY'
import json
import time
import urllib.request

for node_id, port in [("node-a", 19001), ("node-b", 19002), ("node-c", 19003)]:
    url = f"http://127.0.0.1:{port}/health"
    deadline = time.time() + 40
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                body = json.loads(response.read().decode("utf-8"))
            if body.get("replicaState") == "SERVING":
                print(f"{node_id} is SERVING on 127.0.0.1:{port}")
                break
            last_error = f"state={body.get('replicaState')}"
        except Exception as exc:
            last_error = exc
        time.sleep(0.3)
    else:
        raise SystemExit(f"{node_id} is not SERVING at {url}: {last_error}")
PY
}

wait_for_coordinator() {
  python3 - <<'PY'
import json
import time
import urllib.request

url = "http://127.0.0.1:18080/nodes"
deadline = time.time() + 30
last_error = None
while time.time() < deadline:
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            nodes = json.loads(response.read().decode("utf-8"))
        if len(nodes) >= 3:
            print("Coordinator is ready on 127.0.0.1:18080 with 3 DataNodes")
            break
        last_error = f"registered nodes={len(nodes)}"
    except Exception as exc:
        last_error = exc
    time.sleep(0.3)
else:
    raise SystemExit(f"Coordinator is not ready at {url}: {last_error}")
PY
}

# build
cd minisql/build
cmake ..
make -j4 
cd ../..
mvn -q -f dis-minisql/pom.xml package

./stop-system.sh
rm -rf tmp
mkdir -p tmp tmp/log tmp/pid tmp/zkdata tmp/node-a tmp/node-b tmp/node-c

# start zookeeper
apache-zookeeper-3.9.5-bin/bin/zkServer.sh start-foreground ./config/zoo.cfg > tmp/log/zk.log 2>&1 &
echo "$!" > tmp/pid/zk.pid
wait_for_zookeeper


java -jar dis-minisql/target/dis-minisql-1.0.0.jar init-zk ./config/cluster3.json

# start datanodes
java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode ./config/cluster3.json node-a > tmp/log/node-a.log 2>&1 &
echo "$!" > tmp/pid/node-a.pid
java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode ./config/cluster3.json node-b > tmp/log/node-b.log 2>&1 &
echo "$!" > tmp/pid/node-b.pid
java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode ./config/cluster3.json node-c > tmp/log/node-c.log 2>&1 &
echo "$!" > tmp/pid/node-c.pid

wait_for_datanodes

# start coordinator
java -jar dis-minisql/target/dis-minisql-1.0.0.jar coordinator ./config/cluster3.json > tmp/log/coordinator.log 2>&1 &
echo "$!" > tmp/pid/coordinator.pid
wait_for_coordinator

# start foreground client
java -jar dis-minisql/target/dis-minisql-1.0.0.jar client ./config/cluster3.json
