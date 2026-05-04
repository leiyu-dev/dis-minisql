#!/usr/bin/env python3
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
JAVA_DIR = ROOT / "dis-minisql"
JAR = JAVA_DIR / "target" / "dis-minisql-1.0.0.jar"
MINISQL = ROOT / "minisql" / "build" / "bin" / "main"
ZK_DIR = ROOT / "apache-zookeeper-3.9.5-bin"
COORD_PORT = 18080
RUNTIME_DIR = Path(os.environ.get("DIS_MINISQL_E2E_RUNTIME", str(ROOT / "tmp"))).expanduser().resolve()


def wait_for_port(port, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise RuntimeError(f"port {port} did not open")


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def http_json(method, url, payload=None, timeout=15):
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=data, method=method)
    request.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_http(url, timeout=25):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return http_json("GET", url)
        except Exception as exc:
            last_error = exc
            time.sleep(0.3)
    raise RuntimeError(f"HTTP endpoint not ready: {url}: {last_error}")


def start_process(cmd, cwd, log_file):
    log = open(log_file, "w", encoding="utf-8")
    return subprocess.Popen(cmd, cwd=cwd, stdout=log, stderr=subprocess.STDOUT, text=True)


def stop_process(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)
    print(f"[PASS] {message}")


def write_config(path, runtime, nodes, zk_port, coordinator_port):
    config = {
        "clusterName": "dis-minisql-e2e",
        "zkConnect": f"127.0.0.1:{zk_port}",
        "sessionTimeoutMs": 8000,
        "shardCount": 3,
        "replicationFactor": 3,
        "defaultDatabase": "dist",
        "coordinatorHost": "127.0.0.1",
        "coordinatorPort": coordinator_port,
        "minisqlBinary": str(MINISQL),
        "nodes": nodes,
    }
    path.write_text(json.dumps(config, indent=2), encoding="utf-8")


def sql(statement):
    return http_json("POST", f"http://127.0.0.1:{COORD_PORT}/sql", {"sql": statement})


def main():
    global COORD_PORT
    assert_true(JAR.exists(), "Java jar exists")
    assert_true(MINISQL.exists(), "MiniSQL binary exists")
    assert_true((ZK_DIR / "bin" / "zkServer.sh").exists(), "ZooKeeper distribution exists")

    runtime = RUNTIME_DIR
    if runtime.exists():
        shutil.rmtree(runtime)
    runtime.mkdir(parents=True)
    print(f"[INFO] runtime: {runtime}")
    processes = []
    try:
        zk_cfg = runtime / "zoo.cfg"
        zk_data = runtime / "zkdata"
        zk_port = free_port()
        COORD_PORT = free_port()
        node_ports = [free_port() for _ in range(4)]
        zk_cfg.write_text(
            "\n".join([
                "tickTime=2000",
                "initLimit=10",
                "syncLimit=5",
                f"dataDir={zk_data}",
                f"clientPort={zk_port}",
                "admin.enableServer=false",
                "",
            ]),
            encoding="utf-8",
        )
        zk = start_process([str(ZK_DIR / "bin" / "zkServer.sh"), "start-foreground", str(zk_cfg)], ROOT, runtime / "zk.log")
        processes.append(zk)
        wait_for_port(zk_port)
        assert_true(True, "ZooKeeper started")

        nodes3 = [
            {"nodeId": "node-a", "host": "127.0.0.1", "port": node_ports[0], "dataDir": str(runtime / "node-a")},
            {"nodeId": "node-b", "host": "127.0.0.1", "port": node_ports[1], "dataDir": str(runtime / "node-b")},
            {"nodeId": "node-c", "host": "127.0.0.1", "port": node_ports[2], "dataDir": str(runtime / "node-c")},
        ]
        nodes4 = nodes3 + [
            {"nodeId": "node-d", "host": "127.0.0.1", "port": node_ports[3], "dataDir": str(runtime / "node-d")},
        ]
        config3 = runtime / "cluster3.json"
        config4 = runtime / "cluster4.json"
        write_config(config3, runtime, nodes3, zk_port, COORD_PORT)
        write_config(config4, runtime, nodes4, zk_port, COORD_PORT)

        subprocess.check_call(["java", "-jar", str(JAR), "init-zk", str(config3)], cwd=JAVA_DIR)
        for node in ["node-a", "node-b", "node-c"]:
            p = start_process(["java", "-jar", str(JAR), "datanode", str(config3), node], JAVA_DIR, runtime / f"{node}.log")
            processes.append(p)
        for port in node_ports[:3]:
            wait_for_http(f"http://127.0.0.1:{port}/health")
        assert_true(True, "three DataNodes started")

        coordinator = start_process(["java", "-jar", str(JAR), "coordinator", str(config3)], JAVA_DIR, runtime / "coordinator.log")
        processes.append(coordinator)
        wait_for_http(f"http://127.0.0.1:{COORD_PORT}/nodes")
        assert_true(True, "Coordinator started")

        assert_true(sql("create database dist;")["ok"], "create database broadcast succeeds")
        assert_true(sql("create table t (id int unique, age int, primary key(id));")["ok"], "create table t succeeds")
        assert_true(sql("create table o (id int unique, user_id int, amount int, primary key(id));")["ok"], "create table o succeeds")

        for statement in [
            "insert into t values (1, 10);",
            "insert into t values (2, 20);",
            "insert into t values (3, 30);",
            "insert into o values (101, 1, 7);",
            "insert into o values (102, 2, 9);",
        ]:
            response = sql(statement)
            assert_true(response["ok"] and "raft write" in response["route"], f"raft quorum write: {statement}")

        count = sql("select count(*) from t;")
        assert_true(count["ok"] and "3" in count.get("mergedOutput", ""), "coordinator count aggregation works")

        ordered = sql("select * from t order by age desc;")
        assert_true(ordered["ok"] and "mergedOutput" in ordered, "coordinator order by merge works")

        joined = sql("select * from t join o on t.id = o.user_id;")
        assert_true(joined["ok"] and "101" in joined.get("mergedOutput", ""), "coordinator hash join works")

        stop_process(processes.pop(1))  # stop node-a
        time.sleep(1)
        failover = sql("insert into t values (4, 40);")
        assert_true(failover["ok"], "write continues after one DataNode stops")

        node_d = start_process(["java", "-jar", str(JAR), "datanode", str(config4), "node-d"], JAVA_DIR, runtime / "node-d.log")
        processes.append(node_d)
        wait_for_http(f"http://127.0.0.1:{node_ports[3]}/health")
        rebalance = http_json("POST", f"http://127.0.0.1:{COORD_PORT}/admin/rebalance", {})
        assert_true(any("node-d" in shard["replicas"] for shard in rebalance), "dynamic rebalance includes node-d")

        health = http_json("GET", f"http://127.0.0.1:{COORD_PORT}/admin/health")
        assert_true(len(health["nodes"]) >= 3 and len(health["shards"]) == 3, "admin health returns nodes and shards")

        snapshot = http_json("POST", f"http://127.0.0.1:{COORD_PORT}/admin/snapshot", {})
        assert_true(len(snapshot) >= 3, "admin snapshot triggers on live nodes")

        final_count = sql("select count(*) from t;")
        assert_true(final_count["ok"] and "4" in final_count.get("mergedOutput", ""), "final count after failover and rebalance works")

        print("[PASS] full distributed MiniSQL system test completed")
    finally:
        for process in reversed(processes):
            stop_process(process)
        print(f"[INFO] runtime kept for logs: {runtime}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[FAIL] {exc}", file=sys.stderr)
        raise
