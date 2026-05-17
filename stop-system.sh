#!/usr/bin/env bash
set -e

stop_pid_file() {
  pid_file="$1"
  if [[ ! -f "$pid_file" ]]; then
    return
  fi

  pid="$(tr -d '\n' < "$pid_file")"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
  fi
}

kill_pattern() {
  pattern="$1"
  pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
  for pid in $pids; do
    if [[ "$pid" != "$$" ]] && [[ "$pid" != "$PPID" ]]; then
      kill "$pid" 2>/dev/null || true
    fi
  done
}

kill_pattern_force() {
  pattern="$1"
  pids="$(pgrep -f "$pattern" 2>/dev/null || true)"
  for pid in $pids; do
    if [[ "$pid" != "$$" ]] && [[ "$pid" != "$PPID" ]]; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
}

for name in coordinator node-d node-c node-b node-a zk; do
  stop_pid_file "tmp/pid/$name.pid"
  stop_pid_file "tmp/$name.pid"
done

kill_pattern "java -jar dis-minisql/target/dis-minisql-1.0.0.jar coordinator"
kill_pattern "java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode"
kill_pattern "java -jar target/dis-minisql-1.0.0.jar coordinator"
kill_pattern "java -jar target/dis-minisql-1.0.0.jar datanode"
kill_pattern "java -jar /home/ly527609/homework/dis-minisql/dis-minisql/target/dis-minisql-1.0.0.jar coordinator"
kill_pattern "java -jar /home/ly527609/homework/dis-minisql/dis-minisql/target/dis-minisql-1.0.0.jar datanode"
kill_pattern "apache-zookeeper-3.9.5-bin"
kill_pattern "org.apache.zookeeper.server.quorum.QuorumPeerMain.*dis-minisql"
kill_pattern "/home/ly527609/homework/dis-minisql/minisql/build/bin/main --batch"
kill_pattern "minisql/build/bin/main --batch"

sleep 1

kill_pattern_force "java -jar dis-minisql/target/dis-minisql-1.0.0.jar coordinator"
kill_pattern_force "java -jar dis-minisql/target/dis-minisql-1.0.0.jar datanode"
kill_pattern_force "java -jar target/dis-minisql-1.0.0.jar coordinator"
kill_pattern_force "java -jar target/dis-minisql-1.0.0.jar datanode"
kill_pattern_force "java -jar /home/ly527609/homework/dis-minisql/dis-minisql/target/dis-minisql-1.0.0.jar coordinator"
kill_pattern_force "java -jar /home/ly527609/homework/dis-minisql/dis-minisql/target/dis-minisql-1.0.0.jar datanode"
kill_pattern_force "apache-zookeeper-3.9.5-bin"
kill_pattern_force "org.apache.zookeeper.server.quorum.QuorumPeerMain.*dis-minisql"
kill_pattern_force "/home/ly527609/homework/dis-minisql/minisql/build/bin/main --batch"
kill_pattern_force "minisql/build/bin/main --batch"

for port in 12181 18080 19001 19002 19003 19004; do
  pid="$(lsof -ti tcp:"$port" 2>/dev/null || true)"
  if [[ -n "$pid" ]]; then
    kill -9 $pid 2>/dev/null || true
  fi
done

echo "Stopped dis-minisql, ZooKeeper, and MiniSQL batch processes."
