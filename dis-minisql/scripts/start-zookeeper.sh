#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
ZK_DIR="$ROOT_DIR/apache-zookeeper-3.9.5-bin"
CFG="$ZK_DIR/conf/zoo.cfg"

if [[ ! -f "$CFG" ]]; then
  cp "$ZK_DIR/conf/zoo_sample.cfg" "$CFG"
  sed -i "s#dataDir=/tmp/zookeeper#dataDir=$ROOT_DIR/zkdata#" "$CFG"
fi

mkdir -p "$ROOT_DIR/zkdata"
"$ZK_DIR/bin/zkServer.sh" start "$CFG"
