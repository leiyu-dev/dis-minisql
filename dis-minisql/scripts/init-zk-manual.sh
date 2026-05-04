#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

config="${1:-$CLUSTER3_CONFIG}"
if [[ ! -f "$config" ]]; then
  echo "Missing cluster config: $config" >&2
  exit 1
fi

cd "$ROOT_DIR"
java -jar "$JAR_PATH" init-zk "$config"
