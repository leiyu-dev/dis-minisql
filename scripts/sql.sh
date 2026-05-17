#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/manual-common.sh"

if [[ $# -gt 0 ]]; then
  statement="$*"
else
  statement="$(python3 -c 'import sys; print(sys.stdin.read().strip())')"
fi

if [[ -z "$statement" ]]; then
  echo "Usage: $0 '<sql statement>'" >&2
  exit 1
fi

python3 - "$COORD_PORT" "$statement" <<'PY'
import json
import sys
import urllib.request

port, statement = int(sys.argv[1]), sys.argv[2]
payload = json.dumps({"sql": statement}).encode("utf-8")
request = urllib.request.Request(
    f"http://127.0.0.1:{port}/sql",
    data=payload,
    method="POST",
)
request.add_header("Content-Type", "application/json")
with urllib.request.urlopen(request, timeout=20) as response:
    body = json.loads(response.read().decode("utf-8"))
print(json.dumps(body, indent=2, ensure_ascii=False))
PY
