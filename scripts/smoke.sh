#!/usr/bin/env bash
# Local smoke test without Docker: start the app on a scratch dir, run the
# demo script against it, then shut the server down.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
if ! "$PYTHON" -c "import flask" >/dev/null 2>&1; then
  echo "error: $PYTHON lacks Flask; set PYTHON=.venv/bin/python" >&2
  exit 1
fi
DATA=$(mktemp -d)
PORT=${PORT:-8123}
"$PYTHON" -m artifact_repo --data-dir "$DATA" --host 127.0.0.1 --port "$PORT" \
  >"$DATA/server.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER 2>/dev/null || true; rm -rf "$DATA"' EXIT
for _ in $(seq 1 50); do
  if "$PYTHON" - "$PORT" <<'EOF' >/dev/null 2>&1
import sys, urllib.request
urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/api/state", timeout=1)
EOF
  then break; fi
  sleep 0.2
done
"$PYTHON" scripts/demo.py "http://127.0.0.1:$PORT"
echo
echo "server log tail:"
tail -5 "$DATA/server.log"
