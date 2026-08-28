#!/bin/bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
DAEMON="$DIR/shrink-preheat-daemon"
CLIENT="$DIR/test_client"
SOCK="/tmp/shrink_preheat_test.sock"
DPID=""

cleanup() {
    if [[ -n "$DPID" ]] && kill -0 "$DPID" 2>/dev/null; then
        kill "$DPID" 2>/dev/null || true
        wait "$DPID" 2>/dev/null || true
    fi
    rm -f "$SOCK"
}
trap cleanup EXIT INT TERM

rm -f "$SOCK"
echo "=== Starting daemon (17 slots x 32MB) ==="
"$DAEMON" --slots=17 --slot-size=32M --sock="$SOCK" &
DPID=$!
sleep 2

echo ""
echo "=== Running test client ==="
if "$CLIENT"; then
    RC=0
else
    RC=$?
fi

echo ""
echo "=== Stopping daemon (PID=$DPID) ==="
kill "$DPID" 2>/dev/null
wait "$DPID" 2>/dev/null || true
DPID=""

echo ""
if [ $RC -eq 0 ]; then
    echo ">>> TEST PASSED <<<"
else
    echo ">>> TEST FAILED (rc=$RC) <<<"
fi
exit $RC
