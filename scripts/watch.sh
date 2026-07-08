#!/usr/bin/env bash
# ponytail: live dashboard from last run log
set -euo pipefail

LOG=$(ls -t ./target/run-logs/*.log 2>/dev/null | head -1)
if [ -z "$LOG" ]; then
    echo "no run logs found in ./target/run-logs/"
    exit 1
fi

echo "=== rpbot dashboard ==="
echo "log: $LOG"
echo ""

# Uptime
FIRST=$(grep -m1 '^{' "$LOG" | grep -oP '"ts":\K[0-9]+' || echo "0")
LAST=$(tail -1 "$LOG" | grep -oP '"ts":\K[0-9]+' || echo "0")
UPTIME=$(( (LAST - FIRST) / 1000 ))
echo "uptime: $(date -d@"$UPTIME" -u +%H:%M:%S)"

# Restarts
echo "restarts: $(grep -c 'starting bot' "$LOG" 2>/dev/null || true)"

# Pipeline survival
echo ""
echo "--- pipeline ---"
grep 'pipeline totals' "$LOG" | tail -1 | grep -oP '"msg":"[^"]+' | sed 's/"msg":"//' || echo "(waiting for first pipeline snapshot)"

# HF status
echo ""
echo "--- HF ticks (last 5) ---"
grep 'hf tick:' "$LOG" | tail -5 | grep -oP '"msg":"[^"]+' | sed 's/"msg":"//' || echo "(waiting for HF ticks)"

# Assess failures
echo ""
echo "--- assessment ---"
grep 'assess failed\|near-miss' "$LOG" | tail -3 | grep -oP '"msg":"[^"]+' | sed 's/"msg":"//' || echo "(none)"

# Dispatch
echo ""
echo "--- dispatch ---"
grep 'dispatch' "$LOG" | tail -3 | grep -oP '"msg":"[^"]+' | sed 's/"msg":"//' || echo "(none)"

# Errors in last 5 min
echo ""
echo "--- recent errors ---"
NOW=$(date +%s)
grep '"lvl":"ERROR\|"lvl":"WARN' "$LOG" 2>/dev/null | tail -5 | grep -oP '"ts":\K[0-9]+|"msg":"[^"]+' | paste - - | while read ts msg; do
    ts_sec=$(( ts / 1000 ))
    age=$(( NOW - ts_sec ))
    echo "[${age}s ago] $msg"
done || echo "(none)"
echo ""
echo "--- tail ---"
tail -3 "$LOG" | grep -oP '"msg":"[^"]+' | sed 's/"msg":"//' || true
