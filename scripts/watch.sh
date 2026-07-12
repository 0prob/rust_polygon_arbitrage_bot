#!/usr/bin/env bash
# ponytail: live dashboard from last run log
set -euo pipefail
shopt -s nullglob

LOG_ROOT=${RPBOT_LOG_DIR:-/tmp/bot}
RUN_DIR=$(find "$LOG_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'run-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)
if [ -z "$RUN_DIR" ]; then
    echo "no component logs found in $LOG_ROOT"
    exit 1
fi
LOGS=("$RUN_DIR"/*.jsonl)
if [ "${#LOGS[@]}" -eq 0 ]; then
    echo "no component logs found in $RUN_DIR"
    exit 1
fi

echo "=== rpbot dashboard ==="
echo "log: $RUN_DIR"
echo ""

# Uptime
FIRST=$(grep -h -oP '"ts":\K[0-9]+' "${LOGS[@]}" | sort -n | head -1)
LAST=$(grep -h -oP '"ts":\K[0-9]+' "${LOGS[@]}" | sort -n | tail -1)
UPTIME=$(( (LAST - FIRST) / 1000 ))
echo "uptime: $(date -d@"$UPTIME" -u +%H:%M:%S)"

# Restarts
echo "run: $(basename "$RUN_DIR")"

# Pipeline survival
echo ""
echo "--- pipeline ---"
grep -h 'pipeline totals' "${LOGS[@]}" | tail -1 | jq -r .message || echo "(waiting for first pipeline snapshot)"

# HF status
echo ""
echo "--- HF ticks (last 5) ---"
grep -h 'hf tick:' "${LOGS[@]}" | tail -5 | jq -r .message || echo "(waiting for HF ticks)"

# Assess failures
echo ""
echo "--- assessment ---"
grep -h 'assess failed\|near-miss' "${LOGS[@]}" | tail -3 | jq -r .message || echo "(none)"

# Dispatch
echo ""
echo "--- dispatch ---"
grep -h 'dispatch' "${LOGS[@]}" | tail -3 | jq -r .message || echo "(none)"

# Errors in last 5 min
echo ""
echo "--- recent errors ---"
NOW=$(date +%s)
grep -h '"level":"ERROR\|"level":"WARN' "${LOGS[@]}" 2>/dev/null | sort -t: -k2,2n | tail -5 | jq -r '[.ts, .message] | @tsv' | while IFS=$'\t' read -r ts msg; do
    ts_sec=$(( ts / 1000 ))
    age=$(( NOW - ts_sec ))
    echo "[${age}s ago] $msg"
done || echo "(none)"
echo ""
echo "--- tail ---"
cat "${LOGS[@]}" | sort -t: -k2,2n | tail -3 | jq -r .message || true
