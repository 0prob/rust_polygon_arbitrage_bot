#!/usr/bin/env bash
# ponytail: ~50 lines, zero deps — grep | tail is the dashboard
set -euo pipefail
shopt -s nullglob

cd "$(dirname "$0")/.."
LOG_DIR="./target/run-logs"
mkdir -p "$LOG_DIR"
PIDFILE="$LOG_DIR/runner.pid"

if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "[runner] already running (pid=$(cat "$PIDFILE"))"
    exit 1
fi
echo $$ >"$PIDFILE"
trap 'rm -f "$PIDFILE"' EXIT INT TERM

RUN_LOG="$LOG_DIR/run-$(date +%Y%m%d-%H%M%S).log"
BOT_BIN="./target/release/rpbot"
BACKOFF=2
MAX_BACKOFF=60
RESTART_COUNT=0
START_TS=$(date +%s)
BOT_LOG_ROOT=${RPBOT_LOG_DIR:-/tmp/bot}

cleanup() {
    echo "[runner] shutting down..." | tee -a "$RUN_LOG"
    exit 0
}
trap cleanup SIGINT SIGTERM

echo "[runner] starting rpbot continuous run (log=$RUN_LOG)" | tee -a "$RUN_LOG"

while true; do
    echo "[runner] starting bot (restart #$RESTART_COUNT)..." >> "$RUN_LOG"

    # Run with timeout protection (1 hour max per run, the main loop handles disconnects internally)
    if RPBOT_LOG=info RPBOT_LOG_DIR="$BOT_LOG_ROOT" EXECUTION_MODE=dry-run \
        timeout 3600 "$BOT_BIN" >> "$RUN_LOG" 2>&1; then
        EXIT_CODE=$?
        echo "[runner] bot exited normally (code=$EXIT_CODE)" >> "$RUN_LOG"
    else
        EXIT_CODE=$?
        echo "[runner] bot crashed (code=$EXIT_CODE)" >> "$RUN_LOG"
    fi

    RESTART_COUNT=$((RESTART_COUNT + 1))
    UPTIME=$(( $(date +%s) - START_TS ))

    # Extract key stats from last run
    echo "=== LAST RUN SUMMARY ===" >> "$RUN_LOG"
    echo "runtime: $(date -d@"$UPTIME" -u +%H:%M:%S)" >> "$RUN_LOG"
    echo "restarts: $RESTART_COUNT" >> "$RUN_LOG"

    # Pipeline survival (latest)
    grep 'pipeline totals' "$RUN_LOG" | tail -1 >> "$RUN_LOG" 2>/dev/null || true
    # HF ticks with profitable
    grep 'hf tick:' "$RUN_LOG" | tail -3 >> "$RUN_LOG" 2>/dev/null || true
    # Assess failures
    grep 'assess failed\|near-miss' "$RUN_LOG" | tail -3 >> "$RUN_LOG" 2>/dev/null || true
    # Dispatch
    grep 'dispatch' "$RUN_LOG" | tail -3 >> "$RUN_LOG" 2>/dev/null || true
    LATEST_COMPONENT_RUN=$(find "$BOT_LOG_ROOT" -mindepth 1 -maxdepth 1 -type d -name 'run-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | head -1 | cut -d' ' -f2- || true)
    COMPONENT_LOGS=("$LATEST_COMPONENT_RUN"/*.jsonl)
    if [ "${#COMPONENT_LOGS[@]}" -eq 0 ]; then
        echo "warnings: 0" >> "$RUN_LOG"
        echo "errors: 0" >> "$RUN_LOG"
    else
        grep -h -c '"level":"WARN"' "${COMPONENT_LOGS[@]}" 2>/dev/null | awk '{s+=$1} END {print "warnings: " s+0}' >> "$RUN_LOG" || true
        grep -h -c '"level":"ERROR"' "${COMPONENT_LOGS[@]}" 2>/dev/null | awk '{s+=$1} END {print "errors: " s+0}' >> "$RUN_LOG" || true
    fi

    echo "========================" >> "$RUN_LOG"

    # Exponential backoff before restart
    echo "[runner] restarting in ${BACKOFF}s..." >> "$RUN_LOG"
    sleep "$BACKOFF"
    BACKOFF=$(( BACKOFF * 2 ))
    if [ "$BACKOFF" -gt "$MAX_BACKOFF" ]; then
        BACKOFF=$MAX_BACKOFF
    fi
done
