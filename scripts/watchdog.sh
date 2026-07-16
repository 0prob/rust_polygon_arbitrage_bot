#!/usr/bin/env bash
# Restart rpbot if it exits. Logs to target/run-logs/watchdog.log
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
LOG_DIR="$ROOT/target/run-logs"
mkdir -p "$LOG_DIR"
WD_LOG="$LOG_DIR/watchdog.log"
BOT_BIN="$ROOT/target/release/rpbot"
BOT_LOG_ROOT=${RPBOT_LOG_DIR:-/tmp/bot}

log() { echo "$(date -Iseconds) $*" >> "$WD_LOG"; }

log "watchdog started (bin=$BOT_BIN, jsonl_root=$BOT_LOG_ROOT)"
while true; do
  if ! pgrep -f "^${BOT_BIN}$" >/dev/null 2>&1; then
    AGENT_LOG="$LOG_DIR/agent-$(date +%Y%m%d-%H%M%S).log"
    log "rpbot not running — restarting -> $AGENT_LOG"
    nohup env RPBOT_LOG=info RPBOT_LOG_DIR="$BOT_LOG_ROOT" EXECUTION_MODE=dry-run \
      "$BOT_BIN" >> "$AGENT_LOG" 2>&1 &
    log "rpbot pid=$!"
  fi
  sleep 30
done