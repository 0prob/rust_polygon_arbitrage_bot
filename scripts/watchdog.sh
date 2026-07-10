#!/usr/bin/env bash
# Restart rpbot if it exits. Logs to target/run-logs/watchdog.log
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT=$(pwd)
LOG_DIR="$ROOT/target/run-logs"
mkdir -p "$LOG_DIR"
WD_LOG="$LOG_DIR/watchdog.log"

log() { echo "$(date -Iseconds) $*" >> "$WD_LOG"; }

log "watchdog started"
while true; do
  if ! pgrep -x rpbot >/dev/null; then
    AGENT_LOG="$LOG_DIR/agent-$(date +%Y%m%d-%H%M%S).log"
    log "rpbot not running — restarting -> $AGENT_LOG"
    nohup env RPBOT_LOG=info EXECUTION_MODE=dry-run "$ROOT/target/release/rpbot" >> "$AGENT_LOG" 2>&1 &
    log "rpbot pid=$!"
  fi
  sleep 30
done