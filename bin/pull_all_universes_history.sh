#!/bin/bash
# Pulls historical data for all large, mid, and small cap stocks from 2010 to today
# Usage: ./exec/pull_all_universes_history.sh

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLOW_SCRIPT="$PROJECT_ROOT/flow/history_data_pull.py"
CONFIG_DIR="$PROJECT_ROOT/config/market_cap"
START_DATE="2010-01-01"
END_DATE="$(date +%Y-%m-%d)"
LOG_DIR="$PROJECT_ROOT/logs/exec"
mkdir -p "$LOG_DIR"

SEGMENTS=(
  "large_cap:nifty_100_large_cap.json"
  "mid_cap:nifty_midcap_selected.json"
  "small_cap:nifty_smallcap_selected.json"
)

for entry in "${SEGMENTS[@]}"; do
  IFS=":" read -r segment config_file <<< "$entry"
  echo "[INFO] Pulling $segment data using $config_file..."
  python3 "$FLOW_SCRIPT" \
    --config "$CONFIG_DIR/$config_file" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --output-format local \
    --market-cap-segment "$segment" \
    2>&1 | tee -a "$LOG_DIR/history_pull_${segment}_$(date +%Y%m%d_%H%M%S).log"
  echo "[INFO] Finished $segment."
done

echo "[INFO] All universes pulled. Logs in $LOG_DIR."
