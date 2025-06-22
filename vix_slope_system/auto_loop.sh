#!/usr/bin/env bash
###############################################################################
#  auto_loop.sh  –  grind-train while market closed, 15-min cadence when open
###############################################################################
set -euo pipefail

ROOT="/Users/wan/Desktop/vix_slope_system"
PY="/Users/wan/miniconda3/bin/python"

LOG="$ROOT/reports/auto_loop.log"
ERR="$ROOT/reports/auto_loop_err.log"

INTERVAL=15      # 15-minute intraday cadence while NYSE open
STEP=15           # 15-second heartbeat / grind pause

timestamp() { date '+%F %T'; }
log() { printf '%s | %s\n' "$(timestamp)" "$1" | tee -a "$LOG" | \
        logger -t com.vix.loop; }

# ── NYSE open? (UTC-safe) ────────────────────────────────────────────────────
is_open() {
  "$PY" - <<'PY'
import pandas_market_calendars as mcal, datetime as dt, pytz, sys
now=dt.datetime.now(tz=pytz.UTC)
ny=mcal.get_calendar("NYSE")
s=ny.schedule(now.date(), now.date())
if s.empty: sys.exit(1)
o,c=s.iloc[0][['market_open','market_close']]
sys.exit(0 if o<=now<=c else 1)
PY
}

# ── run a block of modules with banners ─────────────────────────────────────
run_block() {
  local title="$1"; shift
  local mods=("$@"); local tot=${#mods[@]}
  local idx=1
  for mod in "${mods[@]}"; do
      log "Stage ${idx}/${tot} – ${title}: ${mod}.py"
      $PY - <<PY 2>>"$ERR" >>"$LOG"
import importlib, util
util.log(f">>> ${mod}.py"); importlib.import_module("${mod}"); util.log(f"<<< ${mod}.py")
PY
      idx=$((idx+1))
  done
}

# ── MAIN LOOP ───────────────────────────────────────────────────────────────
log "🟢 auto_loop launched – intraday every $((INTERVAL/60)) min"

while true; do
  if is_open; then
      ################################################################# OPEN
      cd "$ROOT"
      run_block "Intraday" \
        data_etl_intraday  feature_engineering  live_trade_intraday
      sleep_for=$INTERVAL
  else
      ############################################################### CLOSED
      cd "$ROOT"
      run_block "Nightly-loop" \
        data_etl  feature_engineering  train_backtest  live_predict
      sleep_for=$STEP            # grind: run again after 15 s
  fi

  # countdown
  remain=$sleep_for
  while (( remain > 0 )); do
      printf '\rSleeping %3ds …' "$remain" | tee -a "$LOG"
      sleep "$STEP"
      remain=$((remain - STEP))
  done
  printf '\r%*s\r' 30 '' | tee -a "$LOG"   # clear line
done
