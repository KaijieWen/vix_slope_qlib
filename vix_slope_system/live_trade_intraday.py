"""
Called after every intraday data pull; prints buy/short zone & advice.
"""
import os, joblib, pandas as pd
from util import CFG, log
from portfolio import book_trade
from util import log
log(f"=== ENTER {__file__} ===")

SYMBOL = "SPY"      # can extend to loop over symbols later
HORIZ  = 10         # forward-return horizon, in minutes

# ── helper ──────────────────────────────────────────────────────────
def load(tag):
    return joblib.load(
        f'{CFG["paths"]["model"]}{SYMBOL.lower()}_reg_{tag}.pkl')

def latest_winrate() -> float:
    p = f'{CFG["paths"]["reports"]}winrate_log.csv'
    if not os.path.exists(p):
        return 0.5
    return pd.read_csv(p)["win_rate"].iloc[-1]

# ── main ────────────────────────────────────────────────────────────
def main():
    df = pd.read_parquet(
        f'{CFG["paths"]["ready"]}dataset_intraday_{SYMBOL}.parquet')
    latest = df.tail(1)
    price_now = latest[SYMBOL].iloc[0]
    feats = [c for c in latest.columns if c not in ("RET_FWD", SYMBOL)]

    scaler_hi, reg_hi = load("hi")
    scaler_lo, reg_lo = load("lo")
    X_hi = scaler_hi.transform(latest[feats])
    X_lo = scaler_lo.transform(latest[feats])

    ret_hi = reg_hi.predict(X_hi)[0]   # 80-percentile
    ret_lo = reg_lo.predict(X_lo)[0]   # 20-percentile

    tgt_hi = price_now * (1 + ret_hi)
    tgt_lo = price_now * (1 + ret_lo)

    ts = latest.index[-1].tz_convert("America/New_York")\
                         .strftime("%Y-%m-%d %H:%M")
    log(f"{ts}  {SYMBOL}={price_now:.2f}  → zone {tgt_lo:.2f}-{tgt_hi:.2f}")

    conf = latest_winrate()            # historical win-rate as confidence
    if ret_hi > 0.002 and ret_lo > 0:
        advice = (f"Buy {SYMBOL} {price_now:.2f} now ({ts}), "
                  f"target ≥{tgt_hi:.2f} within {HORIZ} min – conf {conf:.0%}")
        res = book_trade("BUY", price_now, ts)
    elif ret_lo < -0.002 and ret_hi < 0:
        advice = (f"Short {SYMBOL} {price_now:.2f} now, "
                  f"cover ≤{tgt_lo:.2f} within {HORIZ} min – conf {conf:.0%}")
        res = book_trade("SELL", price_now, ts)
    else:
        advice = f"No clear edge – hold (conf {conf:.0%})"
        res = "no-trade"

    log("⇢ " + advice + "   [" + res + "]")

# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()
log(f"=== EXIT  {__file__} ===")