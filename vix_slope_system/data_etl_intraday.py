"""
Intraday 1-minute downloader.
• First tries Polygon; if plan lacks minute data, silently falls back to Yahoo
  (last ~30 calendar days available).
• Auto-selects the most recent NYSE session if --date is omitted.

Cron (15-min cadence during NY hours):
    */15 6-13 * * 1-5  cd /full/path/vix_slope_system && python data_etl_intraday.py
"""
from datetime import date, timedelta
import argparse, pandas as pd, yfinance as yf
from polygon import RESTClient, exceptions as pl_exc
import pandas_market_calendars as mcal
from util import CFG, ensure_dirs, log
import sys
import pandas_market_calendars as mcal, pytz, datetime as dt


def nyse_open_now() -> bool:
    """True iff right now (wall-clock) is inside today’s regular session."""
    ny = mcal.get_calendar("NYSE")
    now = dt.datetime.now(tz=pytz.UTC)
    sched = ny.schedule(start_date=now.date(), end_date=now.date())
    if sched.empty:      # weekend / holiday
        return False
    market_open, market_close = sched.iloc[0][['market_open', 'market_close']]
    return market_open <= now <= market_close

ensure_dirs()
if not nyse_open_now():
    log("Market closed – skipping fetch.")
    sys.exit(0)



poly = RESTClient(CFG["polygon_key"])

# ── helpers ──────────────────────────────────────────────────────────
def last_market_day() -> date:
    nyse = mcal.get_calendar("NYSE")
    sched = nyse.schedule(
        start_date=date.today() - timedelta(days=7),
        end_date=date.today()
    )
    return sched.index[-1].date()

# ---------- Polygon ----------
def polygon_minutes(symbol: str, day: date) -> pd.DataFrame:
    bars = poly.list_aggs(
        symbol, 1, "minute",
        from_=day.isoformat(), to=day.isoformat(), limit=50_000
    )
    df = pd.DataFrame([b.__dict__ for b in bars])
    if df.empty:
        return df
    df = df.rename(
        columns={"o": "open", "h": "high", "l": "low",
                 "c": "close", "v": "volume", "t": "timestamp"}
    )
    df["ts"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    return df[["open", "high", "low", "close", "volume"]]

# ---------- Yahoo fallback ----------
def yahoo_minutes(symbol: str, day: date) -> pd.DataFrame:
    df = yf.download(
        symbol,
        start=day.isoformat(),
        end=(day + timedelta(days=1)).isoformat(),
        interval="1m",
        progress=False,
        threads=False,
        auto_adjust=False,
    )
    if df.empty:
        return df
    # ensure UTC index
    if df.index.tz is None:
        df.index = df.index.tz_localize("America/New_York").tz_convert("UTC")
    else:
        df.index = df.index.tz_convert("UTC")
    df.index.name = "ts"
    df = df.rename(
        columns={"Open": "open", "High": "high", "Low": "low",
                 "Close": "close", "Volume": "volume"}
    )
    return df[["open", "high", "low", "close", "volume"]]

# ── main fetch wrapper ───────────────────────────────────────────────
def fetch_minutes(symbol: str, day: date) -> pd.DataFrame:
    try:
        df = polygon_minutes(symbol, day)
        if df.empty:
            raise ValueError("Polygon returned 0 rows")
        log(f"Polygon minute bars pulled ({len(df):,} rows)")
        return df
    except (pl_exc.BadResponse, Exception) as e:
        log(f"Polygon failed ({e}); switching to Yahoo", level=30)
        df = yahoo_minutes(symbol, day)
        if df.empty:
            raise RuntimeError("Yahoo also returned empty 1-minute data")
        log(f"Yahoo minute bars pulled ({len(df):,} rows)")
        return df

def main(symbol: str, day: date):
    df = fetch_minutes(symbol, day)
    dst = f'{CFG["paths"]["raw"]}{symbol}_{day}.parquet'
    df.to_parquet(dst)
    log(f"{symbol} {day}: stored {len(df):,} rows → {dst}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="SPY")
    ap.add_argument("--date", help="YYYY-MM-DD (omit → last market day)")
    args = ap.parse_args()
    d = date.fromisoformat(args.date) if args.date else last_market_day()
    main(args.symbol.upper(), d)
