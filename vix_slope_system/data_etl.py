"""
Nightly end-of-day downloader using Polygon REST instead of Yahoo.
Run once per day after 16:10 ET.

• --mode backfill   → pulls full history from CFG['start_date'] to today
• --mode daily      → appends only today's bar   (default)
"""
from datetime import date, datetime as dt, timedelta
import argparse, os, pandas as pd
from polygon import RESTClient
from util import CFG, ensure_dirs, log
import sys
import pandas_market_calendars as mcal, pytz, datetime as dt
from util import log
log(f"=== ENTER {__file__} ===")
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
def fetch_polygon(symbol: str, start_iso: str) -> pd.DataFrame:
    """Return a DataFrame with an 'Adj Close' column indexed by UTC date."""
    cli = RESTClient(CFG["polygon_key"])
    bars = cli.list_aggs(
        symbol, 1, "day",
        from_=start_iso,
        to=date.today().isoformat(),
        limit=50_000
    )
    df = pd.DataFrame([b.__dict__ for b in bars])
    if df.empty:
        raise ValueError(f"Polygon returned 0 rows for {symbol}")
    df["t"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.set_index("t", inplace=True)
    return df.rename(columns={"close": "Adj Close"})[["Adj Close"]]

def main(mode: str = "daily") -> None:
    for tag, sym in CFG["symbols"].items():
        dst = f'{CFG["paths"]["raw"]}{tag}.parquet'
        try:
            if mode == "backfill" or not os.path.exists(dst):
                df = fetch_polygon(sym, CFG["start_date"])
            else:
                # fetch only the last two days (Polygon merges weekends)
                start = (date.today() - timedelta(days=3)).isoformat()
                df_new = fetch_polygon(sym, start)
                df_old = pd.read_parquet(dst) if os.path.exists(dst) else pd.DataFrame()
                df = pd.concat([df_old, df_new]).drop_duplicates()
            df.to_parquet(dst)
            log(f"{tag.upper()}: saved {len(df):,} rows → {dst}")
        except Exception as e:
            log(f"{tag.upper()} download FAILED: {e}", level=40)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=("backfill", "daily"), default="daily")
    main(p.parse_args().mode)

log(f"=== EXIT  {__file__} ===")