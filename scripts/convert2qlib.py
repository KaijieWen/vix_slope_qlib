"""
Convert every *.parquet in sample_data/ → Qlib store under qlib_data/daily/.
Handles any common date column name or datetime index and tolerates
missing OHLCV columns (keeps what it finds).
"""
import os, glob, pandas as pd

RAW_DIR = "sample_data"              # put raw files here
OUT_DIR = "qlib_data/daily"
os.makedirs(OUT_DIR, exist_ok=True)

# preferred column set (lower-case)
OHLCV = ["open", "high", "low", "close", "volume"]

calendar = set()
for fp in glob.glob(f"{RAW_DIR}/*.parquet"):
    sym = os.path.basename(fp).split(".")[0].upper()
    df  = pd.read_parquet(fp)

    # --- locate date column or use index ---
    for cand in ["datetime", "date", "timestamp"]:
        if cand in df.columns:
            df.index = pd.to_datetime(df[cand])
            break
    else:  # no column found → treat current index as datetime
        df.index = pd.to_datetime(df.index)

    # --- standardise column names ---
    df.columns = [c.lower() for c in df.columns]

    # keep available OHLCV cols
    keep = [c for c in OHLCV if c in df.columns]
    if not keep:
        print(f"⚠️  {sym}: no OHLCV columns found → skipped")
        continue

    df = df[keep].sort_index()

    # write parquet
    df.to_parquet(f"{OUT_DIR}/{sym}.parquet")
    calendar.update(df.index)
    print(f"✅  wrote {sym}.parquet with {len(df)} rows")

# write calendar
with open(f"{OUT_DIR}/calendar.txt", "w") as fh:
    for ts in sorted(calendar):
        fh.write(ts.strftime('%Y-%m-%d') + '\n')

print("🗓  calendar.txt written with", len(calendar), "trading days")
