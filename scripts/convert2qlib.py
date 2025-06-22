"""
Convert every *.parquet in sample_data/ → Qlib store under qlib_data/daily/.
Handles any common date column name or datetime index.
"""
import os, glob, pandas as pd

RAW_DIR = "sample_data"
OUT_DIR = "qlib_data/daily"
os.makedirs(OUT_DIR, exist_ok=True)

# columns we’ll keep (rename to lower-case later)
KEEP = ["open", "high", "low", "close", "volume"]

calendar = set()
for fp in glob.glob(f"{RAW_DIR}/*.parquet"):
    sym = os.path.basename(fp).split(".")[0].upper()
    df  = pd.read_parquet(fp)

    # --- locate the datetime column or index ---
    dt_col = None
    for cand in ["datetime", "date", "timestamp"]:
        if cand in df.columns:
            dt_col = cand
            break

    if dt_col:
        df.index = pd.to_datetime(df[dt_col])
    else:                                  # assume current index is time
        df.index = pd.to_datetime(df.index)

    # --- standardise columns ---
    df.columns = [c.lower() for c in df.columns]
    df = df[KEEP].sort_index()

    # --- write parquet ---
    df.to_parquet(f"{OUT_DIR}/{sym}.parquet")
    calendar.update(df.index)

# --- write calendar.txt ---
with open(f"{OUT_DIR}/calendar.txt", "w") as fh:
    for ts in sorted(calendar):
        fh.write(ts.strftime("%Y-%m-%d") + "\n")

print("✅  Qlib store written to", OUT_DIR, "with", len(calendar), "trading days")
