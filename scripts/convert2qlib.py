import os, glob, pandas as pd

RAW_DIR = "sample_data"
QLIB_DIR = "qlib_data/daily"
os.makedirs(QLIB_DIR, exist_ok=True)

calendar = set()
for fp in glob.glob(f"{RAW_DIR}/*.parquet"):
    sym = os.path.basename(fp).split(".")[0].upper()
    df = pd.read_parquet(fp)
    df.columns = [c.lower() for c in df.columns]
    df.index = pd.to_datetime(df["datetime"])
    df = df[["open", "high", "low", "close", "volume"]].sort_index()
    df.to_parquet(f"{QLIB_DIR}/{sym}.parquet")
    calendar.update(df.index)

with open(f"{QLIB_DIR}/calendar.txt", "w") as fh:
    for ts in sorted(calendar):
        fh.write(ts.strftime("%Y-%m-%d") + "\n")

print("✅  wrote Qlib store to", QLIB_DIR)
