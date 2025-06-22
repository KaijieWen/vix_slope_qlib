"""
Build EOD & intraday feature sets. Skips gracefully if raw data missing.
"""
import glob, os, pandas as pd, numpy as np
from util import CFG, ensure_dirs, log
from util import log
log(f"=== ENTER {__file__} ===")

"""
feature_engineering.py
───────────────────────────────────────────────────────────────────────────────
• Builds EOD features with *both* 5-day and 10-day direction labels:
      TARGET_5D, TARGET_10D
• Builds intraday minute-bar features for SPY.
• Skips gracefully if raw data missing.
"""
import glob, os, pandas as pd, numpy as np
from util import CFG, ensure_dirs, log

ensure_dirs()
def z(s,w=20): return (s-s.rolling(w).mean())/s.rolling(w).std()

# ── EOD ──────────────────────────────────────────────────────────────
def build_eod():
    raw = [f'{CFG["paths"]["raw"]}{t}.parquet' for t in CFG["symbols"]]
    if not all(os.path.exists(f) and os.path.getsize(f)>100 for f in raw):
        log("EOD build skipped – missing raw files", 30); return
    dfs = {
        t: pd.read_parquet(f)[["Adj Close"]].rename(columns={"Adj Close": t.upper()})
        for t,f in zip(CFG["symbols"], raw)
    }
    df = pd.concat(dfs.values(), axis=1).dropna()
    # spreads and vol
    df["S1"]      = df["VXZ"] - df["VIXY"]
    df["S1_Z"]    = z(df["S1"])
    df["S1_PCT"]  = df["S1"].pct_change()
    df["RV5"]     = np.log(df["SPY"]).diff().rolling(5).std()*np.sqrt(252)
    # targets
    df["TARGET_5D"]  = np.sign(df["SPY"].shift(-5)  / df["SPY"] - 1).replace(0,np.nan)
    df["TARGET_10D"] = np.sign(df["SPY"].shift(-10) / df["SPY"] - 1).replace(0,np.nan)
    df.dropna(inplace=True)
    dst=f'{CFG["paths"]["ready"]}dataset_eod.parquet'
    df.to_parquet(dst)
    log(f"EOD set → {dst} ({len(df):,} rows)")

# ── intraday (SPY) ───────────────────────────────────────────────────
def build_intraday(symbol="SPY", horizon=10):
    pat=f'{CFG["paths"]["raw"]}{symbol}_*.parquet'
    files=sorted(glob.glob(pat))
    if not files:
        log("Intraday build skipped – no minute files",30); return
    parts=[]
    for fp in files:
        df=pd.read_parquet(fp)
        col="close" if "close" in df.columns else "Close"
        s=df[col]; s.name=symbol; parts.append(s)
        # concat along rows (axis=0)
    ser = pd.concat(parts, axis=0).sort_index()

    # ── NEW: if concat returned a DataFrame instead of Series, squeeze it ──
    if isinstance(ser, pd.DataFrame):
        ser = ser.iloc[:, 0]          # keep first (only) column

    ser = ser[~ser.index.duplicated(keep="last")]

    df = ser.to_frame()
    df["RET1"]    = ser.pct_change()
    df["MA10"]    = ser.rolling(10).mean() / ser - 1
    df["ATR10"]   = (ser.rolling(10).max() - ser.rolling(10).min()) / ser.shift(1)
    df["RET_FWD"] = ser.shift(-horizon) / ser - 1
    df.dropna(inplace=True)
    dst=f'{CFG["paths"]["ready"]}dataset_intraday_{symbol}.parquet'
    df.to_parquet(dst)
    log(f"Intraday set → {dst} ({len(df):,} rows)")

if __name__ == "__main__":
    build_eod()
    build_intraday("SPY")


log(f"=== EXIT  {__file__} ===")