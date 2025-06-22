"""
Build EOD & intraday feature sets for the VIX-slope project.
Calling this module *as a script* will write Parquet datasets, but simply
importing it is now side-effect-free.
"""
import glob, os, pandas as pd, numpy as np
from util import CFG, ensure_dirs, log

# ---------- helpers -------------------------------------------------
def z(s, w=20):
    """z-score over |w| periods"""
    return (s - s.rolling(w).mean()) / s.rolling(w).std()

# ---------- EOD -----------------------------------------------------
def build_eod():
    """
    Create daily feature table with:
      S1, S1_Z, S1_PCT, RV5, TARGET_5D, TARGET_10D
    and save to dataset_eod.parquet
    """
    raw_files = [f'{CFG["paths"]["raw"]}{t}.parquet' for t in CFG["symbols"]]
    if not all(os.path.exists(f) and os.path.getsize(f) > 100 for f in raw_files):
        log("EOD build skipped – missing raw files", 30)
        return

    dfs = {
        t: pd.read_parquet(f)[["Adj Close"]].rename(columns={"Adj Close": t.upper()})
        for t, f in zip(CFG["symbols"], raw_files)
    }
    df = pd.concat(dfs.values(), axis=1).dropna()

    df["S1"]      = df["VXZ"] - df["VIXY"]
    df["S1_Z"]    = z(df["S1"])
    df["S1_PCT"]  = df["S1"].pct_change()
    df["RV5"]     = np.log(df["SPY"]).diff().rolling(5).std() * np.sqrt(252)

    df["TARGET_5D"]  = np.sign(df["SPY"].shift(-5)  / df["SPY"] - 1).replace(0, np.nan)
    df["TARGET_10D"] = np.sign(df["SPY"].shift(-10) / df["SPY"] - 1).replace(0, np.nan)
    df.dropna(inplace=True)

    dst = f'{CFG["paths"]["ready"]}dataset_eod.parquet'
    df.to_parquet(dst)
    log(f"EOD set → {dst} ({len(df):,} rows)")

# ---------- intraday (SPY) ------------------------------------------
def build_intraday(symbol: str = "SPY", horizon: int = 10):
    """
    Build minute-bar feature table with:
      RET1, MA10, ATR10, RET_FWD
    """
    pat = f'{CFG["paths"]["raw"]}{symbol}_*.parquet'
    files = sorted(glob.glob(pat))
    if not files:
        log("Intraday build skipped – no minute files", 30)
        return

    parts = [pd.read_parquet(fp) for fp in files]
    ser   = pd.concat([p["close" if "close" in p.columns else "Close"] for p in parts])
    ser   = ser[~ser.index.duplicated("last")]

    df = ser.to_frame(name=symbol)
    df["RET1"]    = ser.pct_change()
    df["MA10"]    = ser.rolling(10).mean() / ser - 1
    df["ATR10"]   = (ser.rolling(10).max() - ser.rolling(10).min()) / ser.shift(1)
    df["RET_FWD"] = ser.shift(-horizon) / ser - 1
    df.dropna(inplace=True)

    dst = f'{CFG["paths"]["ready"]}dataset_intraday_{symbol}.parquet'
    df.to_parquet(dst)
    log(f"Intraday set → {dst} ({len(df):,} rows)")

# ---------- CLI entry-point -----------------------------------------
if __name__ == "__main__":
    ensure_dirs()
    log(f"=== ENTER {__file__} ===")
    build_eod()
    build_intraday("SPY")
    log(f"=== EXIT  {__file__} ===")
