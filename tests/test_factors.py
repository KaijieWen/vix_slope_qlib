import numpy as np, pandas as pd, qlib
from vix_provider.vix_provider import VixProvider
from vix_provider.features import FEATURES

qlib.init(provider=VixProvider("./qlib_data"), verbose=False)

START, END = "2023-04-01", "2023-06-30"
INSTRS      = ["SPY", "VIXY", "VXZ"]

# ---------------------------------------------------------------------
def legacy_frame():
    """
    Re-create the legacy factor table for SPY + spread factors.
    """
    # pull close price for all three instruments
    close = (
        qlib.data.D.features(INSTRS, ["$close"],
                             start_time=START, end_time=END)["$close"]
          .unstack("instrument")               # columns = instruments
          .rename(columns=str.upper)           # SPY / VIXY / VXZ
    )

    df = pd.DataFrame({
        "SPY":  close["SPY"],
        "VIXY": close["VIXY"],
        "VXZ":  close["VXZ"],
    }).dropna()

    # spread and z-score
    df["S1"]     = df["VXZ"] - df["VIXY"]
    df["S1_Z"]   = (df["S1"] - df["S1"].rolling(20).mean()) / df["S1"].rolling(20).std()
    df["S1_PCT"] = df["S1"].pct_change()
    df["RV5"]    = np.log(df["SPY"]).diff().rolling(5).std()*np.sqrt(252)
    df["TARGET_5D"]  = np.sign(df["SPY"].shift(-5)  / df["SPY"] - 1).replace(0, np.nan)
    df["TARGET_10D"] = np.sign(df["SPY"].shift(-10) / df["SPY"] - 1).replace(0, np.nan)
    return df.dropna()

legacy = legacy_frame()

# ---------------------------------------------------------------------
def test_factor_parity():
    """
    DSL expressions in vix_provider.features must reproduce legacy values.
    """
    for fname, expr in FEATURES.items():
        # skip intraday-only factors in this daily test
        if fname in ["RET1", "MA10", "ATR10", "RET_FWD"]:
            continue

        new = (
            qlib.data.D.features(["SPY"], [expr], start_time=START, end_time=END)
              .xs("SPY")            # drop instrument level
              .iloc[:, 0]           # series
        )
        old = legacy[fname].reindex_like(new)
        assert np.allclose(old, new, equal_nan=True, atol=1e-8), fname
