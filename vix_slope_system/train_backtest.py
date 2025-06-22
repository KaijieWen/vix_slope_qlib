"""
train_backtest.py
───────────────────────────────────────────────────────────────────────────────
1. Train two daily classifiers:
     • 5-day direction  (TARGET_5D)
     • 10-day direction (TARGET_10D)

2. Train intraday 0.20 / 0.80 quantile regressors on minute features.

3. Run a full walk-forward back-test (daily) and append win-rate to reports.

All outputs go into models/ and reports/.  Any ±Inf / NaN rows are dropped
before fitting.  Progress is shown with tqdm so auto_loop logs % complete.
"""
from util import CFG, log
log(f"=== ENTER {__file__} ===")

# ── imports ─────────────────────────────────────────────────────────
import os, joblib, numpy as np, pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier, LGBMRegressor
from tqdm import tqdm

# ── helper funcs ────────────────────────────────────────────────────
def clean(df: pd.DataFrame, cols_keep) -> pd.DataFrame:
    return (df.replace([np.inf, -np.inf], np.nan)
              .dropna(subset=cols_keep))

def adaptive_tscv(df, test_days=252, max_splits=6):
    n = len(df)
    possible = max(0, (n - test_days) // test_days)
    splits   = min(max_splits, possible)
    if splits >= 2:
        tscv = TimeSeriesSplit(n_splits=splits, test_size=test_days)
        for tr, te in tscv.split(df):
            yield df.iloc[tr], df.iloc[te]
    else:
        train, test = df.iloc[: n-test_days], df.iloc[n-test_days :]
        yield train, test

def scale_fit(X):
    scaler = StandardScaler().fit(X)
    return scaler, scaler.transform(X)

# ── part 1 – daily classifiers ─────────────────────────────────────
def train_daily(target_col, out_name):
    df = pd.read_parquet(f'{CFG["paths"]["ready"]}dataset_eod.parquet')
    feats = [c for c in df.columns if c not in ("SPY","QQQ",
                                               "TARGET_5D","TARGET_10D")]
    df = clean(df, feats + [target_col])
    if df.empty:
        log(f"Daily ({out_name}): no data after cleaning – abort", level=40)
        return

    X, y = df[feats], df[target_col]
    scaler, Xs = scale_fit(X)

    model = LGBMClassifier(num_leaves=31, random_state=42, verbosity=-1)
    scores = []
    for tr, te in adaptive_tscv(df):
        Xtr, Xte = scaler.transform(tr[feats]), scaler.transform(te[feats])
        ytr, yte = tr[target_col], te[target_col]
        model.fit(Xtr, ytr)
        p = model.predict_proba(Xte)[:,1]
        scores.append(roc_auc_score(yte, p))
    log(f"{out_name} AUCs: {np.round(scores,3).tolist()}  mean={np.mean(scores):.3f}")

    model.fit(Xs, y)
    joblib.dump((scaler, model), f'{CFG["paths"]["model"]}{out_name}.pkl')
    log(f"Saved → models/{out_name}.pkl")

    # append metrics
    metr = f'{CFG["paths"]["reports"]}metrics_log.csv'
    pd.DataFrame([{
        "timestamp": pd.Timestamp.utcnow(),
        "model": out_name,
        "rows": len(df),
        "auc_mean": np.mean(scores)
    }]).to_csv(metr, mode="a", header=not os.path.exists(metr), index=False)

# ── part 2 – intraday quantile regressors ──────────────────────────
def train_intraday(symbol="SPY", horizon=10):
    path = f'{CFG["paths"]["ready"]}dataset_intraday_{symbol}.parquet'
    if not os.path.exists(path):
        log("Intraday dataset missing – skip regs", level=30)
        return
    df = pd.read_parquet(path)
    feats = [c for c in df.columns if c not in ("RET_FWD", symbol)]
    df = clean(df, feats + ["RET_FWD"])
    if df.empty:
        log("Intraday: no data after cleaning – abort", level=40)
        return

    X, y = df[feats], df["RET_FWD"]
    scaler, Xs = scale_fit(X)
    regs = {
        "lo": LGBMRegressor(objective="quantile", alpha=0.2,
                            random_state=42, verbosity=-1),
        "hi": LGBMRegressor(objective="quantile", alpha=0.8,
                            random_state=42, verbosity=-1)
    }
    for tag, reg in regs.items():
        reg.fit(Xs, y)
        joblib.dump((scaler, reg),
            f'{CFG["paths"]["model"]}{symbol.lower()}_reg_{tag}.pkl')
        log(f"Saved intraday reg ({tag})")

# ── part 3 – full walk-forward back-test for win-rate ───────────────
def walkforward_backtest():
    df = pd.read_parquet(f'{CFG["paths"]["ready"]}dataset_eod.parquet')
    feats = [c for c in df.columns if c not in ("SPY","QQQ",
                                               "TARGET_5D","TARGET_10D")]
    df = clean(df, feats + ["TARGET_5D"])
    if len(df) < 300:
        log("WF back-test: not enough rows", level=30); return

    wins=trades=0
    start=252
    for i in tqdm(range(start, len(df)-5),
                  desc="Walk-forward", ncols=70, ascii=True):
        train, test = df.iloc[:i], df.iloc[i:i+1]
        scaler = StandardScaler().fit(train[feats])
        model  = LGBMClassifier(num_leaves=31, verbosity=-1).fit(
                    scaler.transform(train[feats]), train["TARGET_5D"])
        prob_up = model.predict_proba(scaler.transform(test[feats]))[0,1]
        pred = 1 if prob_up>0.6 else -1 if prob_up<0.4 else 0
        real = int(test["TARGET_5D"].iloc[0])
        if pred!=0:
            trades += 1
            if pred==real: wins += 1
    win_rate = wins/trades if trades else 0
    wl = f'{CFG["paths"]["reports"]}winrate_log.csv'
    pd.DataFrame([{
        "timestamp": pd.Timestamp.utcnow(),
        "rows": len(df),
        "trades": trades,
        "wins": wins,
        "win_rate": win_rate
    }]).to_csv(wl, mode="a", header=not os.path.exists(wl), index=False)
    log(f"Walk-forward win-rate {win_rate:.2%} ({wins}/{trades})")

# ── run everything ──────────────────────────────────────────────────
if __name__ == "__main__":
    # ensure TARGET_10D exists – add once to feature file if missing
    fe_path = f'{CFG["paths"]["ready"]}dataset_eod.parquet'
    df_eod = pd.read_parquet(fe_path)
    if "TARGET_10D" not in df_eod.columns:
        df_eod["TARGET_10D"] = np.sign(df_eod["SPY"].shift(-10)/df_eod["SPY"]-1)\
                               .replace(0,np.nan)
        df_eod.to_parquet(fe_path)

    train_daily("TARGET_5D",  "daily_clf_5d")
    train_daily("TARGET_10D", "daily_clf_10d")
    train_intraday("SPY", horizon=10)
    walkforward_backtest()

log(f"=== EXIT  {__file__} ===")
