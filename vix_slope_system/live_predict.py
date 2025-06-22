"""
live_predict.py  –  prints the latest 5-day and 10-day regime calls
"""
import joblib, pandas as pd
from util import CFG, log

def load_model(tag):
    """tag = '5d' or '10d'"""
    return joblib.load(f'{CFG["paths"]["model"]}daily_clf_{tag}.pkl')

def latest_row():
    df = pd.read_parquet(f'{CFG["paths"]["ready"]}dataset_eod.parquet')
    return df.tail(1)

def predict(tag, latest):
    scaler, model = load_model(tag)
    feats = [c for c in latest.columns
             if c not in ("SPY","QQQ","TARGET_5D","TARGET_10D")]
    X = scaler.transform(latest[feats])
    return model.predict_proba(X)[0,1]

def main():
    row = latest_row()
    ts  = row.index[-1].strftime("%Y-%m-%d")
    p5  = predict("5d",  row)
    p10 = predict("10d", row)
    dir5  = "LONG" if p5>0.6 else "SHORT" if p5<0.4 else "FLAT"
    dir10 = "LONG" if p10>0.6 else "SHORT" if p10<0.4 else "FLAT"

    log(f"{ts} 5-day P(up)={p5:.1%} → {dir5}")
    log(f"{ts} 10-day P(up)={p10:.1%} → {dir10}")
    print(f"{ts}\n  5-day : {dir5}  ({p5:.1%})\n 10-day : {dir10} ({p10:.1%})")

if __name__ == "__main__":
    log(f"=== ENTER {__file__} ===")
    main()
    log(f"=== EXIT  {__file__} ===")
