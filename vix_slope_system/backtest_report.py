import pandas as pd, plotly.express as px, webbrowser
from util import CFG

eq_path = f'{CFG["paths"]["reports"]}equity_curve.csv'
met_path = f'{CFG["paths"]["reports"]}metrics_log.csv'

df_eq  = pd.read_csv(eq_path, parse_dates=["timestamp"])
df_met = pd.read_csv(met_path, parse_dates=["timestamp"])

fig1 = px.line(df_eq, x="timestamp", y="nav", title="Equity Curve")
fig2 = px.line(df_met, x="timestamp", y="auc_mean", title="AUC over time")

html = "<h1>P/L Dashboard</h1>" + fig1.to_html(full_html=False) + fig2.to_html(full_html=False)
out = f'{CFG["paths"]["reports"]}dashboard.html'
with open(out, "w") as f: f.write(html)
webbrowser.open("file://" + out)
print("Dashboard opened:", out)
