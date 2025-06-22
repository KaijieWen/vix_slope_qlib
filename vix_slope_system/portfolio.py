import pandas as pd, datetime as dt, json, os
from util import CFG
from util import CFG
START_CASH     = CFG["portfolio"]["start_cash"]
MAX_DAY_TRADES = CFG["portfolio"]["max_day_trades"]

STATE = f'{CFG["paths"]["reports"]}equity_curve.csv'
START_CASH = 10_000          # change to whatever “funding” you want
MAX_DAY_TRADES = 3           # Robinhood PDT cap per 5-trading-day window

def _init():
    if not os.path.exists(STATE):
        df = pd.DataFrame(
            [{"timestamp": dt.datetime.utcnow(), "cash": START_CASH,
              "pos": 0, "nav": START_CASH, "day_trades": 0}])
        df.to_csv(STATE, index=False)

def _rolling_day_trades(df):
    """Return # of round-trips in last 5 trading days."""
    five = df.tail(5 * 390)         # crude: 390 minutes ~ 1 RTH session
    return five["day_trades"].sum()

def book_trade(side, price, timestamp, qty=1):
    """
    side = 'BUY' or 'SELL'
    qty  = number of shares of SPY we notional-trade
    """
    _init()
    df = pd.read_csv(STATE, parse_dates=["timestamp"])
    cash, pos = df.iloc[-1][["cash", "pos"]]

    if side == "BUY":
        cost = price * qty
        if cash < cost:
            return "SKIP – insufficient cash"
        pos += qty
        cash -= cost
    else:   # SELL
        if pos < qty:
            return "SKIP – no inventory"
        pos -= qty
        cash += price * qty

    # PDT check: count round-trips
    day_trades = 0
    if side == "SELL" and pos == 0:         # closed same-day position
        if _rolling_day_trades(df) >= MAX_DAY_TRADES:
            return "SKIP – PDT cap"
        day_trades = 1

    nav = cash + pos * price
    df.loc[len(df)] = [timestamp, cash, pos, nav, day_trades]
    df.to_csv(STATE, index=False)
    return f"EXECUTED {side} {qty}@{price:.2f}  NAV={nav:.2f}"
