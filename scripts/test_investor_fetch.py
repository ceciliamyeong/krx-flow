import datetime as dt
from pykrx import stock
import pandas as pd

today = dt.date.today()
start = today - dt.timedelta(days=60)

for market in ["KOSPI", "KOSDAQ"]:
    print(f"\n=== {market} ===")

    df = stock.get_market_trading_value_by_investor(
        start.strftime("%Y%m%d"),
        today.strftime("%Y%m%d"),
        market=market
    )

    df = df.reset_index()

    print(df.tail())
