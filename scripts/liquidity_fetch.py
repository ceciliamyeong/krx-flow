# scripts/liquidity_fetch.py
import datetime as dt
import pandas as pd
from pykrx import stock


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of {candidates} found in columns={list(df.columns)}")


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    market: "KOSPI" or "KOSDAQ"
    returns columns: date, market, close, turnover
    """
    s = _to_ymd(start)
    e = _to_ymd(end)

    # 1) index OHLCV (close)
    idx = stock.get_index_ohlcv_by_date(s, e, market)
    idx = idx.reset_index()

    date_col = _pick_col(idx, ["날짜", "Date", "date"])
    close_col = _pick_col(idx, ["종가", "Close", "close"])

    # 2) market trading value by date (turnover)
    tv = stock.get_market_trading_value_by_date(s, e, market=market)
    tv = tv.reset_index()

    tv_date_col = _pick_col(tv, ["날짜", "Date", "date"])
    turnover_col = _pick_col(tv, ["거래대금", "TRADING_VALUE", "trading_value", "turnover"])

    out = pd.DataFrame({
        "date": pd.to_datetime(idx[date_col]).dt.date.astype(str),
        "market": market,
        "close": pd.to_numeric(idx[close_col], errors="coerce"),
    })

    tv_out = pd.DataFrame({
        "date": pd.to_datetime(tv[tv_date_col]).dt.date.astype(str),
        "market": market,
        "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
    })

    out = out.merge(tv_out, on=["date", "market"], how="outer").sort_values(["date", "market"])
    return out
