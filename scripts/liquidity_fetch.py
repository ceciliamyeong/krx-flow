# scripts/liquidity_fetch.py
import datetime as dt
import pandas as pd
from pykrx import stock


INDEX_TICKER = {
    "KOSPI": "1001",
    "KOSDAQ": "2001",
}


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _pick_any_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"missing columns: {candidates} / got: {list(df.columns)}")


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    market: 'KOSPI' or 'KOSDAQ'
    output: date, market, close, turnover
    """
    s = _to_ymd(start)
    e = _to_ymd(end)

    # 1) Index OHLCV by *ticker* (avoid pykrx '지수명' path)
    ticker = INDEX_TICKER[market]
    idx = stock.get_index_ohlcv_by_date(s, e, ticker)
    idx = idx.reset_index()

    date_col = _pick_any_col(idx, ["날짜", "Date", "date"])
    close_col = _pick_any_col(idx, ["종가", "Close", "close"])

    idx_out = pd.DataFrame(
        {
            "date": pd.to_datetime(idx[date_col]).dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(idx[close_col], errors="coerce"),
        }
    )

    # 2) Market trading value by date (turnover)
    tv = stock.get_market_trading_value_by_date(s, e, market=market)
    tv = tv.reset_index()

    tv_date_col = _pick_any_col(tv, ["날짜", "Date", "date"])
    turnover_col = _pick_any_col(
        tv,
        [
            "거래대금",
            "거래대금(원)",
            "거래대금합계",
            "TRADING_VALUE",
            "trading_value",
            "turnover",
        ],
    )

    tv_out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[tv_date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )

    out = (
        idx_out.merge(tv_out, on=["date", "market"], how="outer")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
