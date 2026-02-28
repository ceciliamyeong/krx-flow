# scripts/liquidity_fetch.py
import ast
import datetime as dt
import time
import random

import pandas as pd
import requests
from pykrx import stock


NAVER_API = "https://api.finance.naver.com/siseJson.naver"
NAVER_SYMBOL = {
    "KOSPI": "KOSPI",
    "KOSDAQ": "KOSDAQ",
}


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _naver_fetch_index_close(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    Naver: returns OHLCV for index symbol (KOSPI/KOSDAQ)
    We use only date, close
    """
    params = {
        "symbol": NAVER_SYMBOL[market],
        "requestType": "1",
        "startTime": _to_ymd(start),
        "endTime": _to_ymd(end),
        "timeframe": "day",
    }

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://finance.naver.com/",
    }

    r = requests.get(NAVER_API, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    # Response looks like: [['날짜','시가','고가','저가','종가','거래량'], ['20220103',...], ...]
    text = r.text.strip()

    # 안전 파싱: js array → python literal 형태로 변환
    # (따옴표/공백 변형을 최대한 흡수)
    text = text.replace("\n", "").replace("\t", "").replace(" ", "")
    data = ast.literal_eval(text)

    if not data or len(data) < 2:
        return pd.DataFrame(columns=["date", "market", "close"])

    header = data[0]
    rows = data[1:]

    df = pd.DataFrame(rows, columns=header)

    # 컬럼명은 보통 '날짜','종가'
    date_col = "날짜" if "날짜" in df.columns else df.columns[0]
    close_col = "종가" if "종가" in df.columns else df.columns[4]

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], format="%Y%m%d").dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )
    return out


def _pykrx_fetch_market_turnover(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    pykrx: market total trading value by date
    """
    s = _to_ymd(start)
    e = _to_ymd(end)

    # 호출 간격(가끔 rate-limit 흉내)
    time.sleep(0.3 + random.random() * 0.4)

    tv = stock.get_market_trading_value_by_date(s, e, market=market).reset_index()

    # 보통: '날짜', '거래대금' 포함
    date_col = "날짜" if "날짜" in tv.columns else tv.columns[0]

    # 거래대금 컬럼 후보군
    for c in ["거래대금", "거래대금(원)", "거래대금합계", "TRADING_VALUE", "trading_value", "turnover"]:
        if c in tv.columns:
            turnover_col = c
            break
    else:
        raise KeyError(f"turnover column not found. cols={list(tv.columns)}")

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )
    return out


def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    market: 'KOSPI' or 'KOSDAQ'
    output: date, market, close, turnover
    """
    close_df = _naver_fetch_index_close(start, end, market)
    turn_df = _pykrx_fetch_market_turnover(start, end, market)

    out = (
        close_df.merge(turn_df, on=["date", "market"], how="outer")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
