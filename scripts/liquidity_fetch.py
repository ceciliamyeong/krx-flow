# scripts/liquidity_fetch.py
import ast
import datetime as dt
import io
import random
import time
from typing import Optional

import pandas as pd
import requests
from pykrx import stock

# =========================
# 1) NAVER (close)
# =========================
NAVER_API = "https://api.finance.naver.com/siseJson.naver"
NAVER_SYMBOL = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ"}


def _to_ymd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _naver_fetch_index_close(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    params = {
        "symbol": NAVER_SYMBOL[market],
        "requestType": "1",
        "startTime": _to_ymd(start),
        "endTime": _to_ymd(end),
        "timeframe": "day",
    }
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://finance.naver.com/"}
    r = requests.get(NAVER_API, params=params, headers=headers, timeout=30)
    r.raise_for_status()

    text = r.text.strip().replace("\n", "").replace("\t", "")
    data = ast.literal_eval(text)

    if not data or len(data) < 2:
        return pd.DataFrame(columns=["date", "market", "close"])

    header = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=header)

    date_col = "날짜" if "날짜" in df.columns else df.columns[0]
    close_col = "종가" if "종가" in df.columns else df.columns[4]

    return pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col], format="%Y%m%d").dt.date.astype(str),
            "market": market,
            "close": pd.to_numeric(df[close_col], errors="coerce"),
        }
    )


# =========================
# 2) PYKRX (turnover) with retry + validation
# =========================
def _pick_turnover_col(cols: list[str]) -> Optional[str]:
    candidates = [
        "거래대금",
        "거래대금(원)",
        "거래대금합계",
        "TRADING_VALUE",
        "trading_value",
        "turnover",
    ]
    for c in candidates:
        if c in cols:
            return c
    return None


def _pykrx_fetch_market_turnover_once(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    s = _to_ymd(start)
    e = _to_ymd(end)

    # ✅ 버전 호환: market을 위치인자로 전달
    tv = stock.get_market_trading_value_by_date(s, e, market)

    # tv가 DF가 아닐 수 있어서 강제 변환
    if isinstance(tv, pd.Series):
        tv = tv.to_frame().T

    if not isinstance(tv, pd.DataFrame):
        return pd.DataFrame()

    tv = tv.reset_index()

    # 비정상 케이스: cols=['index']만 있고 나머지 없음
    if tv.shape[1] <= 1:
        return pd.DataFrame()

    date_col = "날짜" if "날짜" in tv.columns else ("Date" if "Date" in tv.columns else None)
    if date_col is None:
        # 첫 컬럼이 날짜인 경우가 흔함
        date_col = tv.columns[0]

    turnover_col = _pick_turnover_col(list(tv.columns))
    if turnover_col is None:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(tv[date_col]).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(tv[turnover_col], errors="coerce"),
        }
    )
    # 유효성: turnover가 전부 NaN이면 실패로 간주
    if out["turnover"].notna().sum() == 0:
        return pd.DataFrame()

    return out


def _pykrx_fetch_market_turnover(start: dt.date, end: dt.date, market: str, retries: int = 3) -> pd.DataFrame:
    for i in range(retries):
        # 레이트리밋/간헐 실패 완화
        time.sleep(0.4 + random.random() * 0.6)
        out = _pykrx_fetch_market_turnover_once(start, end, market)
        if not out.empty:
            return out
    return pd.DataFrame()


# =========================
# 3) KRX OTP fallback (turnover)
# =========================
GEN_OTP_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
DL_CSV_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"

# ✅ 여기만 너가 채우면 fallback이 완전해짐.
# 지금은 비워둬도 되고(그럼 fallback 없이 pykrx만 재시도), 채우면 완전 안정화.
KRX_BLD_TURNOVER = ""  # e.g. "dbms/MDC/STAT/standard/XXXXXXX"
KRX_REFERER = "https://data.krx.co.kr/"


def _krx_download_csv(bld: str, payload: dict) -> bytes:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": KRX_REFERER,
        "Origin": "https://data.krx.co.kr",
    }

    form = {"bld": bld}
    form.update(payload)

    r = requests.post(GEN_OTP_URL, data=form, headers=headers, timeout=30)
    r.raise_for_status()
    otp = r.text.strip()

    time.sleep(0.3 + random.random() * 0.5)

    r2 = requests.post(DL_CSV_URL, data={"code": otp}, headers=headers, timeout=30)
    r2.raise_for_status()
    return r2.content


def _krx_fetch_turnover_fallback(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    if not KRX_BLD_TURNOVER:
        # fallback 미설정이면 빈 DF 반환 (상위에서 처리)
        return pd.DataFrame()

    payload = {
        "locale": "ko_KR",
        "strtDd": _to_ymd(start),
        "endDd": _to_ymd(end),
        # 시장 코드: 화면에 따라 다를 수 있음 (필요하면 여기만 맞추면 됨)
        "mktId": "ALL" if market in ["KOSPI", "KOSDAQ"] else "ALL",
        "csvxls_isNo": "false",
    }

    raw = _krx_download_csv(KRX_BLD_TURNOVER, payload)
    text = raw.decode("cp949", errors="replace")
    df = pd.read_csv(io.StringIO(text))

    # 날짜/거래대금 컬럼 자동 탐색
    date_col = None
    for c in ["일자", "날짜", "TRD_DD", "TRD_DATE"]:
        if c in df.columns:
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]

    turnover_col = None
    for c in ["거래대금", "거래대금합계", "거래대금(원)", "TRD_VAL", "TRADING_VALUE"]:
        if c in df.columns:
            turnover_col = c
            break
    if turnover_col is None:
        return pd.DataFrame()

    out = pd.DataFrame(
        {
            "date": pd.to_datetime(df[date_col].astype(str)).dt.date.astype(str),
            "market": market,
            "turnover": pd.to_numeric(df[turnover_col], errors="coerce"),
        }
    )
    if out["turnover"].notna().sum() == 0:
        return pd.DataFrame()
    return out


# =========================
# Public API
# =========================
def fetch_liquidity_range(start: dt.date, end: dt.date, market: str) -> pd.DataFrame:
    """
    market: 'KOSPI' or 'KOSDAQ'
    output: date, market, close, turnover
    """
    close_df = _naver_fetch_index_close(start, end, market)

    # 1차: pykrx
    turn_df = _pykrx_fetch_market_turnover(start, end, market, retries=4)

    # 2차: fallback (KRX OTP 설정돼있으면)
    if turn_df.empty:
        turn_df = _krx_fetch_turnover_fallback(start, end, market)

    # 그래도 비면: turnover는 NaN으로 남기고 close만이라도 저장(파이프라인 죽지 않게)
    out = (
        close_df.merge(turn_df, on=["date", "market"], how="left")
        .sort_values(["date", "market"])
        .reset_index(drop=True)
    )
    return out
