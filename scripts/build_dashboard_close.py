from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: build_dashboard_close-v3-NAT-FIX-2026-03-02")

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import re
import requests
from pykrx import stock

# headless backend for CI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import squarify

ROOT = Path(__file__).resolve().parents[1]
HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INV_PIVOT = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
INV_LONG = ROOT / "data" / "derived" / "investor_flow_daily.csv"
OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"

# --- 핵심 수정 부분: 데이터 로드 및 날짜 필터링 ---

def load_liq_df() -> pd.DataFrame:
    if not HIST_LIQ.exists():
        raise FileNotFoundError(f"Missing {HIST_LIQ}")
    liq = pd.read_csv(HIST_LIQ)
    if "date" not in liq.columns:
        raise KeyError(f"{HIST_LIQ} missing date column")

    # [수정] NaT(시간아님) 및 빈 날짜 행을 즉시 제거하여 전파 방지
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce")
    liq = liq.dropna(subset=["date"])
    
    liq["date"] = liq["date"].dt.date.astype(str)
    liq["market"] = liq["market"].astype(str)
    liq["turnover_krw"] = pd.to_numeric(liq.get("turnover_krw"), errors="coerce")
    liq["close"] = pd.to_numeric(liq.get("close"), errors="coerce")
    
    return liq.sort_values(["date", "market"]).reset_index(drop=True)

# ------------------------------------------------

def fetch_top10_from_naver(market: str) -> pd.DataFrame:
    from io import StringIO
    sosok = "0" if market.upper() == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1"
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    }
    r = requests.get(url, headers=headers, timeout=20)
    r.encoding = "euc-kr"
    tables = pd.read_html(StringIO(r.text), match="종목명")
    df = tables[0].dropna(subset=["종목명"]).copy()
    
    def to_num(x):
        s = str(x).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-]", "", s)
        return pd.to_numeric(s, errors="coerce")

    close_col = next((c for c in df.columns if c in ["현재가", "종가"]), None)
    mcap_col  = next((c for c in df.columns if "시가총액" in str(c)), None)
    ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)

    out = pd.DataFrame()
    out["ticker"] = "" # [수정] null 방지
    out["name"] = df["종목명"].astype(str)
    out["close"] = df[close_col].map(to_num)
    out["mcap"] = df[mcap_col].map(to_num) * 1e8
    out["return_1d"] = df[ret_col].map(to_num) if ret_col else 0.0
    return out.dropna(subset=["mcap"]).sort_values("mcap", ascending=False).head(10).reset_index(drop=True)

def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    try:
        d = str(date_str).replace("-", "")
        df = stock.get_market_cap_by_ticker(d, market=market)
        if df is None or df.empty: raise RuntimeError("empty")
        close_col = _pick_col(df, ["종가", "현재가"])
        mcap_col  = _pick_col(df, ["시가총액"])
        ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)
        df = df.sort_values(mcap_col, ascending=False).head(10).copy()
        df["ticker"] = df.index.astype(str)
        df["name"] = df["ticker"].map(stock.get_market_ticker_name)
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df["mcap"] = pd.to_numeric(df[mcap_col], errors="coerce")
        df["return_1d"] = pd.to_numeric(df[ret_col], errors="coerce") if ret_col else 0.0
        return df[["ticker", "name", "close", "mcap", "return_1d"]].reset_index(drop=True)
    except:
        return fetch_top10_from_naver(market)

def ensure_dirs():
    for p in [OUT_BASE, OUT_ARCHIVE, OUT_CHART]: p.mkdir(parents=True, exist_ok=True)

def krw_readable(x):
    if x is None or pd.isna(x): return None
    v = float(x)
    a = abs(v)
    if a >= 1e12: return f"{v/1e12:+.2f}조"
    if a >= 1e8: return f"{v/1e8:+.0f}억"
    return f"{v:+.0f}"

def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    raise KeyError(f"Missing {candidates}")

def signal_label(ratio):
    if ratio is None or pd.isna(ratio): return None
    r = float(ratio)
    if abs(r) < 0.02: return "WEAK"
    if r > 0.05: return "STRONG_BUY"
    if r > 0: return "NORMAL_BUY"
    if r < -0.05: return "STRONG_SELL"
    return "NORMAL_SELL"

def prev_business_day(date_str):
    d = pd.to_datetime(date_str)
    try:
        days = stock.get_previous_business_days(fromdate=(d - pd.Timedelta(days=10)).strftime("%Y%m%d"), todate=d.strftime("%Y%m%d"))
        return pd.to_datetime(days[-2]).strftime("%Y-%m-%d")
    except:
        return (d - pd.Timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")

def fetch_upjong_top_bottom3_from_naver():
    url = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
    r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=20)
    r.encoding = "euc-kr"
    from io import StringIO
    df = pd.read_html(StringIO(r.text))[0].dropna(subset=["업종명"])
    ret_col = next(c for c in df.columns if "전일대비" in str(c))
    df["return_pct"] = pd.to_numeric(df[ret_col].astype(str).str.replace("%", "").str.extract(r'([-\d\.]+)')[0], errors="coerce")
    top = df.sort_values("return_pct", ascending=False).head(3)
    bot = df.sort_values("return_pct", ascending=True).head(3)
    return {
        "top": [{"name": str(r["업종명"]), "return_pct": float(r["return_pct"])} for _, r in top.iterrows()],
        "bottom": [{"name": str(r["업종명"]), "return_pct": float(r["return_pct"])} for _, r in bot.iterrows()]
    }

def make_treemap_png(df, title, outpath, market):
    plt.figure(figsize=(10, 6))
    sizes = df["mcap"].clip(lower=1).tolist()
    labels = [f"{r['name']}\n{r['return_1d']:+.2f}%" for _, r in df.iterrows()]
    colors = ["#ffcccc" if r > 0 else "#ccccff" for r in df["return_1d"]]
    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.8)
    plt.title(title)
    plt.axis("off")
    plt.savefig(outpath, dpi=150)
    plt.close()

def fetch_volatility_top5(date_str, market):
    p = prev_business_day(date_str).replace("-", "")
    t = date_str.replace("-", "")
    curr = stock.get_market_ohlcv_by_ticker(t, market=market)
    old = stock.get_market_ohlcv_by_ticker(p, market=market)
    df = curr[["종가"]].rename(columns={"종가": "close"})
    df["prev"] = old["종가"]
    df["return_1d"] = (df["close"] / df["prev"] - 1) * 100
    df = df.dropna().sort_values(by="return_1d", key=abs, ascending=False).head(5)
    df["name"] = df.index.map(stock.get_market_ticker_name)
    return [{"name": str(r["name"]), "return_1d": float(r["return_1d"])} for _, r in df.iterrows()]

def fetch_breadth(date_str, market):
    t = date_str.replace("-", "")
    df = stock.get_market_ohlcv_by_ticker(t, market=market)
    # pykrx의 등락률 컬럼 직접 활용 (없으면 계산)
    if "등락률" in df.columns:
        rets = df["등락률"]
    else:
        p = prev_business_day(date_str).replace("-", "")
        old = stock.get_market_ohlcv_by_ticker(p, market=market)
        rets = (df["종가"] / old["종가"] - 1) * 100
    
    return {
        "adv": int((rets > 0).sum()),
        "dec": int((rets < 0).sum()),
        "unch": int((rets == 0).sum()),
        "total": int(len(rets))
    }

def main():
    ensure_dirs()
    liq = load_liq_df()
    
    # [수정] NaT가 아닌 유효한 날짜 중 최신값 선택
    valid_dates = sorted([d for d in liq["date"].unique() if d and d != "NaT"])
    if not valid_dates:
        print("Error: No valid dates.")
        return
    date_str = valid_dates[-1]
    
    liq_day = liq[liq["date"] == date_str]
    markets = {}
    for _, r in liq_day.iterrows():
        mk = r["market"]
        markets[mk] = {
            "close": float(r["close"]),
            "turnover_readable": krw_readable(r["turnover_krw"]),
            "investor_net_krw": {"foreign": None, "institution": None, "individual": None},
            "investor_net_readable": {"foreign": None, "institution": None, "individual": None},
            "flow_signal": {"foreign": None, "institution": None, "individual": None}
        }

    dashboard = {
        "date": date_str,
        "version": "1.0",
        "markets": markets,
        "extras": {
            "top10_treemap": {"KOSPI": [], "KOSDAQ": []},
            "upjong": fetch_upjong_top_bottom3_from_naver(),
            "volatility_top5": {m: fetch_volatility_top5(date_str, m) for m in ["KOSPI", "KOSDAQ"]},
            "breadth": {m: fetch_breadth(date_str, m) for m in ["KOSPI", "KOSDAQ"]}
        }
    }

    for mk in ["KOSPI", "KOSDAQ"]:
        df_top = fetch_top10_mcap_and_return(date_str, mk)
        dashboard["extras"]["top10_treemap"][mk] = df_top.to_dict(orient="records")
        make_treemap_png(df_top, f"{mk} TOP10", OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png", mk)

    latest_path = OUT_BASE / "latest.json"
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Done:", date_str)

if __name__ == "__main__":
    main()
