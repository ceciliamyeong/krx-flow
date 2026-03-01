from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: build_dashboard_close-fixed-2026-03-01")

import json
from pathlib import Path
from typing import Dict, Any, Optional, List
import pandas as pd
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

# ✅ 2월 27일 데이터 강제
FORCE_CLOSE_DATE = "2026-02-27"

def ensure_dirs():
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)
    OUT_CHART.mkdir(parents=True, exist_ok=True)

def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")

def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    return df.columns[0] if not df.empty else ""

# ✅ 이전 영업일 찾기 로직 강화
def prev_business_day(date_str: str) -> str:
    d = pd.to_datetime(date_str).date()
    start = (d - pd.Timedelta(days=20)).strftime("%Y%m%d")
    end = to_krx_date(date_str)
    try:
        days = stock.get_previous_business_days(fromdate=start, todate=end)
        if not days or len(days) < 2:
            return (d - pd.Timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")
        return to_dash_date(days[-2])
    except:
        return (d - pd.Timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")

# ✅ 시가총액/수익률 데이터 가공 (함수 시그니처 수정)
def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    d = to_krx_date(date_str)
    prev_str = prev_business_day(date_str)
    pd_str = to_krx_date(prev_str)

    # get_market_cap_by_ticker는 보통 (date, market)
    cap = stock.get_market_cap_by_ticker(d, market=market)
    # get_market_ohlcv_by_ticker는 (from, to, market)으로 호출해야 안전
    prev_ohlcv = stock.get_market_ohlcv_by_ticker(pd_str, pd_str, market=market)

    if cap.empty or prev_ohlcv.empty:
        raise RuntimeError(f"Data empty for {date_str} or {prev_str}")

    mcap_col = _pick_col(cap, ["시가총액", "Market Cap"])
    close_col = _pick_col(cap, ["종가", "Close"])
    prev_close_col = _pick_col(prev_ohlcv, ["종가", "Close"])

    df = cap[[close_col, mcap_col]].copy().rename(columns={close_col: "close", mcap_col: "mcap"})
    df["ticker"] = df.index.astype(str)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)
    
    prev_close = prev_ohlcv[[prev_close_col]].copy().rename(columns={prev_close_col: "prev_close"})
    df = df.merge(prev_close, left_index=True, right_index=True, how="left")
    
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    return df.sort_values("mcap", ascending=False).head(10)

def make_treemap_png(df_top10: pd.DataFrame, title: str, outpath: Path) -> None:
    if df_top10.empty: return
    plt.figure(figsize=(10, 6))
    sizes = df_top10["mcap"].tolist()
    labels = [f"{r['name']}\n{r['return_1d']:+.2f}%" for _, r in df_top10.iterrows()]
    colors = ['#ff9999' if r['return_1d'] > 0 else '#66b3ff' if r['return_1d'] < 0 else '#d3d3d3' for _, r in df_top10.iterrows()]
    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.8)
    plt.title(title)
    plt.axis("off")
    plt.savefig(outpath, dpi=150)
    plt.close()

def main():
    ensure_dirs()
    date_str = FORCE_CLOSE_DATE
    print(f"📊 Building Dashboard for: {date_str}")

    dashboard: Dict[str, Any] = {"date": date_str, "extras": {"top10_treemap": {}, "treemap_png": {}}}

    for mk in ["KOSPI", "KOSDAQ"]:
        try:
            df_top10 = fetch_top10_mcap_and_return(date_str, mk)
            dashboard["extras"]["top10_treemap"][mk] = df_top10.to_dict(orient="records")
            
            img_path = OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png"
            make_treemap_png(df_top10, f"{mk} TOP 10 ({date_str})", img_path)
            dashboard["extras"]["treemap_png"][mk] = str(img_path.relative_to(ROOT))
        except Exception as e:
            dashboard["extras"][f"{mk}_error"] = str(e)

    latest_path = OUT_BASE / "latest.json"
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"✅ Dashboard creation finished. JSON saved at {latest_path}")

if __name__ == "__main__":
    main()
