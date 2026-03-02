from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: build_dashboard_close-v3-FULL-INTEGRATED-2026-03-02")

import json
from pathlib import Path
from typing import Dict, Any, Optional, List

import pandas as pd
import re
import requests
from pykrx import stock
from io import StringIO

# headless backend for CI
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import squarify


ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"

# ✅ 우선 pivot을 읽고, 없으면 long-form을 읽어서 pivot 생성
INV_PIVOT = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
INV_LONG = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"


# ---------------------------------------------------------
# ✅ 사용자 수정본 반영: Naver Top10 수집 (인코딩 & StringIO 완벽 반영)
# ---------------------------------------------------------
def fetch_top10_from_naver(market: str) -> pd.DataFrame:
    import re
    import requests
    import pandas as pd
    from io import StringIO

    sosok = "0" if market.upper() == "KOSPI" else "1"
    url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}&page=1"
    headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
      "Referer": "https://finance.naver.com/",
      "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()

    # 🔥 사용자 수정 포인트: CI 인코딩 깨짐 방지
    r.encoding = "euc-kr"

    tables = pd.read_html(StringIO(r.text), match="종목명")
    if not tables:
        raise RuntimeError("Naver parse failed: no table matched '종목명'")

    df = tables[0].copy()
    df = df.dropna(subset=["종목명"]).copy()
    
    def to_num(x):
        s = str(x).replace(",", "").strip()
        s = re.sub(r"[^\d\.\-]", "", s)
        return pd.to_numeric(s, errors="coerce")

    close_col = "현재가" if "현재가" in df.columns else ("종가" if "종가" in df.columns else None)
    mcap_col  = next((c for c in df.columns if "시가총액" in str(c)), None)
    ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)

    if close_col is None or mcap_col is None:
        raise RuntimeError(f"Naver table missing cols. columns={list(df.columns)}")

    out = pd.DataFrame()
    out["ticker"] = ""
    out["name"] = df["종목명"].astype(str)
    out["close"] = df[close_col].map(to_num)
    out["mcap"] = df[mcap_col].map(to_num) * 1e8
    out["return_1d"] = df[ret_col].map(to_num) if ret_col else 0.0

    out = out.dropna(subset=["mcap"]).sort_values("mcap", ascending=False).head(10).reset_index(drop=True)
    return out[["ticker", "name", "close", "mcap", "return_1d"]]


def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    try:
        d = to_krx_date(date_str)
        df = stock.get_market_cap_by_ticker(d, market=market)
        if df is None or df.empty:
            raise RuntimeError("pykrx empty")

        close_col = _pick_col(df, ["종가", "현재가", "Close"])
        mcap_col  = _pick_col(df, ["시가총액", "상장시가총액", "Market Cap"])
        ret_col   = next((c for c in df.columns if "등락률" in str(c)), None)

        df = df.sort_values(mcap_col, ascending=False).head(10).copy()
        df["ticker"] = df.index.astype(str)
        df["name"] = df["ticker"].map(stock.get_market_ticker_name)
        df["close"] = pd.to_numeric(df[close_col], errors="coerce")
        df["mcap"] = pd.to_numeric(df[mcap_col], errors="coerce")
        df["return_1d"] = pd.to_numeric(df[ret_col], errors="coerce") if ret_col else 0.0

        out = df[["ticker", "name", "close", "mcap", "return_1d"]]
        out = out.dropna(subset=["mcap"]).reset_index(drop=True)
        out["ticker"] = out["ticker"].fillna("").astype(str)

        if out.empty:
            raise RuntimeError("pykrx cleaned empty")

        return out

    except Exception as e:
        print(f"[Top10] pykrx failed -> fallback to Naver ({market})", e)
        return fetch_top10_from_naver(market)


# ---------------------------------------------------------
# ✅ Utils
# ---------------------------------------------------------
def ensure_dirs():
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)
    OUT_CHART.mkdir(parents=True, exist_ok=True)

def krw_readable(x: Optional[float]) -> Optional[str]:
    if x is None: return None
    try: v = float(x)
    except: return None
    a = abs(v)
    if a >= 1e12: return f"{v/1e12:+.2f}조"
    if a >= 1e8: return f"{v/1e8:+.0f}억"
    return f"{v:+.0f}"

def to_krx_date(s: str) -> str: return str(s).replace("-", "")

def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8: return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    cols = list(df.columns)
    for c in candidates:
        if c in cols: return c
    lower_map = {str(col).lower(): col for col in cols}
    for c in candidates:
        key = str(c).lower()
        if key in lower_map: return lower_map[key]
    for col in cols:
        col_l = str(col).lower()
        for c in candidates:
            if str(c).lower() in col_l: return col
    raise KeyError(f"Column not found: {candidates}")

def signal_label(ratio: Optional[float], strong: float = 0.05, normal: float = 0.02) -> Optional[str]:
    if ratio is None: return None
    r = float(ratio)
    a = abs(r)
    if a < normal: return "WEAK_BUY" if r > 0 else ("WEAK_SELL" if r < 0 else "WEAK")
    if a < strong: return "NORMAL_BUY" if r > 0 else "NORMAL_SELL"
    return "STRONG_BUY" if r > 0 else "STRONG_SELL"

def unit_mult(raw_hint: str) -> float:
    s = str(raw_hint)
    if "(십억원)" in s: return 1e9
    if "(억원)" in s: return 1e8
    if "(백만원)" in s: return 1e6
    if "(천원)" in s: return 1e3
    return 1.0

def norm_inv(x: str) -> str:
    s = str(x).strip()
    if not s: return s
    base = s.split("(")[0].strip()
    if "외국" in base: return "foreign"
    if "기관" in base or base in ["institution_total", "institution"]: return "institution"
    if "개인" in base: return "individual"
    if base in ["foreign", "foreigner", "foreign_total"]: return "foreign"
    if base in ["individual", "individual_total"]: return "individual"
    return base

def prev_business_day(date_str: str) -> str:
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    start = (d - timedelta(days=20)).strftime("%Y-%m-%d")
    try:
        days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(date_str))
        if not days or len(days) < 2:
            return (d - timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")
        return to_dash_date(days[-2])
    except:
        return (d - timedelta(days=1 if d.weekday() != 0 else 3)).strftime("%Y-%m-%d")


# ---------------------------------------------------------
# ✅ 사용자 수정본 반영: Naver Upjong 수집 (KeyError 방지 로직 완벽 반영)
# ---------------------------------------------------------
def fetch_upjong_top_bottom3_from_naver() -> Dict[str, List[Dict[str, Any]]]:
    import pandas as pd
    import re
    from io import StringIO

    url = "https://finance.naver.com/sise/sise_group.naver?type=upjong"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Referer": "https://finance.naver.com/",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }

    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    r.encoding = "euc-kr"  # ✅ 사용자 수정 포인트: 한글 깨짐 방지

    tables = pd.read_html(StringIO(r.text))
    if not tables:
        raise RuntimeError("Naver upjong parse failed: no table matched '업종명'")

    df = tables[0].copy()
    
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            (str(a).strip() if "Unnamed" not in str(a) else str(b).strip())
            if (str(b).strip() == "" or "Unnamed" in str(b))
            else str(b).strip()
            for a, b in df.columns.to_list()
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    
    # ✅ 사용자 수정 포인트: 업종명 컬럼 자동 탐색 후 dropna
    name_col = next((c for c in df.columns if "업종" in str(c)), None)
    if name_col is None:
        raise RuntimeError(f"Naver upjong: name col not found. columns={list(df.columns)}")
    
    df = df.dropna(subset=[name_col]).copy()
    df.rename(columns={name_col: "업종명"}, inplace=True)

    # ✅ 사용자 수정 포인트: 등락률 컬럼 찾기(보통 '전일대비')
    ret_col = None
    for c in df.columns:
        if "전일대비" in str(c) or "등락률" in str(c):
            ret_col = c
            break
    if ret_col is None:
        raise RuntimeError(f"Naver upjong table missing return col. columns={list(df.columns)}")

    def to_pct(x):
        s = str(x).strip().replace("%", "")
        s = re.sub(r"[^\d\.\-\+]", "", s)
        return pd.to_numeric(s, errors="coerce")

    df["return_pct"] = df[ret_col].map(to_pct)
    df = df.dropna(subset=["return_pct"]).copy()

    top = df.sort_values("return_pct", ascending=False).head(3)
    bottom = df.sort_values("return_pct", ascending=True).head(3)

    def pack(dd):
        return [{"name": str(r["업종명"]), "return_pct": float(r["return_pct"])} for _, r in dd.iterrows()]

    return {"top": pack(top), "bottom": pack(bottom)}


# ---------------------------------------------------------
# ✅ Extras: Treemap, Volatility, Breadth
# ---------------------------------------------------------
def make_treemap_png(df_top10: pd.DataFrame, title: str, outpath: Path, market: str = "") -> None:
    import matplotlib.colors as mcolors
    outpath.parent.mkdir(parents=True, exist_ok=True)
    if df_top10 is None or df_top10.empty: return
    df_top10 = df_top10.copy()
    df_top10["mcap"] = pd.to_numeric(df_top10["mcap"], errors="coerce")
    df_top10["return_1d"] = pd.to_numeric(df_top10.get("return_1d"), errors="coerce")
    df_top10 = df_top10.dropna(subset=["mcap"])
    df_top10 = df_top10[df_top10["mcap"] > 0]
    if df_top10.empty: return
    sizes = df_top10["mcap"].astype(float).tolist()
    rets = df_top10["return_1d"].fillna(0.0).astype(float).tolist()
    labels = [f"{r['name']}\n{float(r['return_1d']):+.2f}%" for _, r in df_top10.iterrows()]
    abs_rets = pd.Series([abs(x) for x in rets if pd.notna(x)])
    vmax = max(2.0, min(float(abs_rets.quantile(0.85)) if not abs_rets.empty else 7.0, 12.0))
    red_light, red_dark = "#FFD6D6", "#B00020"
    blue_light, blue_dark = "#D6E4FF", "#0033A0"
    def lerp(c1, c2, t):
        a, b = mcolors.to_rgb(c1), mcolors.to_rgb(c2)
        return tuple(a[i] * (1 - t) + b[i] * t for i in range(3))
    def ret_to_color(ret):
        if ret is None or pd.isna(ret): return "#DDDDDD"
        r = float(ret)
        t = min(abs(r) / vmax, 1.0)
        return lerp(red_light, red_dark, t) if r > 0 else (lerp(blue_light, blue_dark, t) if r < 0 else "#F2F2F2")
    colors = [ret_to_color(x) for x in rets]
    plt.figure(figsize=(10, 6))
    squarify.plot(sizes=sizes, label=labels, color=colors, alpha=0.95)
    plt.title(f"{title} (색상기준 ±{vmax:.1f}%)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(outpath, dpi=150)
    plt.close()

def fetch_volatility_top5(date_str: str, market: str) -> List[Dict[str, Any]]:
    prev_str = prev_business_day(date_str)
    today = stock.get_market_ohlcv_by_ticker(to_krx_date(date_str), market=market)
    prev = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if today is None or today.empty or prev is None or prev.empty: return []
    c_col = _pick_col(today, ["종가", "Close"])
    p_c_col = _pick_col(prev, ["종가", "Close"])
    df = today[[c_col]].copy().rename(columns={c_col: "close"})
    df["ticker"] = df.index.astype(str)
    p_df = prev[[p_c_col]].copy().rename(columns={p_c_col: "prev_close"})
    p_df["ticker"] = p_df.index.astype(str)
    df = df.merge(p_df.reset_index(drop=True), on="ticker", how="left")
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    df = df.dropna(subset=["return_1d"]).sort_values("return_1d", key=abs, ascending=False).head(5)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)
    return [{"ticker": str(r["ticker"]), "name": str(r["name"]), "return_1d": float(r["return_1d"]), "close": float(r["close"])} for _, r in df.iterrows()]

def fetch_breadth(date_str: str, market: str) -> Dict[str, Any]:
    prev_str = prev_business_day(date_str)
    today = stock.get_market_ohlcv_by_ticker(to_krx_date(date_str), market=market)
    prev = stock.get_market_ohlcv_by_ticker(to_krx_date(prev_str), market=market)
    if today is None or today.empty or prev is None or prev.empty: return {}
    c_col = _pick_col(today, ["종가", "Close"])
    p_c_col = _pick_col(prev, ["종가", "Close"])
    df = today[[c_col]].copy().rename(columns={c_col: "close"})
    p_df = prev[[p_c_col]].copy().rename(columns={p_c_col: "prev_close"})
    df = df.merge(p_df, left_index=True, right_index=True, how="left")
    df["return_1d"] = (df["close"] / df["prev_close"] - 1.0) * 100.0
    df = df.dropna(subset=["return_1d"])
    adv = int((df["return_1d"] > 0).sum()); dec = int((df["return_1d"] < 0).sum())
    total = len(df)
    return {"date": date_str, "market": market, "adv": adv, "dec": dec, "unch": total-adv-dec, "total": total, "adv_ratio": adv/total if total>0 else 0}


# ---------------------------------------------------------
# ✅ Data Load & Cleanup (NaT 방어 추가)
# ---------------------------------------------------------
def load_liq_df() -> pd.DataFrame:
    if not HIST_LIQ.exists(): raise FileNotFoundError(f"Missing {HIST_LIQ}")
    liq = pd.read_csv(HIST_LIQ)
    # 🔥 NaT 방어: 날짜 정제 로직 추가
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce")
    liq = liq.dropna(subset=["date"]).copy()
    liq["date"] = liq["date"].dt.date.astype(str)
    liq["market"] = liq["market"].astype(str)
    liq["turnover_krw"] = pd.to_numeric(liq.get("turnover_krw"), errors="coerce")
    liq["close"] = pd.to_numeric(liq.get("close"), errors="coerce")
    return liq.sort_values(["date", "market"]).reset_index(drop=True)

def load_inv_df() -> pd.DataFrame:
    if INV_PIVOT.exists():
        inv = pd.read_csv(INV_PIVOT)
        if not inv.empty:
            inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
            for c in ["foreign_net", "institution_net", "individual_net"]: inv[c] = pd.to_numeric(inv.get(c), errors="coerce")
            return inv.dropna(subset=["date"])
    return pd.DataFrame()


def load_index_rows(liq: pd.DataFrame, date_str: str) -> pd.DataFrame:
    day = liq[liq["date"] == date_str].copy()
    if day.empty: raise RuntimeError(f"No liquidity rows for date={date_str}")
    return day.sort_values(["market"]).reset_index(drop=True)


def build_market_cards(liq_day: pd.DataFrame, inv_day: pd.DataFrame) -> Dict[str, Any]:
    inv_map = {str(r["market"]): r.to_dict() for _, r in inv_day.iterrows()} if not inv_day.empty else {}
    markets = {}
    for _, r in liq_day.iterrows():
        mk = str(r["market"])
        turnover = float(r["turnover_krw"]) if pd.notna(r.get("turnover_krw")) else 0
        inv_row = inv_map.get(mk, {})
        f_net = float(inv_row.get("foreign_net", 0)) if pd.notna(inv_row.get("foreign_net")) else 0
        i_net = float(inv_row.get("institution_net", 0)) if pd.notna(inv_row.get("institution_net")) else 0
        d_net = float(inv_row.get("individual_net", 0)) if pd.notna(inv_row.get("individual_net")) else 0
        ratio_f = f_net/turnover if turnover > 0 else 0
        markets[mk] = {
            "close": float(r["close"]),
            "turnover_krw": turnover,
            "turnover_readable": krw_readable(turnover),
            "investor_net_krw": {"foreign": f_net, "institution": i_net, "individual": d_net},
            "investor_net_readable": {"foreign": krw_readable(f_net), "institution": krw_readable(i_net), "individual": krw_readable(d_net)},
            "investor_ratio": {"foreign": ratio_f},
            "flow_signal": {"foreign": signal_label(ratio_f)}
        }
    return markets


# ---------------------------------------------------------
# ✅ Main
# ---------------------------------------------------------
def main():
    ensure_dirs()
    liq = load_liq_df()
    inv = load_inv_df()

    # 🔥 NaT 제외 유효한 날짜 중 최신값 선택
    valid_dates = sorted([d for d in liq["date"].unique() if d and d != "NaT"])
    if not valid_dates: return
    date_str = valid_dates[-1]
    print("Dashboard date:", date_str)

    liq_day = load_index_rows(liq, date_str)
    inv_day = inv[inv["date"] == date_str].copy() if not inv.empty else pd.DataFrame()

    dashboard = {
        "date": date_str,
        "version": "1.0",
        "markets": build_market_cards(liq_day, inv_day),
        "extras": {
            "top10_treemap": {"KOSPI": [], "KOSDAQ": []},
            "treemap_png": {"KOSPI": "data/derived/charts/treemap_kospi_top10_latest.png", "KOSDAQ": "data/derived/charts/treemap_kosdaq_top10_latest.png"},
            "volatility_top5": {"KOSPI": [], "KOSDAQ": []},
            "breadth": {"KOSPI": {}, "KOSDAQ": {}},
            "upjong": fetch_upjong_top_bottom3_from_naver()
        }
    }

    for mk in ["KOSPI", "KOSDAQ"]:
        try:
            df_top = fetch_top10_mcap_and_return(date_str, mk)
            dashboard["extras"]["top10_treemap"][mk] = df_top.to_dict(orient="records")
            make_treemap_png(df_top, f"{mk} 시총 TOP10 - {date_str}", OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png", market=mk)
            dashboard["extras"]["volatility_top5"][mk] = fetch_volatility_top5(date_str, mk)
            dashboard["extras"]["breadth"][mk] = fetch_breadth(date_str, mk)
        except Exception as e:
            print(f"Error {mk}: {e}")

    import math
    def sanitize(obj):
        if isinstance(obj, float) and math.isnan(obj): return None
        if isinstance(obj, dict): return {k: sanitize(v) for k, v in obj.items()}
        if isinstance(obj, list): return [sanitize(v) for v in obj]
        return obj

    dashboard = sanitize(dashboard)
    (OUT_BASE / "latest.json").write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT_ARCHIVE / f"{date_str}.json").write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Dashboard Update Complete")

if __name__ == "__main__":
    main()
