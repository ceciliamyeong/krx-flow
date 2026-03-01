from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: run_daily-final-fix-2026-03-01")

import json
import inspect
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from pykrx import stock

# ✅ 1. 경로 설정 수정: scripts 폴더 안에 있으므로 부모 폴더를 ROOT로 설정
ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INVESTOR_LONG_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"
INVESTOR_PIVOT_CSV = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"
MERGED_CSV = ROOT / "data" / "derived" / "market_flow_daily.csv"
DERIVED_DIR = ROOT / "data" / "derived"
HISTORY_DIR = ROOT / "data" / "history"

# ✅ 최종 마감일 강제 (에러 로그에 찍힌 날짜 기준)
FORCE_CLOSE_DATE = "2026-02-27"
RAW_UNIT_HINT = "(십억원)"
MARKETS = ["KOSPI", "KOSDAQ"]

def ensure_dirs():
    DERIVED_DIR.mkdir(parents=True, exist_ok=True)
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)

def to_krx_date(s: str) -> str:
    return str(s).replace("-", "")

def to_dash_date(s: str) -> str:
    s = str(s)
    if "-" not in s and len(s) == 8:
        return f"{s[:4]}-{s[4:6]}-{s[6:]}"
    return s

def latest_business_day() -> str:
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    start = (today - timedelta(days=30)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(end))
    if not days:
        raise RuntimeError("Cannot determine business days from pykrx")
    return to_dash_date(days[-1])

def prev_business_day_safe(date_str: str, lookback_days: int = 60) -> Optional[str]:
    from datetime import datetime, timedelta
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None
    start = (d - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    try:
        days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(date_str))
    except Exception:
        return None
    if not days or len(days) < 2:
        return None
    return to_dash_date(days[-2])

def _unit_mult(hint: str) -> float:
    s = str(hint)
    if "(십억원)" in s: return 1e9 # KRX 기준 십억원은 10^9
    if "(억원)" in s: return 1e8
    if "(백만원)" in s: return 1e6
    if "(천원)" in s: return 1e3
    return 1.0

def _norm_inv(t: str) -> str:
    s = str(t).strip()
    if not s: return s
    base = s.split("(")[0].strip()
    if base in ["individual", "individual_total", "개인"]: return "individual"
    if base in ["foreign", "foreigner", "foreign_total", "외국인"]: return "foreign"
    if base in ["institution_total", "institution", "institutions", "기관합계"]: return "institution_total"
    return base

def _load_liquidity_history() -> pd.DataFrame:
    cols = ["date", "market", "turnover_krw", "close"]
    if not HIST_LIQ.exists(): return pd.DataFrame(columns=cols)
    df = pd.read_csv(HIST_LIQ)
    if df.empty: return pd.DataFrame(columns=cols)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["turnover_krw"] = pd.to_numeric(df.get("turnover_krw"), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    return df.dropna(subset=["date", "market"])

def _liquidity_day_from_history(date_str: str) -> pd.DataFrame:
    hist = _load_liquidity_history()
    day = hist[hist["date"] == date_str].copy()
    rows = []
    for mk in MARKETS:
        sub = day[day["market"] == mk]
        if not sub.empty:
            r = sub.iloc[-1].to_dict()
            rows.append({"date": date_str, "market": mk, "turnover_krw": r.get("turnover_krw"), "close": r.get("close")})
        else:
            rows.append({"date": date_str, "market": mk, "turnover_krw": pd.NA, "close": pd.NA})
    return pd.DataFrame(rows)

def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns: return c
    return df.columns[0] # 못 찾으면 첫 번째 컬럼이라도 반환

# ✅ 2. pykrx 함수 호출 인자 에러 수정
def _call_trading_value_by_investor(date_str: str, mk: str) -> pd.DataFrame:
    d = to_krx_date(date_str)
    fn = stock.get_market_trading_value_by_investor
    
    # 에러 로그에 따라 (시작일, 종료일, 시장) 형태를 최우선 시도
    try:
        return fn(d, d, mk)
    except TypeError:
        pass
    
    # 구버전 방식 (날짜, 시장)
    try:
        return fn(d, mk)
    except Exception as e:
        raise RuntimeError(f"pykrx 호출 실패 ({date_str}/{mk}): {e}")

def _fetch_investor_long(date_str: str) -> pd.DataFrame:
    rows = []
    for mk in MARKETS:
        df = _call_trading_value_by_investor(date_str, mk)
        if df is None or df.empty: continue

        buy_col = _pick_col(df, ["매수", "BUY", "매수금액"])
        sell_col = _pick_col(df, ["매도", "SELL", "매도금액"])
        net_col = _pick_col(df, ["순매수", "NET", "순매수금액"])

        for inv_name in df.index.astype(str).tolist():
            rows.append({
                "date": date_str,
                "market": mk,
                "investor_type": f"{inv_name}{RAW_UNIT_HINT}",
                "bid_raw": pd.to_numeric(df.loc[inv_name, buy_col], errors="coerce"),
                "ask_raw": pd.to_numeric(df.loc[inv_name, sell_col], errors="coerce"),
                "net_raw": pd.to_numeric(df.loc[inv_name, net_col], errors="coerce"),
                "raw_unit_hint": RAW_UNIT_HINT,
            })
    return pd.DataFrame(rows)

def _load_investor_long() -> pd.DataFrame:
    if not INVESTOR_LONG_CSV.exists(): return pd.DataFrame()
    df = pd.read_csv(INVESTOR_LONG_CSV)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    return df

def _upsert_investor_long(date_str: str) -> pd.DataFrame:
    hist = _load_investor_long()
    today = _fetch_investor_long(date_str)
    if not hist.empty:
        hist = hist[hist["date"] != date_str].copy()
    out = pd.concat([hist, today], ignore_index=True)
    out.to_csv(INVESTOR_LONG_CSV, index=False)
    return out

def _read_investor_pivot_from_long(inv_long: pd.DataFrame) -> pd.DataFrame:
    if inv_long.empty: return pd.DataFrame()
    inv = inv_long.copy()
    inv["investor_type_norm"] = inv["investor_type"].map(_norm_inv)
    inv["net_krw"] = inv["net_raw"] * _unit_mult(RAW_UNIT_HINT)
    
    keep = inv[inv["investor_type_norm"].isin(["individual", "foreign", "institution_total"])].copy()
    pivot = keep.pivot_table(index=["date", "market"], columns="investor_type_norm", values="net_krw", aggfunc="sum").reset_index()
    
    rename_map = {"individual": "individual_net", "foreign": "foreign_net", "institution_total": "institution_net"}
    return pivot.rename(columns=rename_map)

def _merge_investor(liquidity_day: pd.DataFrame, investor_pivot: pd.DataFrame) -> pd.DataFrame:
    if liquidity_day.empty: return liquidity_day
    out = liquidity_day.merge(investor_pivot, on=["date", "market"], how="left")
    
    denom = pd.to_numeric(out["turnover_krw"], errors="coerce").replace({0: pd.NA})
    for c in ["individual", "foreign", "institution"]:
        col = f"{c}_net"
        if col in out.columns:
            out[f"{c}_ratio"] = out[col] / denom
    return out

def main():
    ensure_dirs()
    date_str = FORCE_CLOSE_DATE if FORCE_CLOSE_DATE else latest_business_day()
    
    print(f"Target date: {date_str}")
    
    liq_day = _liquidity_day_from_history(date_str)
    inv_long_hist = _upsert_investor_long(date_str)
    inv_pivot = _read_investor_pivot_from_long(inv_long_hist)
    inv_pivot.to_csv(INVESTOR_PIVOT_CSV, index=False)
    
    merged = _merge_investor(liq_day, inv_pivot)
    merged.to_csv(MERGED_CSV, index=False)
    
    # Snapshot JSON 생성
    latest_snapshot = {"date": date_str, "markets": {}}
    for _, r in merged[merged["date"] == date_str].iterrows():
        latest_snapshot["markets"][str(r["market"])] = {
            "turnover_krw": float(r["turnover_krw"]) if pd.notna(r["turnover_krw"]) else None,
            "close": float(r["close"]) if pd.notna(r["close"]) else None,
            "foreign_net": float(r["foreign_net"]) if pd.notna(r["foreign_net"]) else None,
            "institution_net": float(r["institution_net"]) if pd.notna(r["institution_net"]) else None,
            "individual_net": float(r["individual_net"]) if pd.notna(r["individual_net"]) else None,
        }
    
    snap_path = DERIVED_DIR / "latest_market_flow_snapshot.json"
    snap_path.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Update Completed Successfully.")

if __name__ == "__main__":
    main()
