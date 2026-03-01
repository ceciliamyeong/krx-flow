from __future__ import annotations

print("RUNNING FILE:", __file__)
print("VERSION: run_daily-final-10billion-forceclose-2026-03-01")

import json
from pathlib import Path
from typing import List

import pandas as pd
from pykrx import stock


ROOT = Path(__file__).resolve().parents[1]

# outputs
HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INVESTOR_LONG_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"          # long-form (개인(십억원) 형태)
INVESTOR_PIVOT_CSV = ROOT / "data" / "derived" / "investor_flow_pivot_daily.csv"  # pivot (KRW)
MERGED_CSV = ROOT / "data" / "derived" / "market_flow_daily.csv"                  # liquidity + investor pivot merge
DERIVED_DIR = ROOT / "data" / "derived"
HISTORY_DIR = ROOT / "data" / "history"

# ✅ 최종 마감일 강제 (None이면 자동 최근 영업일)
FORCE_CLOSE_DATE = "2026-02-27"

# ✅ 거래소 수급 단위: (십억원)
RAW_UNIT_HINT = "(십억원)"


# ------------------------
# Helpers
# ------------------------

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


def _pick_col(df: pd.DataFrame, candidates: List[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(f"None of candidates found: {candidates} / got={df.columns.tolist()}")


def latest_business_day() -> str:
    from datetime import datetime, timedelta
    today = datetime.utcnow().date()
    start = (today - timedelta(days=14)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(end))
    if not days:
        raise RuntimeError("Cannot determine business days from pykrx")
    return to_dash_date(days[-1])


def prev_business_day(date_str: str) -> str:
    from datetime import datetime, timedelta
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    start = (d - timedelta(days=14)).strftime("%Y-%m-%d")
    days = stock.get_previous_business_days(fromdate=to_krx_date(start), todate=to_krx_date(date_str))
    if not days or len(days) < 2:
        raise RuntimeError(f"Cannot find previous business day for {date_str}")
    return to_dash_date(days[-2])


def _unit_mult(hint: str) -> float:
    s = str(hint)
    if "(십억원)" in s:
        return 1e10
    if "(억원)" in s:
        return 1e8
    if "(백만원)" in s:
        return 1e6
    if "(천원)" in s:
        return 1e3
    return 1.0


def _norm_inv(t: str) -> str:
    """
    investor_type 정규화:
      - '개인(십억원)' -> individual
      - '외국인(십억원)' -> foreign
      - '기관(십억원)' / '기관합계(십억원)' / 'institution_total' -> institution_total
    """
    s = str(t).strip()
    if not s:
        return s
    base = s.split("(")[0].strip()

    # english/internal
    if base in ["individual", "individual_total"]:
        return "individual"
    if base in ["foreign", "foreigner", "foreign_total"]:
        return "foreign"
    if base in ["institution_total", "institution", "institutions"]:
        return "institution_total"

    # korean
    if "개인" in base:
        return "individual"
    if "외국" in base:
        return "foreign"
    if "기관" in base:
        return "institution_total"

    return base


# ------------------------
# Liquidity (KOSPI/KOSDAQ index)
# ------------------------

def _fetch_market_index_liquidity(date_str: str) -> pd.DataFrame:
    """
    liquidity_daily.csv:
      date, market, turnover_krw, close
    """
    index_map = {"KOSPI": "1001", "KOSDAQ": "2001"}
    rows = []

    for mk, code in index_map.items():
        df = stock.get_index_ohlcv_by_date(to_krx_date(date_str), to_krx_date(date_str), code)
        if df is None or df.empty:
            raise RuntimeError(f"index ohlcv empty: market={mk}, code={code}, date={date_str}")

        close_col = _pick_col(df, ["종가", "Close"])

        value_col = None
        for cand in ["거래대금", "거래대금(원)", "거래대금(억원)", "Trading Value"]:
            if cand in df.columns:
                value_col = cand
                break
        if value_col is None:
            raise KeyError(f"Cannot find trading value column. cols={df.columns.tolist()}")

        close = float(pd.to_numeric(df.iloc[-1][close_col], errors="coerce"))
        tv_raw = pd.to_numeric(df.iloc[-1][value_col], errors="coerce")

        # if '(억원)' column -> KRW
        turnover_krw = float(tv_raw) * 1e8 if "(억원)" in str(value_col) else float(tv_raw)

        rows.append({"date": date_str, "market": mk, "turnover_krw": turnover_krw, "close": close})

    return pd.DataFrame(rows)


def _load_liquidity_history() -> pd.DataFrame:
    if not HIST_LIQ.exists():
        return pd.DataFrame(columns=["date", "market", "turnover_krw", "close"])
    df = pd.read_csv(HIST_LIQ)
    if df.empty:
        return pd.DataFrame(columns=["date", "market", "turnover_krw", "close"])

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["turnover_krw"] = pd.to_numeric(df.get("turnover_krw"), errors="coerce")
    df["close"] = pd.to_numeric(df.get("close"), errors="coerce")
    df = df.dropna(subset=["date", "market"])
    return df.sort_values(["date", "market"]).reset_index(drop=True)


def _upsert_liquidity(date_str: str) -> pd.DataFrame:
    hist = _load_liquidity_history()
    today_df = _fetch_market_index_liquidity(date_str)

    if not hist.empty:
        hist = hist[hist["date"] != date_str].copy()

    out = pd.concat([hist, today_df], ignore_index=True)
    out = out.sort_values(["date", "market"]).reset_index(drop=True)
    out.to_csv(HIST_LIQ, index=False)

    print("Saved liquidity:", HIST_LIQ, "rows=", len(out))
    return out


# ------------------------
# Investor long-form (market level)
# ------------------------

def _fetch_investor_long(date_str: str) -> pd.DataFrame:
    """
    investor_flow_daily.csv (long):
      date, market, investor_type, bid_raw, ask_raw, net_raw, raw_unit_hint
    investor_type는 '개인(십억원)' 형태로 저장, raw_unit_hint도 '(십억원)' 고정.
    """
    rows = []

    for mk in ["KOSPI", "KOSDAQ"]:
        df = stock.get_market_trading_value_by_investor(to_krx_date(date_str), market=mk)
        if df is None or df.empty:
            raise RuntimeError(f"investor trading value empty: date={date_str}, market={mk}")

        buy_col = None
        sell_col = None
        net_col = None

        for cand in ["매수", "BUY", "buy", "매수금액"]:
            if cand in df.columns:
                buy_col = cand
                break
        for cand in ["매도", "SELL", "sell", "매도금액"]:
            if cand in df.columns:
                sell_col = cand
                break
        for cand in ["순매수", "NET", "net", "순매수금액"]:
            if cand in df.columns:
                net_col = cand
                break

        if buy_col is None or sell_col is None or net_col is None:
            raise KeyError(f"Cannot find buy/sell/net cols. cols={df.columns.tolist()}")

        for inv_name in df.index.astype(str).tolist():
            bid_raw = pd.to_numeric(df.loc[inv_name, buy_col], errors="coerce")
            ask_raw = pd.to_numeric(df.loc[inv_name, sell_col], errors="coerce")
            net_raw = pd.to_numeric(df.loc[inv_name, net_col], errors="coerce")

            investor_type = f"{inv_name}{RAW_UNIT_HINT}"

            rows.append({
                "date": date_str,
                "market": mk,
                "investor_type": investor_type,
                "bid_raw": float(bid_raw) if pd.notna(bid_raw) else None,
                "ask_raw": float(ask_raw) if pd.notna(ask_raw) else None,
                "net_raw": float(net_raw) if pd.notna(net_raw) else None,
                "raw_unit_hint": RAW_UNIT_HINT,
            })

    return pd.DataFrame(rows)


def _load_investor_long() -> pd.DataFrame:
    cols = ["date", "market", "investor_type", "bid_raw", "ask_raw", "net_raw", "raw_unit_hint"]
    if not INVESTOR_LONG_CSV.exists():
        return pd.DataFrame(columns=cols)

    df = pd.read_csv(INVESTOR_LONG_CSV)
    if df.empty:
        return pd.DataFrame(columns=cols)

    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
    df["market"] = df["market"].astype(str)
    df["investor_type"] = df["investor_type"].astype(str)
    df["bid_raw"] = pd.to_numeric(df.get("bid_raw"), errors="coerce")
    df["ask_raw"] = pd.to_numeric(df.get("ask_raw"), errors="coerce")
    df["net_raw"] = pd.to_numeric(df.get("net_raw"), errors="coerce")
    df["raw_unit_hint"] = df.get("raw_unit_hint", RAW_UNIT_HINT)
    df = df.dropna(subset=["date", "market", "investor_type"])
    return df.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)


def _upsert_investor_long(date_str: str) -> pd.DataFrame:
    hist = _load_investor_long()
    today = _fetch_investor_long(date_str)

    if not hist.empty:
        hist = hist[hist["date"] != date_str].copy()

    out = pd.concat([hist, today], ignore_index=True)
    out = out.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)
    out.to_csv(INVESTOR_LONG_CSV, index=False)

    print("Saved investor long:", INVESTOR_LONG_CSV, "rows=", len(out))
    return out


def _read_investor_pivot_from_long(inv_long: pd.DataFrame) -> pd.DataFrame:
    """
    long -> pivot (KRW):
      date, market, foreign_net, institution_net, individual_net
    """
    cols = ["date", "market", "foreign_net", "institution_net", "individual_net"]
    if inv_long is None or inv_long.empty:
        return pd.DataFrame(columns=cols)

    inv = inv_long.copy()
    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)

    inv["investor_type_norm"] = inv["investor_type"].map(_norm_inv)

    inv["net_raw"] = pd.to_numeric(inv.get("net_raw"), errors="coerce")
    inv["raw_unit_hint"] = inv.get("raw_unit_hint", RAW_UNIT_HINT)
    inv["net_krw"] = inv["net_raw"] * inv["raw_unit_hint"].map(_unit_mult)

    keep = inv[inv["investor_type_norm"].isin(["individual", "foreign", "institution_total"])].copy()
    if keep.empty:
        return pd.DataFrame(columns=cols)

    pivot = (
        keep.groupby(["date", "market", "investor_type_norm"], as_index=False)["net_krw"]
        .sum(min_count=1)
        .pivot_table(index=["date", "market"], columns="investor_type_norm", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(columns={
            "individual": "individual_net",
            "foreign": "foreign_net",
            "institution_total": "institution_net",
        })
    )

    for c in ["individual_net", "foreign_net", "institution_net"]:
        if c not in pivot.columns:
            pivot[c] = pd.NA

    pivot = pivot.sort_values(["date", "market"]).reset_index(drop=True)
    return pivot[["date", "market", "foreign_net", "institution_net", "individual_net"]]


# ------------------------
# Merge (SAFE)
# ------------------------

def _merge_investor(liquidity: pd.DataFrame, investor_pivot: pd.DataFrame) -> pd.DataFrame:
    if liquidity is None or liquidity.empty:
        return liquidity

    if investor_pivot is None:
        investor_pivot = pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    net_cols = ["foreign_net", "institution_net", "individual_net"]

    # drop suffix junk
    liq_suffix = [c for c in liquidity.columns if c.endswith("_x") or c.endswith("_y")]
    if liq_suffix:
        liquidity = liquidity.drop(columns=liq_suffix)

    inv_suffix = [c for c in investor_pivot.columns if c.endswith("_x") or c.endswith("_y")]
    if inv_suffix:
        investor_pivot = investor_pivot.drop(columns=inv_suffix)

    # liquidity should not have net cols
    liq_drop = [c for c in net_cols if c in liquidity.columns]
    if liq_drop:
        liquidity = liquidity.drop(columns=liq_drop)

    # rename to avoid collision
    rename_map = {c: f"{c}__inv" for c in net_cols if c in investor_pivot.columns}
    if rename_map:
        investor_pivot = investor_pivot.rename(columns=rename_map)

    present = list(rename_map.values())
    if not investor_pivot.empty and {"date", "market"}.issubset(set(investor_pivot.columns)) and present:
        investor_pivot[present] = investor_pivot[present].apply(pd.to_numeric, errors="coerce")
        investor_pivot = investor_pivot.groupby(["date", "market"], as_index=False)[present].sum(min_count=1)

    out = liquidity.merge(investor_pivot, on=["date", "market"], how="left")

    # restore names
    back_map = {v: k for k, v in rename_map.items()}
    if back_map:
        out = out.rename(columns=back_map)

    # ensure no suffix columns remain
    drop_suffix_final = [c for c in out.columns if c.endswith("_x") or c.endswith("_y")]
    if drop_suffix_final:
        out = out.drop(columns=drop_suffix_final)

    denom = pd.to_numeric(out.get("turnover_krw", pd.Series([pd.NA] * len(out))), errors="coerce").replace({0: pd.NA})

    if "individual_net" in out.columns:
        out["individual_net"] = pd.to_numeric(out["individual_net"], errors="coerce")
        out["individual_ratio"] = out["individual_net"] / denom
    if "foreign_net" in out.columns:
        out["foreign_net"] = pd.to_numeric(out["foreign_net"], errors="coerce")
        out["foreign_ratio"] = out["foreign_net"] / denom
    if "institution_net" in out.columns:
        out["institution_net"] = pd.to_numeric(out["institution_net"], errors="coerce")
        out["institution_ratio"] = out["institution_net"] / denom

    return out.sort_values(["date", "market"]).reset_index(drop=True)


# ------------------------
# Main
# ------------------------

def main():
    ensure_dirs()

    date_str = FORCE_CLOSE_DATE if FORCE_CLOSE_DATE else latest_business_day()
    print("Target date:", date_str, "(prev:", prev_business_day(date_str), ")")
    print("Investor unit:", RAW_UNIT_HINT)
    print("FORCE_CLOSE_DATE:", FORCE_CLOSE_DATE)

    liq_hist = _upsert_liquidity(date_str)
    inv_long_hist = _upsert_investor_long(date_str)

    inv_pivot = _read_investor_pivot_from_long(inv_long_hist)
    inv_pivot.to_csv(INVESTOR_PIVOT_CSV, index=False)
    print("Saved investor pivot:", INVESTOR_PIVOT_CSV, "rows=", len(inv_pivot))

    merged = _merge_investor(liq_hist, inv_pivot)
    merged.to_csv(MERGED_CSV, index=False)
    print("Saved merged market flow:", MERGED_CSV, "rows=", len(merged))

    latest_snapshot = {"date": date_str, "markets": {}}
    latest_rows = merged[merged["date"] == date_str].copy()
    for _, r in latest_rows.iterrows():
        mk = str(r["market"])
        latest_snapshot["markets"][mk] = {
            "turnover_krw": None if pd.isna(r.get("turnover_krw")) else float(r.get("turnover_krw")),
            "close": None if pd.isna(r.get("close")) else float(r.get("close")),
            "foreign_net": None if pd.isna(r.get("foreign_net")) else float(r.get("foreign_net")),
            "institution_net": None if pd.isna(r.get("institution_net")) else float(r.get("institution_net")),
            "individual_net": None if pd.isna(r.get("individual_net")) else float(r.get("individual_net")),
            "foreign_ratio": None if pd.isna(r.get("foreign_ratio")) else float(r.get("foreign_ratio")),
            "institution_ratio": None if pd.isna(r.get("institution_ratio")) else float(r.get("institution_ratio")),
            "individual_ratio": None if pd.isna(r.get("individual_ratio")) else float(r.get("individual_ratio")),
        }

    snap_path = DERIVED_DIR / "latest_market_flow_snapshot.json"
    snap_path.write_text(json.dumps(latest_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    print("Saved snapshot:", snap_path)


if __name__ == "__main__":
    main()
