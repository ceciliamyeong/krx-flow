from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

import pandas as pd

# deps
from pykrx import stock
import matplotlib.pyplot as plt
import squarify


ROOT = Path(__file__).resolve().parents[1]

HIST_LIQ = ROOT / "data" / "history" / "liquidity_daily.csv"
INV_CSV = ROOT / "data" / "derived" / "investor_flow_daily.csv"

OUT_BASE = ROOT / "data" / "derived" / "dashboard"
OUT_ARCHIVE = OUT_BASE / "archive"
OUT_CHART = ROOT / "data" / "derived" / "charts"


# ------------------------
# Utils
# ------------------------

def ensure_dirs():
    OUT_BASE.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE.mkdir(parents=True, exist_ok=True)
    OUT_CHART.mkdir(parents=True, exist_ok=True)


def krw_readable(x: Optional[float]) -> Optional[str]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    a = abs(v)
    if a >= 1e12:
        return f"{v/1e12:+.2f}조"
    if a >= 1e8:
        return f"{v/1e8:+.0f}억"
    return f"{v:+.0f}"


def unit_mult(raw_hint: str) -> float:
    s = str(raw_hint)
    if "(십억원)" in s:
        return 1e10
    if "(억원)" in s:
        return 1e8
    if "(백만원)" in s:
        return 1e6
    if "(천원)" in s:
        return 1e3
    return 1.0


def norm_inv(x: str) -> str:
    """
    Normalize to:
      foreign / institution / individual
    """
    s = str(x).strip()
    base = s.split("(")[0].strip()
    if base == "기관" or s == "institution_total":
        return "institution"
    if base == "개인" or s == "individual":
        return "individual"
    if base == "외국인" or s == "foreign":
        return "foreign"
    return s


def signal_label(ratio: Optional[float], strong: float = 0.05, normal: float = 0.02) -> Optional[str]:
    if ratio is None:
        return None
    a = abs(float(ratio))
    if a >= strong:
        return "STRONG"
    if a >= normal:
        return "NORMAL"
    return "WEAK"


# ------------------------
# Latest date selection
# ------------------------

def load_liq_df() -> pd.DataFrame:
    if not HIST_LIQ.exists():
        raise FileNotFoundError(f"Missing {HIST_LIQ}")
    liq = pd.read_csv(HIST_LIQ)
    if "date" not in liq.columns:
        raise KeyError(f"{HIST_LIQ} missing date column")
    liq["date"] = pd.to_datetime(liq["date"], errors="coerce").dt.date.astype(str)
    liq["market"] = liq["market"].astype(str)
    liq["turnover_krw"] = pd.to_numeric(liq.get("turnover_krw"), errors="coerce")
    liq["close"] = pd.to_numeric(liq.get("close"), errors="coerce")
    liq = liq.dropna(subset=["date", "market"])
    return liq.sort_values(["date", "market"]).reset_index(drop=True)


def load_inv_df() -> pd.DataFrame:
    if not INV_CSV.exists():
        raise FileNotFoundError(f"Missing {INV_CSV}")
    inv = pd.read_csv(INV_CSV)
    if inv.empty:
        return inv
    inv["date"] = pd.to_datetime(inv["date"], errors="coerce").dt.date.astype(str)
    inv["market"] = inv["market"].astype(str)
    inv["investor_type"] = inv["investor_type"].map(norm_inv)
    inv["net_raw"] = pd.to_numeric(inv.get("net_raw"), errors="coerce")
    inv["net_krw"] = inv["net_raw"] * inv.get("raw_unit_hint", "").map(unit_mult)
    inv = inv.dropna(subset=["date", "market", "investor_type"])
    return inv.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)


def pick_latest_trade_date(liq: pd.DataFrame, inv: pd.DataFrame) -> str:
    """
    1) liquidity 최신 date를 후보로 잡고
    2) 그 날짜에 investor 3종(개인/외국인/기관)이 없으면
       investor가 존재하는 가장 가까운 과거 date로 후퇴
    """
    if liq.empty:
        raise RuntimeError("liquidity_daily.csv is empty")

    liq_dates = sorted(liq["date"].unique())
    latest_liq = liq_dates[-1]

    if inv is None or inv.empty:
        # investor가 비어있으면 일단 liquidity 최신으로 진행(카드 일부가 빈 채로라도)
        return latest_liq

    # 해당 날짜에 core investor types가 존재하는지 체크
    def has_core(date_str: str) -> bool:
        sub = inv[inv["date"] == date_str]
        if sub.empty:
            return False
        types = set(sub["investor_type"].unique().tolist())
        return {"foreign", "institution", "individual"}.issubset(types)

    if has_core(latest_liq):
        return latest_liq

    # investor가 있는 날짜들 중 liquidity 최신보다 같거나 이전 중 가장 최근
    inv_dates = sorted(inv["date"].unique())
    candidates = [d for d in inv_dates if d <= latest_liq]
    if not candidates:
        return latest_liq

    # 가장 최근 후보부터 core 여부 확인
    for d in reversed(candidates):
        if has_core(d):
            return d

    # core는 없지만 investor가 있는 가장 최근 날짜로라도
    return candidates[-1]


# ------------------------
# Core cards: index + flow
# ------------------------

def load_index_rows(liq: pd.DataFrame, date_str: str) -> pd.DataFrame:
    day = liq[liq["date"] == date_str].copy()
    if day.empty:
        raise RuntimeError(f"No liquidity rows for date={date_str}")
    # 기대: KOSPI, KOSDAQ 2행
    return day.sort_values(["market"]).reset_index(drop=True)


def load_investor_pivot(inv: pd.DataFrame, date_str: str) -> pd.DataFrame:
    """
    Returns:
      date, market, foreign_net, institution_net, individual_net (KRW)
    """
    if inv is None or inv.empty:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    sub = inv[inv["date"] == date_str].copy()
    if sub.empty:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    keep = sub[sub["investor_type"].isin(["foreign", "institution", "individual"])].copy()
    if keep.empty:
        return pd.DataFrame(columns=["date", "market", "foreign_net", "institution_net", "individual_net"])

    pivot = (
        keep.groupby(["date", "market", "investor_type"], as_index=False)["net_krw"]
        .sum()
        .pivot_table(index=["date", "market"], columns="investor_type", values="net_krw", aggfunc="sum")
        .reset_index()
        .rename(columns={"foreign": "foreign_net", "institution": "institution_net", "individual": "individual_net"})
    )

    for c in ["foreign_net", "institution_net", "individual_net"]:
        if c not in pivot.columns:
            pivot[c] = pd.NA

    return pivot.sort_values(["market"]).reset_index(drop=True)


def build_market_cards(liq_day: pd.DataFrame, inv_pivot: pd.DataFrame) -> Dict[str, Any]:
    merged = liq_day.merge(inv_pivot, on=["date", "market"], how="left")

    markets: Dict[str, Any] = {}
    for _, r in merged.iterrows():
        mk = str(r["market"])
        turnover = None if pd.isna(r.get("turnover_krw")) else float(r.get("turnover_krw"))
        close = None if pd.isna(r.get("close")) else float(r.get("close"))

        foreign = None if pd.isna(r.get("foreign_net")) else float(r.get("foreign_net"))
        inst = None if pd.isna(r.get("institution_net")) else float(r.get("institution_net"))
        indiv = None if pd.isna(r.get("individual_net")) else float(r.get("individual_net"))

        def ratio(v: Optional[float]) -> Optional[float]:
            if v is None or turnover is None or turnover == 0:
                return None
            return float(v) / float(turnover)

        ratios = {
            "foreign": ratio(foreign),
            "institution": ratio(inst),
            "individual": ratio(indiv),
        }

        markets[mk] = {
            "close": close,
            "turnover_krw": turnover,
            "turnover_readable": krw_readable(turnover),
            "investor_net_krw": {
                "foreign": foreign,
                "institution": inst,
                "individual": indiv,
            },
            "investor_net_readable": {
                "foreign": krw_readable(foreign),
                "institution": krw_readable(inst),
                "individual": krw_readable(indiv),
            },
            "investor_ratio": ratios,
            "flow_signal": {
                "foreign": signal_label(ratios["foreign"]),
                "institution": signal_label(ratios["institution"]),
                "individual": signal_label(ratios["individual"]),
            },
        }

    return markets


# ------------------------
# Top10 Treemap + Volatility + Breadth
# ------------------------

def fetch_top10_mcap_and_return(date_str: str, market: str) -> pd.DataFrame:
    ymd = date_str.replace("-", "")
    mcap = stock.get_market_cap_by_ticker(ymd, market=market)
    ohlcv = stock.get_market_ohlcv_by_ticker(ymd, market=market)

    mcap_col = "시가총액" if "시가총액" in mcap.columns else mcap.select_dtypes("number").columns[0]
    ret_col = "등락률" if "등락률" in ohlcv.columns else ("등락률(%)" if "등락률(%)" in ohlcv.columns else None)
    if ret_col is None:
        # best-effort fallback
        if "종가" in ohlcv.columns and "전일종가" in ohlcv.columns:
            ohlcv["__ret"] = (ohlcv["종가"] / ohlcv["전일종가"] - 1) * 100
            ret_col = "__ret"
        else:
            ohlcv["__ret"] = 0.0
            ret_col = "__ret"

    df = (
        mcap[[mcap_col]].rename(columns={mcap_col: "mcap"})
        .join(ohlcv[[ret_col]].rename(columns={ret_col: "return_pct"}), how="left")
        .reset_index()
        .rename(columns={"티커": "ticker", "index": "ticker"})
    )
    df["ticker"] = df["ticker"].astype(str)
    df["mcap"] = pd.to_numeric(df["mcap"], errors="coerce")
    df["return_pct"] = pd.to_numeric(df["return_pct"], errors="coerce")

    df = df.dropna(subset=["mcap"]).sort_values("mcap", ascending=False).head(10).copy()
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)
    return df[["ticker", "name", "mcap", "return_pct"]].reset_index(drop=True)


def make_treemap_png(df: pd.DataFrame, title: str, out_path: Path):
    sizes = df["mcap"].astype(float).tolist()
    labels = [
        f"{n}\n{rp:+.2f}%"
        for n, rp in zip(df["name"].tolist(), df["return_pct"].fillna(0.0).astype(float).tolist())
    ]

    # 코인판 감성: 상승=초록, 하락=빨강
    colors = []
    for rp in df["return_pct"].fillna(0.0).astype(float).tolist():
        if rp > 0:
            colors.append("#2E7D32")
        elif rp < 0:
            colors.append("#C62828")
        else:
            colors.append("#9E9E9E")

    fig = plt.figure(figsize=(12, 7))
    ax = fig.add_subplot(111)
    ax.set_axis_off()
    ax.set_title(title)

    squarify.plot(
        sizes=sizes,
        label=labels,
        color=colors,
        alpha=0.9,
        text_kwargs={"fontsize": 11},
        ax=ax,
    )

    fig.tight_layout()
    fig.savefig(out_path, dpi=160)
    plt.close(fig)


def fetch_volatility_top5(date_str: str, market: str) -> List[Dict[str, Any]]:
    """
    '오늘 이슈' 카드: abs(등락률) 기준 Top5
    """
    ymd = date_str.replace("-", "")
    ohlcv = stock.get_market_ohlcv_by_ticker(ymd, market=market)

    ret_col = "등락률" if "등락률" in ohlcv.columns else ("등락률(%)" if "등락률(%)" in ohlcv.columns else None)
    if ret_col is None:
        if "종가" in ohlcv.columns and "전일종가" in ohlcv.columns:
            ohlcv["__ret"] = (ohlcv["종가"] / ohlcv["전일종가"] - 1) * 100
            ret_col = "__ret"
        else:
            ohlcv["__ret"] = 0.0
            ret_col = "__ret"

    df = ohlcv[[ret_col]].rename(columns={ret_col: "return_pct"}).copy()
    df["return_pct"] = pd.to_numeric(df["return_pct"], errors="coerce").fillna(0.0)
    df["abs_return"] = df["return_pct"].abs()
    df = df.sort_values("abs_return", ascending=False).head(5).reset_index()

    ticker_col = "티커" if "티커" in df.columns else df.columns[0]
    df = df.rename(columns={ticker_col: "ticker"})
    df["ticker"] = df["ticker"].astype(str)
    df["name"] = df["ticker"].map(stock.get_market_ticker_name)

    return df[["ticker", "name", "return_pct"]].to_dict(orient="records")


def fetch_breadth(date_str: str, market: str) -> Dict[str, int]:
    """
    상승/하락/보합 종목 수
    """
    ymd = date_str.replace("-", "")
    ohlcv = stock.get_market_ohlcv_by_ticker(ymd, market=market)

    ret_col = "등락률" if "등락률" in ohlcv.columns else ("등락률(%)" if "등락률(%)" in ohlcv.columns else None)
    if ret_col is None:
        if "종가" in ohlcv.columns and "전일종가" in ohlcv.columns:
            ohlcv["__ret"] = (ohlcv["종가"] / ohlcv["전일종가"] - 1) * 100
            ret_col = "__ret"
        else:
            ohlcv["__ret"] = 0.0
            ret_col = "__ret"

    s = pd.to_numeric(ohlcv[ret_col], errors="coerce").fillna(0.0)
    return {
        "up": int((s > 0).sum()),
        "down": int((s < 0).sum()),
        "flat": int((s == 0).sum()),
    }


# ------------------------
# Main
# ------------------------

def main():
    ensure_dirs()

    liq = load_liq_df()
    inv = load_inv_df()

    date_str = pick_latest_trade_date(liq, inv)

    liq_day = load_index_rows(liq, date_str)
    inv_pivot = load_investor_pivot(inv, date_str)

    dashboard: Dict[str, Any] = {
        "date": date_str,
        "version": "1.0",
        "markets": build_market_cards(liq_day, inv_pivot),
        "extras": {},
    }

    # Top10 treemap + data
    top10: Dict[str, Any] = {}
    for mk in ["KOSPI", "KOSDAQ"]:
        df_top10 = fetch_top10_mcap_and_return(date_str, mk)
        top10[mk] = df_top10.to_dict(orient="records")

        # always overwrite latest images (for web)
        make_treemap_png(
            df_top10,
            f"{mk} 시총 TOP10 — {date_str}",
            OUT_CHART / f"treemap_{mk.lower()}_top10_latest.png",
        )

    dashboard["extras"]["top10_treemap"] = top10
    dashboard["extras"]["treemap_png"] = {
        "KOSPI": "data/derived/charts/treemap_kospi_top10_latest.png",
        "KOSDAQ": "data/derived/charts/treemap_kosdaq_top10_latest.png",
    }

    # Volatility top5 + Breadth
    dashboard["extras"]["volatility_top5"] = {
        "KOSPI": fetch_volatility_top5(date_str, "KOSPI"),
        "KOSDAQ": fetch_volatility_top5(date_str, "KOSDAQ"),
    }
    dashboard["extras"]["breadth"] = {
        "KOSPI": fetch_breadth(date_str, "KOSPI"),
        "KOSDAQ": fetch_breadth(date_str, "KOSDAQ"),
    }

    # archive + latest
    archive_path = OUT_ARCHIVE / f"{date_str}.json"
    latest_path = OUT_BASE / "latest.json"

    archive_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")

    print("Built dashboard for:", date_str)
    print("Archive:", archive_path)
    print("Latest:", latest_path)
    print("Charts:", OUT_CHART)


if __name__ == "__main__":
    main()
