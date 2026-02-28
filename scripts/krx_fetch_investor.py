#!/usr/bin/env python3
"""
scripts/krx_fetch_investor.py

Fetch investor net trading (market-level) for KOSPI/KOSDAQ from:
  https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd (POST)

Confirmed bld for "투자자별 매매 동향 (시장)" you captured:
  dbms/MDC/MAIN/MDCMAIN00103

This script loops day-by-day and writes:
  data/derived/investor_flow_daily.csv

Columns:
  date, market, investor_type, bid_raw, ask_raw, net_raw, raw_unit_hint
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
from pathlib import Path

import pandas as pd
import requests


KRX_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"
BLD_INVESTOR = "dbms/MDC/MAIN/MDCMAIN00103"

MKTID_MAP = {"KOSPI": "STK", "KOSDAQ": "KSQ"}

INV_MAP = {
    "개인": "individual",
    "외국인": "foreign",
    "기관": "institution_total",
    "기관(십억원)": "institution_total",
    "금융투자": "financial_investment",
    "보험": "insurance",
    "투신(사모)": "investment_trust",
    "은행": "bank",
    "기타금융기관": "other_financial",
    "연기금등": "pension",
    "기타법인": "other_corp",
    "기타외국인": "other_foreign",
}


def _to_yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _to_int(x):
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return int(x)
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    s = s.replace(",", "")
    try:
        return int(float(s))
    except Exception:
        return None


def _request_krx(payload: dict, session: requests.Session, retries: int = 3) -> dict:
    last_err = None
    for i in range(retries):
        try:
            r = session.post(
                KRX_URL,
                data=payload,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Origin": "https://data.krx.co.kr",
                    "Referer": "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
                    "X-Requested-With": "XMLHttpRequest",
                },
                timeout=20,
            )
            r.raise_for_status()
            try:
                return r.json()
            except Exception:
                return json.loads(r.text)
        except Exception as e:
            last_err = e
            time.sleep(0.7 * (i + 1))
    raise RuntimeError(f"KRX request failed. payload={payload} err={last_err}")


def fetch_one_day(trd_dd: dt.date, market: str, session: requests.Session) -> pd.DataFrame:
    payload = {
        "bld": BLD_INVESTOR,
        "mktId": MKTID_MAP[market],
        "trdDd": _to_yyyymmdd(trd_dd),  # often required; if ignored KRX returns latest
        "locale": "ko_KR",
    }
    js = _request_krx(payload, session=session)
    rows = js.get("output") or js.get("OutBlock1") or []
    if not rows:
        return pd.DataFrame(columns=["date", "market", "investor_type", "bid_raw", "ask_raw", "net_raw", "raw_unit_hint"])

    out = []
    for r in rows:
        inv_label = (r.get("INVST_TP") or r.get("INVST_TP_NM") or "").strip()
        inv = INV_MAP.get(inv_label, inv_label or "unknown")

        out.append(
            {
                "date": trd_dd.isoformat(),
                "market": market,
                "investor_type": inv,
                "bid_raw": _to_int(r.get("ACC_BID_TRDVAL") or r.get("BID_TRDVAL")),
                "ask_raw": _to_int(r.get("ACC_ASK_TRDVAL") or r.get("ASK_TRDVAL")),
                "net_raw": _to_int(r.get("NETBID_TRDVAL") or r.get("NET_TRDVAL")),
                "raw_unit_hint": inv_label,  # keep to confirm units later
            }
        )
    return pd.DataFrame(out)


def backfill(start: dt.date, end: dt.date, markets: list[str]) -> pd.DataFrame:
    frames = []
    with requests.Session() as sess:
        cur = start
        while cur <= end:
            if cur.weekday() < 5:
                for m in markets:
                    d = fetch_one_day(cur, m, sess)
                    if not d.empty:
                        frames.append(d)
                time.sleep(0.15)  # throttle
            cur += dt.timedelta(days=1)

    if not frames:
        return pd.DataFrame(columns=["date", "market", "investor_type", "bid_raw", "ask_raw", "net_raw", "raw_unit_hint"])
    return pd.concat(frames, ignore_index=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--market", default="BOTH", choices=["KOSPI", "KOSDAQ", "BOTH"])
    ap.add_argument("--mode", default="daily", choices=["daily"])
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    markets = ["KOSPI", "KOSDAQ"] if args.market == "BOTH" else [args.market]

    df = backfill(start, end, markets).sort_values(["date", "market", "investor_type"]).reset_index(drop=True)

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data" / "derived"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_csv = out_dir / "investor_flow_daily.csv"

    if out_csv.exists():
        old = pd.read_csv(out_csv)
        df = pd.concat([old, df], ignore_index=True)
        df = df.drop_duplicates(subset=["date", "market", "investor_type"], keep="last")

    df = df.sort_values(["date", "market", "investor_type"]).reset_index(drop=True)
    df.to_csv(out_csv, index=False)
    print("Saved:", out_csv, "rows=", len(df))


if __name__ == "__main__":
    main()
