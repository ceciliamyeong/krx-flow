import argparse
import datetime as dt
from pathlib import Path
import pandas as pd
import sys

# scripts 폴더 import 안정화
sys.path.append(str(Path(__file__).resolve().parent))

from krx_fetch_investor import fetch_investor_flow_range

ROOT = Path(__file__).resolve().parents[1]
HIST = ROOT / "data" / "history"
HIST.mkdir(parents=True, exist_ok=True)

SCHEMA_COLS = [
    "date",
    "market",
    "turnover",
    "retail_net",
    "foreign_net",
    "institution_net",
    "advancers",
    "decliners",
    "top10_turnover_share",
]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--market", default="BOTH")
    args = parser.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    markets = ["KOSPI", "KOSDAQ"] if args.market == "BOTH" else [args.market]

    frames = []
    quality = []

    for m in markets:
        try:
            df_inv = fetch_investor_flow_range(start, end, m)
            frames.append(df_inv)

            quality.append({
                "market": m,
                "module": "investor",
                "status": "ok",
                "rows": len(df_inv),
            })

        except Exception as e:
            quality.append({
                "market": m,
                "module": "investor",
                "status": "error",
                "error": str(e)[:200],
            })

    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    for col in SCHEMA_COLS:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[SCHEMA_COLS].sort_values(["date", "market"])

    out_flow = HIST / f"flow_{args.start}_{args.end}_{args.market}.csv"
    out_quality = HIST / f"quality_{args.start}_{args.end}_{args.market}.csv"

    df.to_csv(out_flow, index=False)
    pd.DataFrame(quality).to_csv(out_quality, index=False)

    print("Flow saved:", out_flow)
    print("Quality saved:", out_quality)


if __name__ == "__main__":
    main()
