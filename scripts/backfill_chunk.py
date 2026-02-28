#!/usr/bin/env python3
"""
scripts/backfill_chunk.py

Chunked backfill runner. Keeps long backfills stable in GitHub Actions by
splitting the requested range into weekly/monthly chunks and calling the
existing fetch/backfill scripts.

Example:
  python scripts/backfill_chunk.py --start 2022-01-01 --end 2022-12-31 --chunk monthly --market BOTH
"""
from __future__ import annotations

import argparse
import datetime as dt
import subprocess
from pathlib import Path


def _chunks(start: dt.date, end: dt.date, mode: str):
    cur = start
    while cur <= end:
        if mode == "weekly":
            nxt = min(end, cur + dt.timedelta(days=6))
        elif mode == "monthly":
            if cur.month == 12:
                eom = dt.date(cur.year, 12, 31)
            else:
                eom = dt.date(cur.year, cur.month + 1, 1) - dt.timedelta(days=1)
            nxt = min(end, eom)
        else:
            raise ValueError("chunk must be weekly|monthly")
        yield cur, nxt
        cur = nxt + dt.timedelta(days=1)


def _run(cmd: list[str]):
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--chunk", default="monthly", choices=["weekly", "monthly"])
    ap.add_argument("--market", default="BOTH", choices=["KOSPI", "KOSDAQ", "BOTH"])
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)

    root = Path(__file__).resolve().parents[1]
    scripts_dir = root / "scripts"

    for s, e in _chunks(start, end, args.chunk):
        s_str = s.isoformat()
        e_str = e.isoformat()

        f1 = scripts_dir / "backfill_liquidity.py"
        if f1.exists():
            _run(["python", str(f1), "--start", s_str, "--end", e_str, "--market", args.market])

        f2 = scripts_dir / "krx_fetch_investor.py"
        if f2.exists():
            _run(["python", str(f2), "--start", s_str, "--end", e_str, "--market", args.market, "--mode", "daily"])

    print("Done")


if __name__ == "__main__":
    main()
