# scripts/run_daily.py
from __future__ import annotations

import datetime as dt
from pathlib import Path
import subprocess
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

HISTORY_DIR = ROOT / "data" / "history"
DERIVED_DIR = ROOT / "data" / "derived"

LIQUIDITY_CSV = HISTORY_DIR / "liquidity_daily.csv"
INVESTOR_CSV = DERIVED_DIR / "investor_flow_daily.csv"
COMPLETE_CSV = HISTORY_DIR / "liquidity_daily_complete.csv"


def run_cmd(cmd: list[str]):
    print(">", " ".join(cmd))
    subprocess.run(cmd, check=True)


def merge_data():
    if not LIQUIDITY_CSV.exists():
        raise RuntimeError(f"Missing {LIQUIDITY_CSV}")

    if not INVESTOR_CSV.exists():
        print("⚠ investor CSV not found, saving liquidity only")
        df = pd.read_csv(LIQUIDITY_CSV)
        df.to_csv(COMPLETE_CSV, index=False)
        return

    liq = pd.read_csv(LIQUIDITY_CSV)
    inv = pd.read_csv(INVESTOR_CSV)

    # 투자자별 순매수 pivot
    inv_pivot = (
        inv.pivot_table(
            index=["date", "market"],
            columns="investor_type",
            values="net_raw",
            aggfunc="sum",
        )
        .reset_index()
    )

    # 컬럼 정리 (있을 때만 rename)
    rename_map = {
        "individual": "individual_net",
        "foreign": "foreign_net",
        "institution_total": "institution_net",
    }
    for k, v in rename_map.items():
        if k in inv_pivot.columns:
            inv_pivot = inv_pivot.rename(columns={k: v})

    df = liq.merge(inv_pivot, on=["date", "market"], how="left")

    # ratio 계산
    if "individual_net" in df.columns:
        df["individual_ratio"] = df["individual_net"] / df["turnover_krw"]

    if "foreign_net" in df.columns:
        df["foreign_ratio"] = df["foreign_net"] / df["turnover_krw"]

    df.to_csv(COMPLETE_CSV, index=False)
    print("Saved merged:", COMPLETE_CSV)


def main():
    today = dt.date.today()
    start = today - dt.timedelta(days=7)

    # 1️⃣ liquidity 업데이트 (이미 있다면 생략 가능)
    # run_cmd(["python", "scripts/backfill_liquidity.py",
    #          "--start", start.isoformat(),
    #          "--end", today.isoformat(),
    #          "--market", "BOTH"])

    # 2️⃣ investor 업데이트
    run_cmd([
        "python",
        "scripts/krx_fetch_investor.py",
        "--start", start.isoformat(),
        "--end", today.isoformat(),
        "--market", "BOTH",
        "--mode", "daily"
    ])

    # 3️⃣ merge
    merge_data()

    # 4️⃣ 차트 생성
    run_cmd(["python", "scripts/build_liquidity_charts.py"])


if __name__ == "__main__":
    main()
