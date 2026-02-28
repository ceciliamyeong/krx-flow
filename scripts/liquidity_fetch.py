# scripts/liquidity_fetch.py

import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA_FILE = ROOT / "data" / "history" / "liquidity_daily.csv"


def load_liquidity_history():
    """
    Load full liquidity history CSV
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"Liquidity file not found: {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "market"]).reset_index(drop=True)

    return df


def fetch_liquidity_range(start, end, market="BOTH"):
    """
    Fetch liquidity data for given date range.
    
    Parameters
    ----------
    start : datetime.date
    end : datetime.date
    market : 'KOSPI', 'KOSDAQ', or 'BOTH'
    
    Returns
    -------
    DataFrame
    """
    df = load_liquidity_history()

    mask = (df["date"] >= pd.to_datetime(start)) & (
        df["date"] <= pd.to_datetime(end)
    )

    df = df.loc[mask]

    if market != "BOTH":
        df = df[df["market"] == market]

    return df.reset_index(drop=True)
