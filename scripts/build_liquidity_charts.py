from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

ROOT = Path(__file__).resolve().parents[1]
IN_CSV = ROOT / "data" / "history" / "liquidity_daily.csv"
OUT_DIR = ROOT / "data" / "derived" / "charts"

def _fmt_krw(x):
    # 보기 좋게 단위 축약 (원 단위 입력 가정)
    if x is None:
        return ""
    x = float(x)
    if abs(x) >= 1e12:
        return f"{x/1e12:.1f}T"
    if abs(x) >= 1e8:
        return f"{x/1e8:.0f}억"
    if abs(x) >= 1e6:
        return f"{x/1e6:.0f}백만"
    return f"{x:.0f}"

def plot_market(df: pd.DataFrame, market: str, window_days: int | None = 365):
    d = df[df["market"] == market].copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d.sort_values("date")

    if window_days:
        cutoff = d["date"].max() - pd.Timedelta(days=window_days)
        d = d[d["date"] >= cutoff]

    # 거래대금이 너무 커서 보기 힘들면 "조원"으로 스케일
    d["turnover_trn"] = d["turnover_krw"] / 1e12  # 조원

    fig, ax1 = plt.subplots(figsize=(12, 6))

    # 지수(종가) 라인
    ax1.plot(d["date"], d["close"])
    ax1.set_ylabel(f"{market} Index (Close)")
    ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax1.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax1.xaxis.get_major_locator()))

    # 거래대금 막대(오른쪽 축)
    ax2 = ax1.twinx()
    ax2.bar(d["date"], d["turnover_trn"], width=1.0)
    ax2.set_ylabel("Turnover (KRW, Trillion)")

    title = f"{market}: Close (Line) vs Turnover (Bar)"
    if window_days:
        title += f" — last {window_days}d"
    ax1.set_title(title)

    fig.tight_layout()
    return fig

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(IN_CSV)

    # 기본: 최근 1년 + 전체(백필 확인용) 2종 세트 추천
    for market in ["KOSPI", "KOSDAQ"]:
        fig_1y = plot_market(df, market, window_days=365)
        out_1y = OUT_DIR / f"{market.lower()}_close_vs_turnover_1y.png"
        fig_1y.savefig(out_1y, dpi=160)
        plt.close(fig_1y)

        fig_all = plot_market(df, market, window_days=None)
        out_all = OUT_DIR / f"{market.lower()}_close_vs_turnover_all.png"
        fig_all.savefig(out_all, dpi=160)
        plt.close(fig_all)

    print("Saved charts into:", OUT_DIR)

if __name__ == "__main__":
    main()
