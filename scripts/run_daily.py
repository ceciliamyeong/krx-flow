from pathlib import Path
import datetime as dt

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output"
OUT.mkdir(exist_ok=True)

def main():
    today = dt.date.today().isoformat()
    file = OUT / "healthcheck.txt"
    file.write_text(f"KRX FLOW OK - {today}", encoding="utf-8")
    print("Pipeline OK:", today)

if __name__ == "__main__":
    main()
