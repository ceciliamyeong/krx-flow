import subprocess
import sys

def main():
    # 1) (선택) 당일 데이터 append 하는 로직이 있으면 여기서 실행
    # subprocess.check_call([sys.executable, "append_today.py"])

    # 2) 시그널 생성
    subprocess.check_call([sys.executable, "build_liquidity_signals.py"])

if __name__ == "__main__":
    main()
