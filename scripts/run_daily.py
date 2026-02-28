import subprocess
import sys
import os

def main():
    # 1) (선택) 당일 데이터 append 하는 로직이 있으면 여기서 실행
    # subprocess.check_call([sys.executable, "append_today.py"])

    # 2) 시그널 생성
    current_dir = os.path.dirname(os.path.abspath(__file__))
    target_path = os.path.join(current_dir, "build_liquidity_signals.py")
    
    subprocess.check_call([sys.executable, target_path])

if __name__ == "__main__":
    main()
