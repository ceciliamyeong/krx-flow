import requests
import datetime as dt
import random
import pandas as pd

BASE = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://data.krx.co.kr/",
    "Origin": "https://data.krx.co.kr",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
}

def _post(payload: dict, session: requests.Session):
    # ✅ 세션으로 먼저 메인 페이지 한번 찍어서 쿠키 확보
    session.get("https://data.krx.co.kr", headers=HEADERS, timeout=30)

    # ✅ 약간의 랜덤 딜레이(봇 패턴 완화)
    time.sleep(0.6 + random.random() * 0.8)

    r = session.post(BASE, headers=HEADERS, data=payload, timeout=30)

    # ✅ 에러면 상태코드 + 본문 일부 노출 (Actions 로그용)
    if r.status_code != 200:
        raise RuntimeError(f"KRX HTTP {r.status_code} body[:200]={r.text[:200]}")
    return r

def fetch_investor_flow(date: dt.date, market: str):
    mkt_map = {
        "KOSPI": "STK",
        "KOSDAQ": "KSQ"
    }

    payload = {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT02201",
        "locale": "ko_KR",
        "inqTpCd": "1",
        "trdVolVal": "2",
        "askBid": "3",
        "mktId": mkt_map[market],
        "strtDd": date.strftime("%Y%m%d"),
        "endDd": date.strftime("%Y%m%d"),
        "share": "2",
        "money": "3",
        "csvxls_isNo": "false"
    }

    session = requests.Session()
    r = _post(payload, session)
    js = r.json()

    df = pd.DataFrame(js["OutBlock_1"])

    # 개인 / 외국인 / 기관 합계 추출
    retail = df.loc[df["INVST_TP_NM"] == "개인", "NET_BUY_AMT"].values
    foreign = df.loc[df["INVST_TP_NM"] == "외국인", "NET_BUY_AMT"].values
    inst = df.loc[df["INVST_TP_NM"] == "기관합계", "NET_BUY_AMT"].values

    return {
        "date": date.isoformat(),
        "market": market,
        "retail_net": float(retail[0]) if len(retail) else None,
        "foreign_net": float(foreign[0]) if len(foreign) else None,
        "institution_net": float(inst[0]) if len(inst) else None,
    }
