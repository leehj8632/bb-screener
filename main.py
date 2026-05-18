from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener, get_top100_naver, get_ohlc_history, calc_bb, classify_conditions
import uvicorn
import os
from datetime import datetime, timedelta

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    return FileResponse("static/index.html")

@app.get("/api/analyze")
def analyze(proximity: float = 3.0, date: str = None):
    try:
        result = run_screener(proximity, date)
        return {"status": "ok", "data": result}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/debug-cj")
def debug_cj():
    """CJ(001040)가 리스트에 있는지, BB 계산은 맞는지 확인"""
    today = datetime.today()
    while today.weekday() >= 5:
        today -= timedelta(days=1)
    date_str = today.strftime("%Y%m%d")
    start_str = (today - timedelta(days=90)).strftime("%Y%m%d")

    # 1. KOSPI 거래대금 순위에서 CJ 위치 확인
    kospi_list = get_top100_naver("KOSPI", date_str)
    cj_in_list = [(i+1, code, name, amt) for i, (code, name, mkt, amt) in enumerate(kospi_list) if code == "001040"]

    # 2. CJ OHLC 데이터 확인
    df = get_ohlc_history("001040", start_str, date_str)
    ohlc_ok = df is not None and not df.empty and len(df) >= 4

    bb1_result = None
    bb2_result = None
    if ohlc_ok:
        close_today = float(df["종가"].iloc[-1])
        open_s = df["시가"].dropna()
        close_s = df["종가"].dropna()
        if len(open_s) >= 4:
            _, bb1_mid, bb1_lower = calc_bb(open_s, 4, 4.0)
            bb1_conds = classify_conditions(close_today, bb1_lower, bb1_mid, 3.0)
            bb1_result = {"mid": round(bb1_mid), "lower": round(bb1_lower), "close": close_today, "conditions": bb1_conds}
        if len(close_s) >= 20:
            _, bb2_mid, bb2_lower = calc_bb(close_s, 20, 2.0)
            bb2_conds = classify_conditions(close_today, bb2_lower, bb2_mid, 3.0)
            bb2_result = {"mid": round(bb2_mid), "lower": round(bb2_lower), "close": close_today, "conditions": bb2_conds}

    return {
        "date": date_str,
        "cj_in_kospi_top100": cj_in_list,
        "kospi_list_count": len(kospi_list),
        "kospi_list_codes": [code for code, _, _, _ in kospi_list],
        "cj_ohlc_count": len(df) if ohlc_ok else 0,
        "bb1": bb1_result,
        "bb2": bb2_result,
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
