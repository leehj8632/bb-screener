from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener, get_ohlc_history, get_top100_naver, calc_bb
import uvicorn
import os

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

@app.get("/api/debug")
def debug(date: str = None):
    """볼린저밴드 계산 디버그 - CJ(001040) 직접 확인"""
    from datetime import datetime, timedelta
    import numpy as np

    if not date:
        date = datetime.today().strftime("%Y%m%d")

    # 주말 조정
    d = datetime.strptime(date, "%Y%m%d")
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    date = d.strftime("%Y%m%d")
    start = (d - timedelta(days=90)).strftime("%Y%m%d")

    results = {"date": date, "start": start}

    # 1. 네이버 거래대금 순위 상위 5개 확인
    try:
        kospi_top5 = get_top100_naver("KOSPI", date)[:5]
        results["kospi_top5"] = [{"code": c, "name": n, "amount": a} for c, n, m, a in kospi_top5]
    except Exception as e:
        results["kospi_top5_error"] = str(e)

    # 2. CJ(001040) OHLC 확인
    try:
        df = get_ohlc_history("001040", start, date)
        results["cj_ohlc_count"] = len(df) if df is not None and not df.empty else 0
        if df is not None and not df.empty:
            results["cj_latest5"] = df.tail(5).reset_index().to_dict(orient="records")
            results["cj_columns"] = df.columns.tolist()

            close_today = float(df["종가"].iloc[-1])
            results["cj_close"] = close_today

            # BB1: 시가 기준 MA4 SD×4
            open_s = df["시가"].dropna()
            results["cj_open_count"] = len(open_s)
            if len(open_s) >= 4:
                bb1_upper, bb1_mid, bb1_lower = calc_bb(open_s, 4, 4.0)
                results["cj_bb1"] = {
                    "upper": round(bb1_upper, 0),
                    "mid": round(bb1_mid, 0),
                    "lower": round(bb1_lower, 0),
                    "close": close_today,
                    "below_lower": close_today < bb1_lower,
                    "below_mid": close_today < bb1_mid,
                }

            # BB2: 종가 기준 MA20 SD×2
            close_s = df["종가"].dropna()
            results["cj_close_count"] = len(close_s)
            if len(close_s) >= 20:
                bb2_upper, bb2_mid, bb2_lower = calc_bb(close_s, 20, 2.0)
                results["cj_bb2"] = {
                    "upper": round(bb2_upper, 0),
                    "mid": round(bb2_mid, 0),
                    "lower": round(bb2_lower, 0),
                    "close": close_today,
                    "below_lower": close_today < bb2_lower,
                    "below_mid": close_today < bb2_mid,
                }
    except Exception as e:
        results["cj_error"] = str(e)

    return results

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
