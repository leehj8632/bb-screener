from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener
import uvicorn
import os
import requests

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

@app.get("/api/test-all")
def test_all():
    results = {}

    # 1. pykrx 테스트 - 코스피 거래대금 상위
    try:
        from pykrx import stock
        df = stock.get_market_ohlcv_by_ticker("20260402", market="KOSPI")
        results["pykrx_kospi_count"] = len(df)
        results["pykrx_kospi_columns"] = df.columns.tolist()
        if not df.empty and "거래대금" in df.columns:
            top5 = df.sort_values("거래대금", ascending=False).head(5)
            results["pykrx_kospi_top5"] = [
                {"code": idx, "name": stock.get_market_ticker_name(idx), "amount": int(row["거래대금"])}
                for idx, row in top5.iterrows()
            ]
    except Exception as e:
        results["pykrx_kospi"] = f"ERROR: {e}"

    # 2. pykrx OHLC 테스트 - 하이브 (352820)
    try:
        from pykrx import stock
        df2 = stock.get_market_ohlcv_by_date("20260113", "20260402", "352820")
        results["pykrx_hybe_count"] = len(df2)
        results["pykrx_hybe_columns"] = df2.columns.tolist()
        if not df2.empty:
            results["pykrx_hybe_latest3"] = df2.tail(3).reset_index().to_dict(orient="records")
    except Exception as e:
        results["pykrx_hybe"] = f"ERROR: {e}"

    # 3. 네이버 증권 접속 테스트
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
        resp = requests.get("https://finance.naver.com/sise/sise_quant.naver?sosok=0", headers=headers, timeout=10)
        results["naver_status"] = resp.status_code
        results["naver_length"] = len(resp.text)
    except Exception as e:
        results["naver"] = f"ERROR: {e}"

    return results

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
