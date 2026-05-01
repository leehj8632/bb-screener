from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener, get_access_token, kis_get, get_ohlc_history, BASE_URL, APP_KEY, APP_SECRET
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

@app.get("/api/test")
def test_api():
    results = {}
    try:
        token = get_access_token()
        results["token"] = "OK" if token else "EMPTY"
    except Exception as e:
        results["token"] = f"ERROR: {e}"

    # 거래대금 순위 상위 5개 종목코드 확인
    try:
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_COND_SCR_DIV_CODE": "20171",
                "FID_INPUT_ISCD": "0000",
                "FID_DIV_CLS_CODE": "0",
                "FID_BLNG_CLS_CODE": "0",
                "FID_TRGT_CLS_CODE": "111111111",
                "FID_TRGT_EXLS_CLS_CODE": "0000000000",
                "FID_INPUT_PRICE_1": "",
                "FID_INPUT_PRICE_2": "",
                "FID_VOL_CNT": "",
                "FID_INPUT_DATE_1": "",
            },
            tr_id="FHPST01710000"
        )
        items = data.get("output", [])[:5]
        results["kospi_top5"] = [
            {"code": i.get("mksc_shrn_iscd"), "name": i.get("hts_kor_isnm"), "amount": i.get("acml_tr_pbmn")}
            for i in items
        ]
    except Exception as e:
        results["kospi_top5"] = f"ERROR: {e}"

    # 하이브 (352820) OHLC 직접 조회
    try:
        df = get_ohlc_history("352820", "20260102", "20260402", "KOSPI")
        if df is not None and not df.empty:
            results["hybe_ohlc_count"] = len(df)
            results["hybe_latest"] = df.tail(3).to_dict(orient="records")
            results["hybe_columns"] = df.columns.tolist()
        else:
            results["hybe_ohlc"] = "EMPTY"
    except Exception as e:
        results["hybe_ohlc"] = f"ERROR: {e}"

    # 카카오 (035720) OHLC 직접 조회
    try:
        df2 = get_ohlc_history("035720", "20260102", "20260402", "KOSPI")
        if df2 is not None and not df2.empty:
            results["kakao_ohlc_count"] = len(df2)
            results["kakao_latest"] = df2.tail(3).to_dict(orient="records")
        else:
            results["kakao_ohlc"] = "EMPTY"
    except Exception as e:
        results["kakao_ohlc"] = f"ERROR: {e}"

    return results

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
