from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener, get_access_token, BASE_URL, APP_KEY, APP_SECRET
import uvicorn
import os
import requests

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    """API 연결 진단용 엔드포인트"""
    results = {}

    # 1. 토큰 확인
    try:
        token = get_access_token()
        results["token"] = "OK" if token else "EMPTY"
        results["token_length"] = len(token) if token else 0
    except Exception as e:
        results["token"] = f"ERROR: {e}"

    # 2. 거래대금 순위 API 직접 호출 테스트
    try:
        token = get_access_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": "FHPST01710000",
            "custtype": "P",
            "Content-Type": "application/json; charset=utf-8"
        }
        params = {
            "fid_cond_mrkt_div_code": "J",
            "fid_cond_scr_div_code": "20171",
            "fid_input_iscd": "0000",
            "fid_div_cls_code": "0",
            "fid_blng_cls_code": "0",
            "fid_trgt_cls_code": "111111111",
            "fid_trgt_exls_cls_code": "0000000000",
            "fid_input_price_1": "",
            "fid_input_price_2": "",
            "fid_vol_cnt": "",
            "fid_input_date_1": "",
        }
        url = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/trading-value"
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        results["trading_value_status"] = resp.status_code
        results["trading_value_text_len"] = len(resp.text)
        results["trading_value_text_preview"] = resp.text[:300]
        try:
            data = resp.json()
            results["trading_value_rt_cd"] = data.get("rt_cd")
            results["trading_value_msg"] = data.get("msg1", "")
            results["trading_value_output_count"] = len(data.get("output", []))
        except:
            results["trading_value_json"] = "PARSE_FAILED"
    except Exception as e:
        results["trading_value"] = f"ERROR: {e}"

    # 3. 삼성전자 시세 테스트
    try:
        token = get_access_token()
        headers = {
            "authorization": f"Bearer {token}",
            "appkey": APP_KEY,
            "appsecret": APP_SECRET,
            "tr_id": "FHKST01010100",
            "custtype": "P",
            "Content-Type": "application/json; charset=utf-8"
        }
        url = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
        resp = requests.get(url, headers=headers, params={"fid_cond_mrkt_div_code": "J", "fid_input_iscd": "005930"}, timeout=30)
        results["inquire_price_status"] = resp.status_code
        results["inquire_price_preview"] = resp.text[:200]
    except Exception as e:
        results["inquire_price"] = f"ERROR: {e}"

    return results

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
