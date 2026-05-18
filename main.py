from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from screener import run_screener, get_ohlc_history, calc_bb
import uvicorn
import os
import requests
from bs4 import BeautifulSoup

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://finance.naver.com"
}

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

@app.get("/api/debug-naver")
def debug_naver():
    """네이버 거래대금 순위 HTML 구조 확인"""
    try:
        url = "https://finance.naver.com/sise/sise_quant.naver"
        resp = requests.get(url, headers=HEADERS, params={"sosok": "0"}, timeout=15)
        soup = BeautifulSoup(resp.text, "html.parser")

        table = soup.find("table", {"class": "type_2"})
        if not table:
            return {"error": "테이블 없음", "html_preview": resp.text[:500]}

        rows = table.find_all("tr")
        result = []
        for row in rows[:10]:
            cols = row.find_all("td")
            if len(cols) < 3:
                continue
            # 모든 컬럼 값 출력
            col_values = [c.text.strip() for c in cols]
            name_tag = cols[1].find("a") if len(cols) > 1 else None
            href = name_tag.get("href", "") if name_tag else ""
            code = href.split("code=")[-1] if "code=" in href else ""
            result.append({
                "code": code,
                "col_count": len(cols),
                "all_cols": col_values,
            })

        return {"rows": result}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/debug-cj")
def debug_cj():
    """CJ BB 계산 확인"""
    from datetime import datetime, timedelta
    date = datetime.today().strftime("%Y%m%d")
    d = datetime.strptime(date, "%Y%m%d")
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    date = d.strftime("%Y%m%d")
    start = (d - timedelta(days=90)).strftime("%Y%m%d")

    df = get_ohlc_history("001040", start, date)
    if df is None or df.empty:
        return {"error": "데이터 없음"}

    close_today = float(df["종가"].iloc[-1])
    open_s = df["시가"].dropna()
    close_s = df["종가"].dropna()

    bb1_upper, bb1_mid, bb1_lower = calc_bb(open_s, 4, 4.0)
    bb2_upper, bb2_mid, bb2_lower = calc_bb(close_s, 20, 2.0)

    return {
        "date": date,
        "close": close_today,
        "bb1": {"upper": round(bb1_upper), "mid": round(bb1_mid), "lower": round(bb1_lower), "below_lower": close_today < bb1_lower},
        "bb2": {"upper": round(bb2_upper), "mid": round(bb2_mid), "lower": round(bb2_lower), "below_lower": close_today < bb2_lower},
        "data_count": len(df),
    }

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
