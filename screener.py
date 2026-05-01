import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APP_KEY    = os.environ.get("KIS_APP_KEY", "")
APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
# 9443 포트 대신 443 포트 사용
BASE_URL   = "https://openapi.koreainvestment.com:443"

_token_cache = {"token": None, "expires": None}

def get_access_token() -> str:
    now = datetime.now()
    if _token_cache["token"] and _token_cache["expires"] and now < _token_cache["expires"]:
        return _token_cache["token"]
    # 토큰 발급은 9443으로
    url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
    body = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    resp = requests.post(url, json=body, timeout=15)
    data = resp.json()
    token = data.get("access_token", "")
    _token_cache["token"] = token
    _token_cache["expires"] = now + timedelta(hours=23)
    logger.info("토큰 발급 완료")
    return token

def kis_get(path: str, params: dict, tr_id: str, timeout: int = 20) -> dict:
    token = get_access_token()
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": tr_id,
        "custtype": "P",
        "Content-Type": "application/json; charset=utf-8"
    }
    # 443 포트로 시도, 실패하면 9443으로 재시도
    for base in [BASE_URL, "https://openapi.koreainvestment.com:9443"]:
        try:
            url = f"{base}{path}"
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200 and resp.text:
                return resp.json()
        except requests.exceptions.Timeout:
            logger.warning(f"타임아웃: {base}{path}")
        except Exception as e:
            logger.warning(f"오류 [{base}]: {e}")
    return {}

def is_valid_code(code: str) -> bool:
    """정상 종목코드 여부 확인 (6자리 숫자)"""
    if not code:
        return False
    code = code.strip()
    return len(code) == 6 and code.isdigit()

def get_top100_by_amount(market: str) -> list:
    """거래대금 순위 상위 100개 - 정상 종목코드만 필터링"""
    mkt_code = "J" if market == "KOSPI" else "NQ"
    result = []

    for _ in range(5):  # 최대 5번 호출로 100개 확보
        data = kis_get(
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            params={
                "FID_COND_MRKT_DIV_CODE": mkt_code,
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
        items = data.get("output", [])
        logger.info(f"[{market}] volume-rank: {len(items)}개, rt_cd={data.get('rt_cd')}")

        for item in items:
            code = item.get("mksc_shrn_iscd", "").strip()
            name = item.get("hts_kor_isnm", code).strip()

            # 비정상 코드 필터링 (ETF/ETN/스팩 등 제외하지 않고 코드 형식만 확인)
            if not is_valid_code(code):
                logger.debug(f"비정상 코드 스킵: {code} ({name})")
                continue

            try:
                amount_raw = float(str(item.get("acml_tr_pbmn", "0")).replace(",", ""))
            except:
                amount_raw = 0

            if amount_raw >= 100000000:
                amount_str = f"{int(amount_raw)//100000000:,}억"
            elif amount_raw >= 10000:
                amount_str = f"{int(amount_raw)//10000:,}만"
            else:
                amount_str = "-"

            # 중복 제거
            if code not in [r[0] for r in result]:
                result.append((code, name, market, amount_str))

        if len(result) >= 100 or len(items) == 0:
            break
        time.sleep(0.3)

    logger.info(f"[{market}] 유효 종목 수집: {len(result[:100])}개")
    return result[:100]

def get_ohlc_history(ticker: str, start: str, end: str, market: str) -> pd.DataFrame:
    """
    일봉 OHLC - 기간별 시세 조회
    tr_id: FHKST03010100
    """
    mkt_code = "J" if market == "KOSPI" else "NQ"

    data = kis_get(
        "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
        params={
            "fid_cond_mrkt_div_code": mkt_code,
            "fid_input_iscd": ticker,
            "fid_input_date_1": start,
            "fid_input_date_2": end,
            "fid_period_div_code": "D",
            "fid_org_adj_prc": "0"
        },
        tr_id="FHKST03010100",
        timeout=20
    )

    items = data.get("output2", [])

    # output2 없으면 output1 시도
    if not items:
        items = data.get("output1", [])

    if not items:
        # 대안: 최근 30일 API
        data2 = kis_get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            params={
                "fid_cond_mrkt_div_code": mkt_code,
                "fid_input_iscd": ticker,
                "fid_period_div_code": "D",
                "fid_org_adj_prc": "0"
            },
            tr_id="FHKST01010400",
            timeout=15
        )
        items = data2.get("output", [])

    if not items:
        return pd.DataFrame()

    rows = []
    for item in items:
        try:
            date_val = item.get("stck_bsop_date", "")
            open_p   = float(str(item.get("stck_oprc","0")).replace(",","") or "0")
            close_p  = float(str(item.get("stck_clpr","0")).replace(",","") or "0")
            high_p   = float(str(item.get("stck_hgpr","0")).replace(",","") or "0")
            low_p    = float(str(item.get("stck_lwpr","0")).replace(",","") or "0")
            if close_p > 0:
                rows.append({"날짜": date_val, "시가": open_p, "고가": high_p, "저가": low_p, "종가": close_p})
        except:
            continue

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("날짜").reset_index(drop=True)
    # 요청 기간 필터
    df = df[(df["날짜"] >= start) & (df["날짜"] <= end)]
    return df

def calc_bb(series, period, multiplier):
    mid = series.rolling(window=period).mean()
    std = series.rolling(window=period).std(ddof=0)
    return float((mid+multiplier*std).iloc[-1]), float(mid.iloc[-1]), float((mid-multiplier*std).iloc[-1])

def is_near(price, target, proximity_pct):
    if not target or np.isnan(target) or target == 0:
        return False
    return abs(price - target) / abs(target) * 100 <= proximity_pct

def classify_conditions(price, bb_lower, bb_mid, proximity_pct):
    conditions = []
    if np.isnan(bb_lower) or np.isnan(bb_mid):
        return conditions
    if price < bb_lower:
        conditions.append("하한 이탈")
    elif is_near(price, bb_lower, proximity_pct):
        conditions.append("하한 근접")
    if price < bb_mid:
        conditions.append("중간선 하회")
    elif is_near(price, bb_mid, proximity_pct):
        conditions.append("중간선 근접")
    return conditions

def get_prev_business_day(date):
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date

def run_screener(proximity: float = 3.0, date_str_input: str = None):
    today = datetime.today()
    if date_str_input and len(date_str_input) == 8:
        try:
            today = datetime.strptime(date_str_input, "%Y%m%d")
        except Exception as e:
            logger.warning(f"날짜 파싱 실패: {e}")

    today = get_prev_business_day(today)
    date_str  = today.strftime("%Y%m%d")
    start_str = (today - timedelta(days=90)).strftime("%Y%m%d")
    logger.info(f"분석날짜: {date_str}, 근접기준: {proximity}%")

    result = {
        "analysisDate": today.strftime("%Y-%m-%d"),
        "proximity": proximity,
        "bb1": {"label": "BB1 (시가MA4·SD×4)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "bb2": {"label": "BB2 (종가MA20·SD×2)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "overlap_lower": [], "overlap_mid": [],
        "summary": {"total_analyzed": 0, "bb1_matched": 0, "bb2_matched": 0, "overlap_lower_count": 0, "overlap_mid_count": 0}
    }

    all_tickers = []
    for market_name in ["KOSPI", "KOSDAQ"]:
        tickers = get_top100_by_amount(market_name)
        all_tickers.extend(tickers)
        time.sleep(0.5)

    result["summary"]["total_analyzed"] = len(all_tickers)
    logger.info(f"전체 분석 대상: {len(all_tickers)}개")
    if not all_tickers:
        logger.error("종목 목록 비어있음!")
        return result

    for idx, (ticker, name, market, amount_str) in enumerate(all_tickers):
        try:
            df = get_ohlc_history(ticker, start_str, date_str, market)
            if df is None or df.empty or len(df) < 4:
                continue
            if "종가" not in df.columns or "시가" not in df.columns:
                continue
            close_today = float(df["종가"].iloc[-1])

            open_series = df["시가"].dropna()
            if len(open_series) >= 4:
                _, bb1_mid, bb1_lower = calc_bb(open_series, 4, 4.0)
                bb1_conds = classify_conditions(close_today, bb1_lower, bb1_mid, proximity)
                si = {"name": name, "code": ticker, "market": market, "price": close_today,
                      "amount": amount_str, "bb_lower": round(bb1_lower,0), "bb_mid": round(bb1_mid,0)}
                m1 = False
                if "하한 이탈" in bb1_conds:
                    result["bb1"]["below_lower"].append({**si, "condition": "하한 이탈"}); m1=True
                elif "하한 근접" in bb1_conds:
                    result["bb1"]["near_lower"].append({**si, "condition": "하한 근접"}); m1=True
                if "중간선 하회" in bb1_conds:
                    result["bb1"]["below_mid"].append({**si, "condition": "중간선 하회"}); m1=True
                elif "중간선 근접" in bb1_conds:
                    result["bb1"]["near_mid"].append({**si, "condition": "중간선 근접"}); m1=True
                if m1: result["summary"]["bb1_matched"] += 1

            close_series = df["종가"].dropna()
            if len(close_series) >= 20:
                _, bb2_mid, bb2_lower = calc_bb(close_series, 20, 2.0)
                bb2_conds = classify_conditions(close_today, bb2_lower, bb2_mid, proximity)
                si2 = {"name": name, "code": ticker, "market": market, "price": close_today,
                       "amount": amount_str, "bb_lower": round(bb2_lower,0), "bb_mid": round(bb2_mid,0)}
                m2 = False
                if "하한 이탈" in bb2_conds:
                    result["bb2"]["below_lower"].append({**si2, "condition": "하한 이탈"}); m2=True
                elif "하한 근접" in bb2_conds:
                    result["bb2"]["near_lower"].append({**si2, "condition": "하한 근접"}); m2=True
                if "중간선 하회" in bb2_conds:
                    result["bb2"]["below_mid"].append({**si2, "condition": "중간선 하회"}); m2=True
                elif "중간선 근접" in bb2_conds:
                    result["bb2"]["near_mid"].append({**si2, "condition": "중간선 근접"}); m2=True
                if m2: result["summary"]["bb2_matched"] += 1

            if idx % 10 == 0:
                logger.info(f"진행: {idx+1}/{len(all_tickers)}")
            time.sleep(0.2)

        except Exception as e:
            logger.error(f"[{ticker}] 오류: {e}")
            continue

    logger.info(f"BB1: {result['summary']['bb1_matched']}개, BB2: {result['summary']['bb2_matched']}개")

    bb1_lower_set = set(s["code"] for s in result["bb1"]["below_lower"]+result["bb1"]["near_lower"])
    bb2_lower_set = set(s["code"] for s in result["bb2"]["below_lower"]+result["bb2"]["near_lower"])
    bb1_mid_set   = set(s["code"] for s in result["bb1"]["below_mid"]+result["bb1"]["near_mid"])
    bb2_mid_set   = set(s["code"] for s in result["bb2"]["below_mid"]+result["bb2"]["near_mid"])

    def find_stock_info(code, bb_section):
        for cat in ["below_lower","near_lower","below_mid","near_mid"]:
            for s in bb_section[cat]:
                if s["code"] == code:
                    return s, s.get("condition","")
        return None, ""

    for code in bb1_lower_set & bb2_lower_set:
        s1,c1 = find_stock_info(code, result["bb1"])
        _,c2  = find_stock_info(code, result["bb2"])
        if s1: result["overlap_lower"].append({"name":s1["name"],"code":code,"market":s1["market"],
            "price":s1["price"],"amount":s1["amount"],"bb1_condition":c1,"bb2_condition":c2})

    for code in bb1_mid_set & bb2_mid_set:
        s1,c1 = find_stock_info(code, result["bb1"])
        _,c2  = find_stock_info(code, result["bb2"])
        if s1: result["overlap_mid"].append({"name":s1["name"],"code":code,"market":s1["market"],
            "price":s1["price"],"amount":s1["amount"],"bb1_condition":c1,"bb2_condition":c2})

    result["summary"]["overlap_lower_count"] = len(result["overlap_lower"])
    result["summary"]["overlap_mid_count"]   = len(result["overlap_mid"])
    logger.info(f"겹치는 종목 - 하한: {result['summary']['overlap_lower_count']}개, 중간: {result['summary']['overlap_mid_count']}개")
    return result
