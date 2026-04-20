import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KRX_API_KEY = os.environ.get("KRX_API_KEY", "")
KRX_BASE_URL = "http://data-dbg.krx.co.kr/svc/apis"

def get_prev_business_day(date: datetime) -> datetime:
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date

def krx_get(endpoint: str, params: dict) -> pd.DataFrame:
    """KRX Open API 호출"""
    headers = {"AUTH_KEY": KRX_API_KEY}
    url = f"{KRX_BASE_URL}/{endpoint}"
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        data = resp.json()
        logger.info(f"KRX API 응답 키: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        # OutBlock_1 키로 데이터 반환
        if "OutBlock_1" in data:
            return pd.DataFrame(data["OutBlock_1"])
        else:
            logger.warning(f"OutBlock_1 없음. 응답: {str(data)[:200]}")
            return pd.DataFrame()
    except Exception as e:
        logger.error(f"KRX API 오류 [{endpoint}]: {e}")
        return pd.DataFrame()

def get_top100_by_amount(market: str, date: str) -> pd.DataFrame:
    """
    거래대금 상위 100 종목 반환
    KRX API: stk/stk_dd_trd (주식 일별 거래 현황)
    market: KOSPI -> STK, KOSDAQ -> KSQ
    """
    mkt_map = {"KOSPI": "STK", "KOSDAQ": "KSQ"}
    mkt_id = mkt_map.get(market, "STK")

    # 주식 일별 전종목 시세
    df = krx_get("sto/stk_dd_trd", {"basDd": date, "mktId": mkt_id})

    if df.empty:
        # 대안 엔드포인트 시도
        df = krx_get("sto/stk_bydd_trd", {"basDd": date, "mktId": mkt_id})

    logger.info(f"[{market}] 전종목 시세 조회: {len(df)}개, 컬럼: {df.columns.tolist()[:8] if not df.empty else '없음'}")

    if df.empty:
        return pd.DataFrame()

    # 거래대금 컬럼 찾기 (TRAD_AMT, 거래대금 등)
    amount_col = None
    for col in ["TRAD_AMT", "ACC_TRDVAL", "거래대금", "TRDVAL"]:
        if col in df.columns:
            amount_col = col
            break

    if not amount_col:
        logger.warning(f"거래대금 컬럼 없음. 컬럼 목록: {df.columns.tolist()}")
        return pd.DataFrame()

    # 종목코드 컬럼 찾기
    code_col = None
    for col in ["ISU_SRT_CD", "SHOTN_ISIN", "종목코드", "ISIN"]:
        if col in df.columns:
            code_col = col
            break

    # 종목명 컬럼 찾기
    name_col = None
    for col in ["ISU_ABBRV", "ISU_NM", "종목명"]:
        if col in df.columns:
            name_col = col
            break

    df[amount_col] = pd.to_numeric(df[amount_col].astype(str).str.replace(",", ""), errors="coerce").fillna(0)
    df = df.sort_values(amount_col, ascending=False).head(100)

    return df, code_col, name_col, amount_col

def get_ohlc_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    """
    종목 일별 OHLC 조회
    KRX API: sto/stk_bydd_trd
    """
    df = krx_get("sto/stk_bydd_trd", {"isinCd": ticker, "strtDd": start, "endDd": end})

    if df.empty:
        return pd.DataFrame()

    logger.debug(f"[{ticker}] OHLC 컬럼: {df.columns.tolist()}")

    # 컬럼 매핑
    col_map = {}
    for target, candidates in [
        ("날짜",   ["BAS_DD", "TRD_DD"]),
        ("시가",   ["OPN_PRC", "OPNPRC"]),
        ("고가",   ["HGH_PRC", "HGHPRC"]),
        ("저가",   ["LOW_PRC", "LOWPRC"]),
        ("종가",   ["CLS_PRC", "CLSPRC", "TDD_CLSPRC"]),
        ("거래량", ["ACC_TRDVOL", "TRDVOL"]),
        ("거래대금", ["ACC_TRDVAL", "TRAD_AMT", "TRDVAL"]),
    ]:
        for c in candidates:
            if c in df.columns:
                col_map[c] = target
                break

    df = df.rename(columns=col_map)

    # 숫자 변환
    for col in ["시가", "고가", "저가", "종가", "거래량", "거래대금"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(",", ""), errors="coerce")

    if "날짜" in df.columns:
        df = df.sort_values("날짜").reset_index(drop=True)

    return df

def calc_bb(series: pd.Series, period: int, multiplier: float):
    mid = series.rolling(window=period).mean()
    std = series.rolling(window=period).std(ddof=0)
    upper = mid + multiplier * std
    lower = mid - multiplier * std
    return float(upper.iloc[-1]), float(mid.iloc[-1]), float(lower.iloc[-1])

def is_near(price: float, target: float, proximity_pct: float) -> bool:
    if target == 0 or np.isnan(target):
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

def run_screener(proximity: float = 3.0, date_str_input: str = None):
    today = datetime.today()
    if date_str_input and len(date_str_input) == 8:
        try:
            today = datetime.strptime(date_str_input, "%Y%m%d")
        except Exception as e:
            logger.warning(f"날짜 파싱 실패: {e}")

    today = get_prev_business_day(today)
    date_str = today.strftime("%Y%m%d")
    start_str = (today - timedelta(days=90)).strftime("%Y%m%d")

    logger.info(f"분석날짜: {date_str}, 근접기준: {proximity}%")

    result = {
        "analysisDate": today.strftime("%Y-%m-%d"),
        "proximity": proximity,
        "bb1": {"label": "BB1 (시가MA4·SD×4)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "bb2": {"label": "BB2 (종가MA20·SD×2)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "overlap_lower": [],
        "overlap_mid": [],
        "summary": {"total_analyzed": 0, "bb1_matched": 0, "bb2_matched": 0, "overlap_lower_count": 0, "overlap_mid_count": 0}
    }

    all_tickers = []  # [(code, name, market, amount_str), ...]

    for market_name in ["KOSPI", "KOSDAQ"]:
        try:
            ret = get_top100_by_amount(market_name, date_str)
            if isinstance(ret, tuple):
                df, code_col, name_col, amount_col = ret
            else:
                logger.warning(f"[{market_name}] 데이터 없음")
                continue

            if df.empty or not code_col:
                logger.warning(f"[{market_name}] 종목 데이터 없음")
                continue

            for _, row in df.iterrows():
                code = str(row[code_col]).zfill(6)
                name = str(row[name_col]) if name_col else code
                amount_raw = float(row[amount_col]) if amount_col else 0
                if amount_raw >= 100000000:
                    amount_str = f"{int(amount_raw) // 100000000:,}억"
                elif amount_raw >= 10000:
                    amount_str = f"{int(amount_raw) // 10000:,}만"
                else:
                    amount_str = "-"
                all_tickers.append((code, name, market_name, amount_str))

            logger.info(f"[{market_name}] 수집: {len([t for t in all_tickers if t[2]==market_name])}개")
        except Exception as e:
            logger.error(f"[{market_name}] 수집 오류: {e}")
        time.sleep(0.5)

    result["summary"]["total_analyzed"] = len(all_tickers)
    logger.info(f"전체 분석 대상: {len(all_tickers)}개")

    if len(all_tickers) == 0:
        logger.error("종목 목록 비어있음!")
        return result

    for idx, (ticker, name, market, amount_str) in enumerate(all_tickers):
        try:
            df = get_ohlc_history(ticker, start_str, date_str)
            if df is None or df.empty or len(df) < 4:
                continue
            if "종가" not in df.columns or "시가" not in df.columns:
                continue

            close_today = float(df["종가"].iloc[-1])

            # BB1: 시가 기준, MA4, SD×4
            open_series = df["시가"].dropna()
            if len(open_series) >= 4:
                bb1_upper, bb1_mid, bb1_lower = calc_bb(open_series, 4, 4.0)
                bb1_conds = classify_conditions(close_today, bb1_lower, bb1_mid, proximity)
                stock_info = {
                    "name": name, "code": ticker, "market": market,
                    "price": close_today, "amount": amount_str,
                    "bb_lower": round(bb1_lower, 0), "bb_mid": round(bb1_mid, 0)
                }
                matched_bb1 = False
                if "하한 이탈" in bb1_conds:
                    result["bb1"]["below_lower"].append({**stock_info, "condition": "하한 이탈"})
                    matched_bb1 = True
                elif "하한 근접" in bb1_conds:
                    result["bb1"]["near_lower"].append({**stock_info, "condition": "하한 근접"})
                    matched_bb1 = True
                if "중간선 하회" in bb1_conds:
                    result["bb1"]["below_mid"].append({**stock_info, "condition": "중간선 하회"})
                    matched_bb1 = True
                elif "중간선 근접" in bb1_conds:
                    result["bb1"]["near_mid"].append({**stock_info, "condition": "중간선 근접"})
                    matched_bb1 = True
                if matched_bb1:
                    result["summary"]["bb1_matched"] += 1

            # BB2: 종가 기준, MA20, SD×2
            close_series = df["종가"].dropna()
            if len(close_series) >= 20:
                bb2_upper, bb2_mid, bb2_lower = calc_bb(close_series, 20, 2.0)
                bb2_conds = classify_conditions(close_today, bb2_lower, bb2_mid, proximity)
                stock_info2 = {
                    "name": name, "code": ticker, "market": market,
                    "price": close_today, "amount": amount_str,
                    "bb_lower": round(bb2_lower, 0), "bb_mid": round(bb2_mid, 0)
                }
                matched_bb2 = False
                if "하한 이탈" in bb2_conds:
                    result["bb2"]["below_lower"].append({**stock_info2, "condition": "하한 이탈"})
                    matched_bb2 = True
                elif "하한 근접" in bb2_conds:
                    result["bb2"]["near_lower"].append({**stock_info2, "condition": "하한 근접"})
                    matched_bb2 = True
                if "중간선 하회" in bb2_conds:
                    result["bb2"]["below_mid"].append({**stock_info2, "condition": "중간선 하회"})
                    matched_bb2 = True
                elif "중간선 근접" in bb2_conds:
                    result["bb2"]["near_mid"].append({**stock_info2, "condition": "중간선 근접"})
                    matched_bb2 = True
                if matched_bb2:
                    result["summary"]["bb2_matched"] += 1

            if idx % 20 == 0:
                logger.info(f"진행: {idx+1}/{len(all_tickers)}")
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"[{ticker}] 오류: {e}")
            continue

    logger.info(f"BB1: {result['summary']['bb1_matched']}개, BB2: {result['summary']['bb2_matched']}개")

    # 겹치는 종목
    bb1_lower_set = set(s["code"] for s in result["bb1"]["below_lower"] + result["bb1"]["near_lower"])
    bb2_lower_set = set(s["code"] for s in result["bb2"]["below_lower"] + result["bb2"]["near_lower"])
    bb1_mid_set   = set(s["code"] for s in result["bb1"]["below_mid"]   + result["bb1"]["near_mid"])
    bb2_mid_set   = set(s["code"] for s in result["bb2"]["below_mid"]   + result["bb2"]["near_mid"])

    def find_stock_info(code, bb_section):
        for cat in ["below_lower", "near_lower", "below_mid", "near_mid"]:
            for s in bb_section[cat]:
                if s["code"] == code:
                    return s, s.get("condition", "")
        return None, ""

    for code in bb1_lower_set & bb2_lower_set:
        s1, c1 = find_stock_info(code, result["bb1"])
        _, c2  = find_stock_info(code, result["bb2"])
        if s1:
            result["overlap_lower"].append({
                "name": s1["name"], "code": code, "market": s1["market"],
                "price": s1["price"], "amount": s1["amount"],
                "bb1_condition": c1, "bb2_condition": c2
            })

    for code in bb1_mid_set & bb2_mid_set:
        s1, c1 = find_stock_info(code, result["bb1"])
        _, c2  = find_stock_info(code, result["bb2"])
        if s1:
            result["overlap_mid"].append({
                "name": s1["name"], "code": code, "market": s1["market"],
                "price": s1["price"], "amount": s1["amount"],
                "bb1_condition": c1, "bb2_condition": c2
            })

    result["summary"]["overlap_lower_count"] = len(result["overlap_lower"])
    result["summary"]["overlap_mid_count"]   = len(result["overlap_mid"])
    logger.info(f"겹치는 종목 - 하한: {result['summary']['overlap_lower_count']}개, 중간: {result['summary']['overlap_mid_count']}개")

    return result
