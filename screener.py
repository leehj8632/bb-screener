from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_prev_business_day(date: datetime) -> datetime:
    while date.weekday() >= 5:
        date -= timedelta(days=1)
    return date

def get_top100_by_amount(market: str, date: str) -> list:
    """거래대금 상위 100 종목 반환"""
    try:
        # 전종목 시세 (거래량 포함)
        df_ohlcv = stock.get_market_ohlcv_by_ticker(date, market=market)
        logger.info(f"[{market}] ohlcv 컬럼: {df_ohlcv.columns.tolist()}")

        # 거래대금 = 거래량 × 종가 로 계산 (거래대금 컬럼 없을 때)
        if "거래대금" not in df_ohlcv.columns:
            if "거래량" in df_ohlcv.columns and "종가" in df_ohlcv.columns:
                df_ohlcv["거래대금"] = df_ohlcv["거래량"] * df_ohlcv["종가"]
            else:
                logger.error(f"[{market}] 거래대금 계산 불가: {df_ohlcv.columns.tolist()}")
                return []

        df_sorted = df_ohlcv.sort_values("거래대금", ascending=False).head(100)
        logger.info(f"[{market}] 거래대금 상위 100 수집 완료")

        result = []
        for ticker in df_sorted.index:
            try:
                name = stock.get_market_ticker_name(ticker)
            except:
                name = ticker
            amount_raw = int(df_sorted.loc[ticker, "거래대금"])
            if amount_raw >= 100000000:
                amount_str = f"{amount_raw // 100000000:,}억"
            elif amount_raw >= 10000:
                amount_str = f"{amount_raw // 10000:,}만"
            else:
                amount_str = "-"
            result.append((ticker, name, market, amount_str))

        return result

    except Exception as e:
        logger.error(f"[{market}] get_top100 오류: {e}")
        return []

def get_ohlc_history(ticker: str, start: str, end: str) -> pd.DataFrame:
    """일봉 OHLC 조회"""
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        if df is None or df.empty:
            return pd.DataFrame()
        # 컬럼 확인
        needed = ["시가", "종가"]
        if not all(c in df.columns for c in needed):
            logger.warning(f"[{ticker}] 컬럼 부족: {df.columns.tolist()}")
            return pd.DataFrame()
        return df
    except Exception as e:
        logger.error(f"[{ticker}] OHLC 오류: {e}")
        return pd.DataFrame()

def calc_bb(series: pd.Series, period: int, multiplier: float):
    mid = series.rolling(window=period).mean()
    std = series.rolling(window=period).std(ddof=0)
    upper = mid + multiplier * std
    lower = mid - multiplier * std
    return float(upper.iloc[-1]), float(mid.iloc[-1]), float(lower.iloc[-1])

def is_near(price: float, target: float, proximity_pct: float) -> bool:
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
        tickers = get_top100_by_amount(market_name, date_str)
        logger.info(f"[{market_name}] 수집: {len(tickers)}개")
        all_tickers.extend(tickers)
        time.sleep(0.5)

    result["summary"]["total_analyzed"] = len(all_tickers)
    logger.info(f"전체 분석 대상: {len(all_tickers)}개")

    if not all_tickers:
        logger.error("종목 목록 비어있음!")
        return result

    for idx, (ticker, name, market, amount_str) in enumerate(all_tickers):
        try:
            df = get_ohlc_history(ticker, start_str, date_str)
            if df is None or df.empty or len(df) < 4:
                continue

            close_today = float(df["종가"].iloc[-1])

            # BB1: 시가 기준, MA4, SD×4
            open_series = df["시가"].dropna()
            if len(open_series) >= 4:
                _, bb1_mid, bb1_lower = calc_bb(open_series, 4, 4.0)
                bb1_conds = classify_conditions(close_today, bb1_lower, bb1_mid, proximity)
                si = {"name": name, "code": ticker, "market": market, "price": close_today,
                      "amount": amount_str, "bb_lower": round(bb1_lower, 0), "bb_mid": round(bb1_mid, 0)}
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

            # BB2: 종가 기준, MA20, SD×2
            close_series = df["종가"].dropna()
            if len(close_series) >= 20:
                _, bb2_mid, bb2_lower = calc_bb(close_series, 20, 2.0)
                bb2_conds = classify_conditions(close_today, bb2_lower, bb2_mid, proximity)
                si2 = {"name": name, "code": ticker, "market": market, "price": close_today,
                       "amount": amount_str, "bb_lower": round(bb2_lower, 0), "bb_mid": round(bb2_mid, 0)}
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

            if idx % 20 == 0:
                logger.info(f"진행: {idx+1}/{len(all_tickers)}")
            time.sleep(0.15)

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
        if s1: result["overlap_lower"].append({
            "name":s1["name"],"code":code,"market":s1["market"],
            "price":s1["price"],"amount":s1["amount"],
            "bb1_condition":c1,"bb2_condition":c2})

    for code in bb1_mid_set & bb2_mid_set:
        s1,c1 = find_stock_info(code, result["bb1"])
        _,c2  = find_stock_info(code, result["bb2"])
        if s1: result["overlap_mid"].append({
            "name":s1["name"],"code":code,"market":s1["market"],
            "price":s1["price"],"amount":s1["amount"],
            "bb1_condition":c1,"bb2_condition":c2})

    result["summary"]["overlap_lower_count"] = len(result["overlap_lower"])
    result["summary"]["overlap_mid_count"]   = len(result["overlap_mid"])
    logger.info(f"겹치는 종목 - 하한: {result['summary']['overlap_lower_count']}개, 중간: {result['summary']['overlap_mid_count']}개")
    return result
