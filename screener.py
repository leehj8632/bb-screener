from pykrx import stock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time

def get_top100_by_amount(market: str, date: str) -> list:
    """거래대금 상위 100 종목 코드 반환"""
    try:
        df = stock.get_market_trading_value_by_ticker(date, market=market)
        df = df.sort_values("거래대금", ascending=False).head(100)
        return df.index.tolist()
    except:
        return []

def get_ohlc(ticker: str, start: str, end: str) -> pd.DataFrame:
    """일봉 OHLC 데이터 반환"""
    try:
        df = stock.get_market_ohlcv_by_date(start, end, ticker)
        return df
    except:
        return pd.DataFrame()

def calc_bb(series: pd.Series, period: int, multiplier: float):
    """볼린저밴드 계산: (상한, 중간, 하한)"""
    mid = series.rolling(window=period).mean()
    std = series.rolling(window=period).std(ddof=0)
    upper = mid + multiplier * std
    lower = mid - multiplier * std
    return upper.iloc[-1], mid.iloc[-1], lower.iloc[-1]

def is_near(price: float, target: float, proximity_pct: float) -> bool:
    if target == 0:
        return False
    return abs(price - target) / abs(target) * 100 <= proximity_pct

def classify_conditions(price, bb_lower, bb_mid, proximity_pct):
    conditions = []
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
    if date_str_input:
        try:
            today = datetime.strptime(date_str_input, "%Y%m%d")
        except:
            pass

    # 주말이면 금요일로 조정
    if today.weekday() == 5:
        today -= timedelta(days=1)
    elif today.weekday() == 6:
        today -= timedelta(days=2)

    date_str = today.strftime("%Y%m%d")
    start_str = (today - timedelta(days=60)).strftime("%Y%m%d")  # BB20 계산용 여유분

    markets = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ"}

    result = {
        "analysisDate": today.strftime("%Y-%m-%d"),
        "proximity": proximity,
        "bb1": {"label": "BB1 (시가MA4·SD×4)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "bb2": {"label": "BB2 (종가MA20·SD×2)", "below_lower": [], "near_lower": [], "below_mid": [], "near_mid": []},
        "overlap_lower": [],
        "overlap_mid": [],
        "summary": {"total_analyzed": 0, "bb1_matched": 0, "bb2_matched": 0, "overlap_lower_count": 0, "overlap_mid_count": 0}
    }

    all_tickers = []

    for market_name, market_code in markets.items():
        tickers = get_top100_by_amount(market_code, date_str)
        for t in tickers:
            all_tickers.append((t, market_name))
        time.sleep(0.3)

    result["summary"]["total_analyzed"] = len(all_tickers)

    # 종목명 일괄 조회
    name_map = {}
    try:
        name_map_kospi = stock.get_market_ticker_name
        for t, m in all_tickers:
            try:
                name_map[t] = stock.get_market_ticker_name(t)
            except:
                name_map[t] = t
    except:
        pass

    bb1_results = {}
    bb2_results = {}

    for ticker, market in all_tickers:
        try:
            df = get_ohlc(ticker, start_str, date_str)
            if df is None or len(df) < 20:
                continue

            name = name_map.get(ticker, ticker)
            close_today = float(df["종가"].iloc[-1])

            # 거래대금 (오늘)
            try:
                amount_df = stock.get_market_trading_value_by_date(date_str, date_str, ticker)
                amount_raw = int(amount_df["거래대금"].iloc[-1]) if not amount_df.empty else 0
                amount_str = f"{amount_raw // 100000000:,}억" if amount_raw >= 100000000 else f"{amount_raw // 10000:,}만"
            except:
                amount_str = "-"

            # BB1: 시가 기준, MA4, SD×4
            if len(df) >= 4:
                open_series = df["시가"]
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
                    bb1_results[ticker] = bb1_conds

            # BB2: 종가 기준, MA20, SD×2
            if len(df) >= 20:
                close_series = df["종가"]
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
                    bb2_results[ticker] = bb2_conds

            time.sleep(0.1)

        except Exception as e:
            continue

    # 겹치는 종목 계산
    bb1_lower_set = set(
        s["code"] for s in result["bb1"]["below_lower"] + result["bb1"]["near_lower"]
    )
    bb2_lower_set = set(
        s["code"] for s in result["bb2"]["below_lower"] + result["bb2"]["near_lower"]
    )
    bb1_mid_set = set(
        s["code"] for s in result["bb1"]["below_mid"] + result["bb1"]["near_mid"]
    )
    bb2_mid_set = set(
        s["code"] for s in result["bb2"]["below_mid"] + result["bb2"]["near_mid"]
    )

    overlap_lower_codes = bb1_lower_set & bb2_lower_set
    overlap_mid_codes = bb1_mid_set & bb2_mid_set

    def find_stock_info(code, bb_section):
        for cat in ["below_lower", "near_lower", "below_mid", "near_mid"]:
            for s in bb_section[cat]:
                if s["code"] == code:
                    return s, s.get("condition", "")
        return None, ""

    for code in overlap_lower_codes:
        s1, c1 = find_stock_info(code, result["bb1"])
        s2, c2 = find_stock_info(code, result["bb2"])
        if s1:
            result["overlap_lower"].append({
                "name": s1["name"], "code": code, "market": s1["market"],
                "price": s1["price"], "amount": s1["amount"],
                "bb1_condition": c1, "bb2_condition": c2
            })

    for code in overlap_mid_codes:
        s1, c1 = find_stock_info(code, result["bb1"])
        s2, c2 = find_stock_info(code, result["bb2"])
        if s1:
            result["overlap_mid"].append({
                "name": s1["name"], "code": code, "market": s1["market"],
                "price": s1["price"], "amount": s1["amount"],
                "bb1_condition": c1, "bb2_condition": c2
            })

    result["summary"]["overlap_lower_count"] = len(result["overlap_lower"])
    result["summary"]["overlap_mid_count"] = len(result["overlap_mid"])

    return result
