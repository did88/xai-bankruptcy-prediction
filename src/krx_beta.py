"""KRX 52주 베타 계산 모듈."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from pykrx import stock


def get_52week_beta(ticker: str, benchmark: str = "1028") -> float:
    """주어진 종목의 최근 52주 베타를 반환한다.

    Parameters
    ----------
    ticker : str
        KRX 종목 코드 (예: '005930').
    benchmark : str, optional
        비교할 지수 코드. 기본은 코스피200('1028').
    """
    end = datetime.today()
    start = end - timedelta(weeks=52)

    start_str = start.strftime("%Y%m%d")
    end_str = end.strftime("%Y%m%d")

    df_stock = stock.get_market_ohlcv_by_date(start_str, end_str, ticker)
    df_index = stock.get_index_ohlcv_by_date(start_str, end_str, benchmark)

    if df_stock.empty or df_index.empty:
        raise ValueError("주어진 기간에 데이터가 없습니다.")

    stock_ret = df_stock["종가"].pct_change().dropna()
    index_ret = df_index["종가"].pct_change().dropna()

    aligned = pd.concat([stock_ret, index_ret], axis=1, join="inner").dropna()
    cov = aligned.iloc[:, 0].cov(aligned.iloc[:, 1])
    var = aligned.iloc[:, 1].var()

    return float(cov / var)


if __name__ == "__main__":
    code = "005930"  # 삼성전자
    beta = get_52week_beta(code)
    print(f"{code} 52주 베타: {beta:.4f}")

