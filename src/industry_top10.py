"""업종별 시가총액 상위 10종목 추출 스크립트."""

from __future__ import annotations

import pandas as pd


def export_industry_top10(
    input_excel: str,
    output_excel: str,
    industry_col: str = "업종",
    market_cap_col: str = "시가총액",
) -> None:
    """주어진 엑셀 파일에서 업종별 시가총액 상위 10개 종목을 계산해 저장한다.

    Parameters
    ----------
    input_excel : str
        분석할 원본 엑셀 파일 경로.
    output_excel : str
        업종별 Top 10 정보를 저장할 엑셀 파일 경로.
    industry_col : str, optional
        업종을 나타내는 컬럼명. 기본은 ``"업종"``.
    market_cap_col : str, optional
        시가총액을 나타내는 컬럼명. 기본은 ``"시가총액"``.
    """

    df = pd.read_excel(input_excel)

    # 시가총액을 숫자형으로 변환하고 결측값 제거
    df[market_cap_col] = pd.to_numeric(df[market_cap_col], errors="coerce")
    df = df.dropna(subset=[industry_col, market_cap_col])

    with pd.ExcelWriter(output_excel) as writer:
        for industry, group in df.groupby(industry_col):
            top10 = group.sort_values(market_cap_col, ascending=False).head(10)
            sheet_name = str(industry)[:31]  # 엑셀 시트명 제한
            top10.to_excel(writer, sheet_name=sheet_name, index=False)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="업종별 시가총액 Top 10 추출")
    parser.add_argument("input_excel", help="입력 엑셀 파일 경로")
    parser.add_argument("output_excel", help="출력 엑셀 파일 경로")
    parser.add_argument(
        "--industry-col",
        default="업종",
        help="업종 컬럼명 (기본: 업종)",
    )
    parser.add_argument(
        "--market-cap-col",
        default="시가총액",
        help="시가총액 컬럼명 (기본: 시가총액)",
    )
    args = parser.parse_args()

    export_industry_top10(
        args.input_excel,
        args.output_excel,
        industry_col=args.industry_col,
        market_cap_col=args.market_cap_col,
    )
