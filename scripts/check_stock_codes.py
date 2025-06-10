"""주식 코드 상세 확인 스크립트"""

import asyncio
import os
from pathlib import Path
from dart_bulk_downloader import fetch_corp_codes
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

async def main():
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        raise EnvironmentError("DART_API_KEY 환경변수를 설정하세요")
    
    print("📋 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    
    # stock_code 분석
    print("\n📊 Stock Code 분석:")
    print(f"전체 기업 수: {len(corp_df):,}")
    
    # 실제로 stock_code가 있는지 확인
    has_stock = corp_df['stock_code'].notna() & (corp_df['stock_code'].str.strip() != '')
    print(f"Stock code가 있는 기업: {has_stock.sum():,}")
    print(f"Stock code가 없는 기업: {(~has_stock).sum():,}")
    
    # stock_code 샘플 확인
    print("\n📋 Stock code 샘플 (처음 20개):")
    stock_sample = corp_df[has_stock][['corp_code', 'corp_name', 'stock_code']].head(20)
    print(stock_sample.to_string(index=False))
    
    # 금융 관련 기업 찾기
    print("\n💰 금융 관련 기업 샘플:")
    financial_keywords = "금융|은행|보험|증권|캐피탈|투자|자산운용|신용|저축|카드"
    is_financial = corp_df['corp_name'].str.contains(financial_keywords, na=False)
    financial_sample = corp_df[is_financial & has_stock][['corp_name', 'stock_code']].head(20)
    print(financial_sample.to_string(index=False))
    print(f"\n금융 관련 기업 수: {is_financial.sum():,}")
    
    # 주요 상장사 확인
    print("\n🏢 주요 상장사 확인:")
    major_corps = ['삼성전자', 'SK하이닉스', 'LG전자', '현대자동차', 'POSCO', 'NAVER', '카카오']
    for corp_name in major_corps:
        matches = corp_df[corp_df['corp_name'].str.contains(corp_name, na=False)]
        if not matches.empty:
            for _, row in matches.iterrows():
                print(f"{row['corp_name']}: {row['stock_code']}")
    
    # stock_code 길이 분석
    print("\n📏 Stock code 길이 분석:")
    valid_stocks = corp_df[has_stock]['stock_code'].str.strip()
    lengths = valid_stocks.str.len().value_counts().sort_index()
    print(lengths)
    
    # KOSPI/KOSDAQ 구분 가능한지 확인 (일반적으로 KOSPI는 6자리 숫자)
    print("\n🏛️ Stock code 패턴 분석:")
    six_digit = valid_stocks.str.match(r'^\d{6}$')
    print(f"6자리 숫자 코드: {six_digit.sum():,}개 (주로 KOSPI)")
    
    # 사용 가능한 기업 수
    print(f"\n✅ 사용 가능한 상장 기업 수: {has_stock.sum():,}")
    
if __name__ == "__main__":
    asyncio.run(main())
