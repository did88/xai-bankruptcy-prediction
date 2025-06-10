"""DART 기업코드 XML 구조 확인 스크립트"""

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
    
    print("\n📊 DataFrame 정보:")
    print(f"Shape: {corp_df.shape}")
    print(f"\n컬럼 목록: {list(corp_df.columns)}")
    print(f"\n처음 5개 행:")
    print(corp_df.head())
    
    print(f"\n데이터 타입:")
    print(corp_df.dtypes)
    
    # 주식 코드가 있는 기업만 확인
    if 'stock_code' in corp_df.columns:
        has_stock = corp_df['stock_code'].notna()
        print(f"\n주식 코드가 있는 기업: {has_stock.sum()}개")
    
    # corp_cls 대신 사용할 수 있는 컬럼 찾기
    for col in corp_df.columns:
        if 'corp' in col.lower() or 'cls' in col.lower() or 'class' in col.lower():
            print(f"\n'{col}' 컬럼의 고유값:")
            print(corp_df[col].value_counts().head(10))

if __name__ == "__main__":
    asyncio.run(main())