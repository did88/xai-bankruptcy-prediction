"""성공 확인된 기업들로 DART 다운로드 테스트"""

import asyncio
import os
from pathlib import Path
from dart_bulk_downloader import fetch_bulk_statements, save_to_excel
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

async def test_known_good_companies():
    """성공이 확인된 기업들로 테스트"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    # 테스트에서 성공한 기업들
    known_good_corps = [
        ("00119195", "동화약품"),
        ("00112378", "KR모터스"),
        ("00101628", "경방"),
        ("00126937", "삼양홀딩스"),
        ("00150244", "하이트진로"),
    ]
    
    corp_codes = [corp[0] for corp in known_good_corps]
    corp_names = [corp[1] for corp in known_good_corps]
    
    print(f"🚀 성공 확인된 {len(corp_codes)}개 기업으로 다운로드 테스트")
    print(f"기업 목록: {', '.join(corp_names)}")
    
    years = range(2020, 2023)  # 2020-2022
    
    print(f"📅 연도 범위: {list(years)}")
    print(f"📊 예상 요청 수: {len(corp_codes)} × {len(years)} = {len(corp_codes) * len(years)}개")
    
    # 다운로드 실행 (기업명 포함)
    statements = await fetch_bulk_statements(api_key, corp_codes, years, workers=5, include_corp_names=True)
    
    print(f"\n📋 결과:")
    print(f"   - 수집된 총 행 수: {len(statements):,}")
    
    if not statements.empty:
        print(f"   - 컬럼: {list(statements.columns)}")
        print(f"   - 고유 기업 수: {statements['corp_code'].nunique()}")
        print(f"   - 고유 연도 수: {statements['bsns_year'].nunique()}")
        print(f"   - 재무제표 구분: {statements['fs_div'].value_counts().to_dict()}")
        
        print(f"\n📊 기업별 데이터 수:")
        corp_counts = statements.groupby('corp_code').size()
        for corp_code, count in corp_counts.items():
            corp_name = next((name for code, name in known_good_corps if code == corp_code), "Unknown")
            print(f"   - {corp_name} ({corp_code}): {count}행")
        
        print(f"\n📅 연도별 데이터 수:")
        year_counts = statements.groupby('bsns_year').size()
        for year, count in year_counts.items():
            print(f"   - {year}년: {count}행")
        
        # 샘플 데이터 출력
        print(f"\n📄 샘플 데이터 (처음 3행):")
        print(statements.head(3)[['corp_code', 'bsns_year', 'fs_div', 'account_nm', 'thstrm_amount']])
        
        # 파일 저장
        output_dir = Path(__file__).resolve().parent.parent / "data"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / "dart_test_success.xlsx"
        
        save_to_excel(statements, output_path)
        print(f"\n💾 테스트 결과 저장: {output_path}")
        
        print(f"\n✅ 테스트 성공! 이제 전체 다운로드를 실행할 수 있습니다.")
        
    else:
        print(f"\n❌ 데이터 수집 실패")

if __name__ == "__main__":
    asyncio.run(test_known_good_companies())
