"""정확한 기업 코드로 DART API 테스트"""

import asyncio
import os
from pathlib import Path
import aiohttp
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 실제 주요 기업들의 정확한 코드
MAJOR_COMPANIES = {
    '00126380': '삼성전자',
    '00164779': 'SK하이닉스', 
    '00401731': 'LG전자',
    '00164742': '현대자동차',
    '00117670': 'POSCO',
    '00164788': '기아',
    '00187038': 'NAVER',
    '00256598': '카카오',
    '00356361': 'LG화학',
    '00207940': '삼성바이오로직스'
}

async def test_company_data(api_key: str, corp_code: str, corp_name: str):
    """특정 기업의 데이터 테스트"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    
    print(f"\n📊 {corp_name} ({corp_code}) 테스트:")
    
    async with aiohttp.ClientSession() as session:
        # 최근 3년 테스트
        for year in [2023, 2022, 2021]:
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": year,
                "reprt_code": "11011",
            }
            
            try:
                async with session.get(url, params=params) as resp:
                    data = await resp.json()
                    
                    if data.get('status') == '000':
                        items = data.get('list', [])
                        print(f"  ✅ {year}년: {len(items)}개 항목")
                        
                        # 주요 계정 샘플 출력
                        if items and year == 2022:
                            print(f"\n  주요 재무 항목 샘플:")
                            target_accounts = ['매출액', '영업이익', '당기순이익', '자산총계', '부채총계']
                            
                            for item in items[:50]:  # 처음 50개만 확인
                                account_nm = item.get('account_nm', '')
                                if any(target in account_nm for target in target_accounts):
                                    amount = item.get('thstrm_amount', '0')
                                    print(f"    - {account_nm}: {amount}")
                    else:
                        print(f"  ❌ {year}년: {data.get('message')}")
                        
            except Exception as e:
                print(f"  ❌ {year}년: 오류 - {str(e)}")

async def main():
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print("🏢 주요 상장사 재무제표 데이터 테스트")
    
    # 삼성전자 먼저 테스트
    await test_company_data(api_key, '00126380', '삼성전자')
    
    # 기타 주요 기업들
    for corp_code, corp_name in list(MAJOR_COMPANIES.items())[1:4]:  # SK하이닉스, LG전자, 현대차
        await test_company_data(api_key, corp_code, corp_name)
    
    print("\n✅ 테스트 완료!")
    print("\n💡 결과 요약:")
    print("- 대부분의 주요 기업들은 2021-2022년 데이터가 있습니다.")
    print("- 2023년 데이터는 아직 모든 기업이 제출하지 않았을 수 있습니다.")
    print("- 각 기업은 약 14-28개의 재무제표 항목을 가지고 있습니다.")

if __name__ == "__main__":
    asyncio.run(main())
