"""DART API 상세 디버깅 스크립트"""

import asyncio
import os
from pathlib import Path
import aiohttp
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

async def find_major_companies(df):
    """주요 대기업 찾기"""
    major_companies = {
        '삼성전자': '00126380',
        'SK하이닉스': '00164779', 
        'LG전자': '00401731',
        '현대차': '00164742',
        'POSCO홀딩스': '00117670',
        '기아': '00164788',
        'NAVER': '00187038',
        '카카오': '00256598',
        'LG화학': '00356361',
        '삼성바이오로직스': '00913160'
    }
    
    print("\n🏢 주요 기업 확인:")
    found_companies = []
    
    for name, expected_code in major_companies.items():
        matches = df[df['corp_name'].str.contains(name, na=False)]
        if not matches.empty:
            for _, corp in matches.iterrows():
                if name in corp['corp_name']:
                    print(f"  - {corp['corp_name']} ({corp['corp_code']}) - 주식코드: {corp['stock_code']}")
                    found_companies.append(corp)
                    break
    
    return found_companies

async def test_multiple_years(api_key: str, corp_code: str, corp_name: str):
    """여러 연도 테스트"""
    print(f"\n📅 {corp_name} 연도별 데이터 확인:")
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    
    async with aiohttp.ClientSession() as session:
        for year in range(2024, 2019, -1):  # 2024부터 2020까지
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": year,
                "reprt_code": "11011",
            }
            
            try:
                async with session.get(url, params=params) as resp:
                    data = await resp.json()
                    status = data.get('status')
                    list_count = len(data.get('list', []))
                    
                    if status == '000':
                        print(f"  ✅ {year}년: {list_count}개 항목")
                        # 첫 번째 항목 샘플 출력
                        if list_count > 0 and year == 2023:
                            print(f"\n    샘플 데이터 (첫 3개 항목):")
                            for i, item in enumerate(data['list'][:3]):
                                print(f"    [{i+1}] {item.get('account_nm')} - {item.get('thstrm_amount')}")
                    else:
                        print(f"  ❌ {year}년: {data.get('message')}")
                        
            except Exception as e:
                print(f"  ❌ {year}년: 오류 - {str(e)}")

async def test_different_apis(api_key: str, corp_code: str, corp_name: str):
    """다른 API 엔드포인트 테스트"""
    print(f"\n🔍 {corp_name} 다른 API 테스트:")
    
    # 다른 재무제표 API들
    apis = {
        "fnlttSinglAcnt": "단일회사 전체 재무제표",
        "fnlttSinglAcntAll": "단일회사 전체 재무제표(모든 항목)",
        "fnlttMultiAcnt": "다중회사 주요계정"
    }
    
    async with aiohttp.ClientSession() as session:
        for api_name, description in apis.items():
            url = f"https://opendart.fss.or.kr/api/{api_name}.json"
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": 2022,
                "reprt_code": "11011",
            }
            
            try:
                async with session.get(url, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get('status')
                        list_count = len(data.get('list', []))
                        print(f"  {description}: Status={status}, Items={list_count}")
                    else:
                        print(f"  {description}: HTTP {resp.status}")
            except Exception as e:
                print(f"  {description}: 오류 - {str(e)}")

async def main():
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    # 기업 목록 가져오기
    print("📋 기업 목록 가져오기...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 주요 대기업 찾기
    major_corps = await find_major_companies(corp_df)
    
    # 삼성전자로 테스트
    samsung = next((corp for corp in major_corps if '삼성전자' in corp['corp_name'] and '우' not in corp['corp_name']), None)
    
    if samsung is not None:
        await test_multiple_years(api_key, samsung['corp_code'], samsung['corp_name'])
        await test_different_apis(api_key, samsung['corp_code'], samsung['corp_name'])
    
    # 상위 시가총액 기업들 테스트
    print("\n📊 상위 10개 기업 (알파벳 순):")
    top_companies = target_df.sort_values('corp_name').head(10)
    
    for _, corp in top_companies.iterrows():
        print(f"\n{corp['corp_name']} ({corp['corp_code']}):")
        params = {
            "crtfc_key": api_key,
            "corp_code": corp['corp_code'],
            "bsns_year": 2022,
            "reprt_code": "11011",
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params=params) as resp:
                data = await resp.json()
                if data.get('status') == '000':
                    print(f"  ✅ 2022년 데이터 있음 ({len(data.get('list', []))}개 항목)")
                else:
                    print(f"  ❌ 2022년 데이터 없음")

if __name__ == "__main__":
    asyncio.run(main())
