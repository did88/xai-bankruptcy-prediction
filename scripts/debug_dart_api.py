"""DART API 디버깅 스크립트"""

import asyncio
import os
from pathlib import Path
import aiohttp
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

async def test_single_api_call(api_key: str, corp_code: str, year: int):
    """단일 API 호출 테스트"""
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",  # 사업보고서
    }
    
    print(f"\n📡 API 호출 테스트:")
    print(f"URL: {url}")
    print(f"Parameters: {params}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                print(f"Status Code: {resp.status}")
                data = await resp.json()
                
                print(f"\n응답 데이터:")
                print(f"Status: {data.get('status')}")
                print(f"Message: {data.get('message')}")
                
                if data.get('status') == '000':
                    list_data = data.get('list', [])
                    print(f"데이터 개수: {len(list_data)}")
                    if list_data:
                        print(f"\n첫 번째 항목:")
                        for key, value in list_data[0].items():
                            print(f"  {key}: {value}")
                else:
                    print(f"\n❌ API 에러: {data.get('message')}")
                    
                return data
                
        except Exception as e:
            print(f"❌ 요청 실패: {str(e)}")
            return None

async def test_api_key(api_key: str):
    """API 키 유효성 테스트"""
    url = "https://opendart.fss.or.kr/api/company.json"
    params = {
        "crtfc_key": api_key,
        "corp_code": "00126380"  # 삼성전자
    }
    
    print(f"\n🔑 API 키 테스트:")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url, params=params) as resp:
                data = await resp.json()
                if data.get('status') == '000':
                    print(f"✅ API 키 유효함")
                    print(f"테스트 기업: {data.get('corp_name')}")
                else:
                    print(f"❌ API 키 문제: {data.get('message')}")
                return data.get('status') == '000'
        except Exception as e:
            print(f"❌ API 키 테스트 실패: {str(e)}")
            return False

async def main():
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print(f"API Key: {api_key[:10]}..." if len(api_key) > 10 else api_key)
    
    # API 키 테스트
    if not await test_api_key(api_key):
        print("\n⚠️ API 키가 유효하지 않습니다. .env 파일을 확인하세요.")
        return
    
    # 기업 목록 가져오기
    print("\n📋 기업 목록 가져오기...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 테스트할 기업 선택
    test_corps = target_df.head(5)
    print(f"\n🏢 테스트 기업:")
    for _, corp in test_corps.iterrows():
        print(f"  - {corp['corp_name']} ({corp['corp_code']}) - {corp['stock_code']}")
    
    # 첫 번째 기업으로 상세 테스트
    if len(test_corps) > 0:
        first_corp = test_corps.iloc[0]
        await test_single_api_call(api_key, first_corp['corp_code'], 2023)
        
        # 다른 report_code 테스트
        print("\n📋 다른 보고서 유형 테스트:")
        report_codes = {
            "11011": "사업보고서",
            "11012": "반기보고서",
            "11013": "1분기보고서",
            "11014": "3분기보고서"
        }
        
        for code, name in report_codes.items():
            print(f"\n{name} ({code}):")
            params = {
                "crtfc_key": api_key,
                "corp_code": first_corp['corp_code'],
                "bsns_year": 2023,
                "reprt_code": code,
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get("https://opendart.fss.or.kr/api/fnlttSinglAcnt.json", params=params) as resp:
                    data = await resp.json()
                    status = data.get('status')
                    message = data.get('message', '')
                    list_count = len(data.get('list', []))
                    print(f"  Status: {status}, Message: {message}, Items: {list_count}")

if __name__ == "__main__":
    asyncio.run(main())
