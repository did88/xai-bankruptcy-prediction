"""DART API 응답 디버깅 스크립트"""

import asyncio
import os
from pathlib import Path
import pandas as pd
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial
import aiohttp
from dotenv import load_dotenv

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

DART_SINGLE_ACCOUNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"

async def test_single_request():
    """단일 요청 테스트"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print("📋 기업 코드 목록 가져오는 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    if len(target_df) == 0:
        print("❌ 필터링된 기업이 없습니다.")
        return
    
    # 첫 번째 기업으로 테스트
    test_corp = target_df.iloc[0]
    corp_code = test_corp['corp_code']
    corp_name = test_corp.get('corp_name', 'Unknown')
    
    print(f"\n🧪 테스트 기업: {corp_name} ({corp_code})")
    
    # 여러 연도 테스트
    test_years = [2022, 2021, 2020]
    
    async with aiohttp.ClientSession() as session:
        for year in test_years:
            print(f"\n📅 {year}년 데이터 요청...")
            
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": year,
                "reprt_code": "11011",  # 사업보고서
                "fs_div": "CFS",        # 연결재무제표
            }
            
            try:
                async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                    print(f"   HTTP Status: {resp.status}")
                    
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get("status", "")
                        message = data.get("message", "")
                        
                        print(f"   API Status: {status}")
                        print(f"   Message: {message}")
                        
                        if status == "000":
                            list_data = data.get("list", [])
                            print(f"   ✅ 성공: {len(list_data)}개 계정 항목")
                            
                            if list_data:
                                # 첫 번째 항목 샘플 출력
                                sample = list_data[0]
                                print(f"   샘플 데이터: {sample}")
                                return True
                        else:
                            print(f"   ❌ API 오류: {status} - {message}")
                    else:
                        print(f"   ❌ HTTP 오류: {resp.status}")
                        
            except Exception as e:
                print(f"   ❌ 요청 실패: {e}")
    
    # 다른 보고서 유형들도 테스트
    print(f"\n🔍 다른 보고서 유형 테스트 (2022년)...")
    
    report_types = [
        ("11013", "1분기보고서"),
        ("11012", "반기보고서"),
        ("11014", "3분기보고서"),
        ("11011", "사업보고서")
    ]
    
    async with aiohttp.ClientSession() as session:
        for reprt_code, report_name in report_types:
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": 2022,
                "reprt_code": reprt_code,
                "fs_div": "CFS",
            }
            
            try:
                async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        status = data.get("status", "")
                        message = data.get("message", "")
                        
                        if status == "000":
                            list_data = data.get("list", [])
                            print(f"   ✅ {report_name}: {len(list_data)}개 항목")
                        else:
                            print(f"   ❌ {report_name}: {status} - {message}")
                            
            except Exception as e:
                print(f"   ❌ {report_name} 요청 실패: {e}")
    
    return False

async def test_multiple_corps():
    """여러 기업으로 테스트"""
    api_key = os.getenv("DART_API_KEY")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    print(f"\n🔍 상위 10개 기업으로 테스트...")
    
    success_count = 0
    test_corps = target_df.head(10)
    
    async with aiohttp.ClientSession() as session:
        for idx, row in test_corps.iterrows():
            corp_code = row['corp_code']
            corp_name = row.get('corp_name', 'Unknown')
            
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": 2022,
                "reprt_code": "11011",
                "fs_div": "CFS",
            }
            
            try:
                async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "000":
                            list_data = data.get("list", [])
                            if list_data:
                                print(f"   ✅ {corp_name}: {len(list_data)}개 항목")
                                success_count += 1
                            else:
                                print(f"   ⚠️ {corp_name}: 응답은 성공했지만 데이터 없음")
                        else:
                            print(f"   ❌ {corp_name}: {data.get('status')} - {data.get('message')}")
                    
            except Exception as e:
                print(f"   ❌ {corp_name}: 요청 실패 - {e}")
    
    print(f"\n📊 결과: {success_count}/10 기업에서 데이터 수집 성공")
    return success_count

if __name__ == "__main__":
    print("🚀 DART API 디버깅 시작...\n")
    
    # 단일 요청 테스트
    success = asyncio.run(test_single_request())
    
    if success:
        # 여러 기업 테스트
        asyncio.run(test_multiple_corps())
    else:
        print("\n❌ 기본 API 테스트에서 실패했습니다.")
        print("   가능한 원인:")
        print("   1. API 키가 잘못되었거나 만료됨")
        print("   2. API 서버 문제")
        print("   3. 네트워크 연결 문제")
        print("   4. 선택한 기업에 해당 연도 데이터가 없음")
