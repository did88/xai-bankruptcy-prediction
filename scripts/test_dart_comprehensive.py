"""DART API 광범위 테스트 스크립트 - 실제 데이터가 있는 기업 찾기"""

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

async def find_valid_companies():
    """실제 데이터가 있는 기업들을 찾기"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return
    
    print("📋 기업 코드 목록 가져오는 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    print(f"✅ 필터링된 기업 수: {len(target_df)}개")
    
    # 대기업부터 테스트 (주식코드가 작은 순서로)
    target_df_sorted = target_df.sort_values('stock_code').reset_index(drop=True)
    
    success_companies = []
    test_years = [2022, 2021, 2020]
    
    print(f"\n🔍 상위 50개 기업에서 데이터 존재 여부 테스트...")
    
    async with aiohttp.ClientSession() as session:
        for idx in range(min(50, len(target_df_sorted))):
            row = target_df_sorted.iloc[idx]
            corp_code = row['corp_code']
            corp_name = row.get('corp_name', 'Unknown')
            stock_code = row.get('stock_code', 'N/A')
            
            print(f"\n📊 테스트 {idx+1}/50: {corp_name} ({stock_code}) - {corp_code}")
            
            company_has_data = False
            
            for year in test_years:
                params = {
                    "crtfc_key": api_key,
                    "corp_code": corp_code,
                    "bsns_year": year,
                    "reprt_code": "11011",  # 사업보고서
                    "fs_div": "CFS",        # 연결재무제표
                }
                
                try:
                    async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            status = data.get("status", "")
                            
                            if status == "000":
                                list_data = data.get("list", [])
                                if list_data:
                                    print(f"   ✅ {year}년: {len(list_data)}개 계정 항목")
                                    company_has_data = True
                                    
                                    success_companies.append({
                                        'corp_code': corp_code,
                                        'corp_name': corp_name,
                                        'stock_code': stock_code,
                                        'year': year,
                                        'data_count': len(list_data)
                                    })
                                    break  # 해당 기업에서 데이터를 찾았으면 다음 기업으로
                                else:
                                    print(f"   ⚠️ {year}년: 응답 성공하지만 데이터 없음")
                            else:
                                message = data.get("message", "")
                                print(f"   ❌ {year}년: {status} - {message}")
                        else:
                            print(f"   ❌ {year}년: HTTP {resp.status}")
                            
                except Exception as e:
                    print(f"   ❌ {year}년: 요청 실패 - {e}")
                
                # 속도 제한 준수
                await asyncio.sleep(0.1)
            
            if not company_has_data:
                print(f"   ❌ {corp_name}: 모든 연도에서 데이터 없음")
            
            # 성공한 기업 5개 이상 찾으면 조기 종료
            if len(success_companies) >= 5:
                break
    
    print(f"\n📊 테스트 결과:")
    print(f"   - 테스트한 기업 수: {min(50, len(target_df_sorted))}개")
    print(f"   - 데이터가 있는 기업 수: {len(success_companies)}개")
    
    if success_companies:
        print(f"\n✅ 데이터가 있는 기업 목록:")
        for comp in success_companies:
            print(f"   {comp['corp_name']} ({comp['stock_code']}) - {comp['year']}년: {comp['data_count']}개 항목")
        
        # 첫 번째 성공한 기업으로 상세 테스트
        print(f"\n🔬 상세 테스트: {success_companies[0]['corp_name']}")
        await detailed_test(api_key, success_companies[0]['corp_code'], success_companies[0]['corp_name'])
        
        return success_companies
    else:
        print("\n❌ 모든 테스트 기업에서 데이터를 찾을 수 없습니다.")
        print("   가능한 원인:")
        print("   1. 2020-2022년 연결재무제표 데이터가 일반적으로 제공되지 않음")
        print("   2. 개별재무제표(OFS) 사용 필요")
        print("   3. 다른 보고서 유형 사용 필요")
        
        # 개별재무제표로 다시 테스트
        print(f"\n🔄 개별재무제표(OFS)로 재테스트...")
        await test_with_ofs(api_key, target_df_sorted)

async def detailed_test(api_key: str, corp_code: str, corp_name: str):
    """성공한 기업으로 상세 테스트"""
    async with aiohttp.ClientSession() as session:
        # 다양한 설정으로 테스트
        test_configs = [
            {"fs_div": "CFS", "name": "연결재무제표"},
            {"fs_div": "OFS", "name": "개별재무제표"},
        ]
        
        for config in test_configs:
            print(f"\n📋 {config['name']} 테스트:")
            
            for year in [2022, 2021, 2020]:
                params = {
                    "crtfc_key": api_key,
                    "corp_code": corp_code,
                    "bsns_year": year,
                    "reprt_code": "11011",
                    "fs_div": config["fs_div"],
                }
                
                try:
                    async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("status") == "000":
                                list_data = data.get("list", [])
                                print(f"   ✅ {year}년: {len(list_data)}개 항목")
                                
                                if list_data:
                                    # 샘플 데이터 출력
                                    sample = list_data[0]
                                    print(f"      샘플: {sample.get('account_nm', 'N/A')} = {sample.get('thstrm_amount', 'N/A')}")
                            else:
                                print(f"   ❌ {year}년: {data.get('status')} - {data.get('message')}")
                        
                except Exception as e:
                    print(f"   ❌ {year}년: 요청 실패 - {e}")
                
                await asyncio.sleep(0.1)

async def test_with_ofs(api_key: str, target_df_sorted: pd.DataFrame):
    """개별재무제표로 테스트"""
    async with aiohttp.ClientSession() as session:
        for idx in range(min(10, len(target_df_sorted))):
            row = target_df_sorted.iloc[idx]
            corp_code = row['corp_code']
            corp_name = row.get('corp_name', 'Unknown')
            
            print(f"\n📊 OFS 테스트: {corp_name}")
            
            params = {
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bsns_year": 2022,
                "reprt_code": "11011",
                "fs_div": "OFS",  # 개별재무제표
            }
            
            try:
                async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("status") == "000":
                            list_data = data.get("list", [])
                            if list_data:
                                print(f"   ✅ OFS 데이터 있음: {len(list_data)}개 항목")
                                return True
                        else:
                            print(f"   ❌ {data.get('status')} - {data.get('message')}")
                    
            except Exception as e:
                print(f"   ❌ 요청 실패: {e}")
            
            await asyncio.sleep(0.1)
    
    return False

if __name__ == "__main__":
    print("🚀 DART API 광범위 테스트 시작...\n")
    asyncio.run(find_valid_companies())
