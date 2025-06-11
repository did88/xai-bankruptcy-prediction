"""DART API 연결 테스트"""

import asyncio
import aiohttp
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

async def test_dart_connection():
    """DART API 연결 상태 테스트"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return False
    
    test_urls = [
        "https://opendart.fss.or.kr/api/corpCode.xml",
        "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json",
        "https://www.google.com",  # 일반 인터넷 연결 테스트
    ]
    
    print("🌐 네트워크 연결 테스트 시작...")
    
    try:
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(limit=5)
        
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            for i, url in enumerate(test_urls, 1):
                print(f"\n{i}. {url} 테스트 중...")
                try:
                    if "opendart" in url:
                        test_url = f"{url}?crtfc_key={api_key}"
                    else:
                        test_url = url
                    
                    async with session.get(test_url) as resp:
                        print(f"   ✅ 상태 코드: {resp.status}")
                        if resp.status == 200:
                            content_length = resp.headers.get('Content-Length', 'Unknown')
                            print(f"   📊 응답 크기: {content_length} bytes")
                        else:
                            print(f"   ⚠️ 비정상 응답 코드")
                            
                except asyncio.TimeoutError:
                    print(f"   ❌ 타임아웃 (30초 초과)")
                except Exception as e:
                    print(f"   ❌ 연결 실패: {e}")
    
    except Exception as e:
        print(f"❌ 세션 생성 실패: {e}")
        return False
    
    print(f"\n🔍 네트워크 진단 완료")
    return True

async def test_simple_api_call():
    """간단한 API 호출 테스트"""
    api_key = os.getenv("DART_API_KEY")
    
    print(f"\n📡 DART API 간단 호출 테스트...")
    
    try:
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # 단일 기업의 단일 연도 데이터 요청
            params = {
                "crtfc_key": api_key,
                "corp_code": "00119195",  # 동화약품 (이전 테스트에서 성공함)
                "bsns_year": "2022",
                "reprt_code": "11011",
                "fs_div": "CFS",
            }
            
            url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
            async with session.get(url, params=params) as resp:
                print(f"   ✅ 상태 코드: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    status = data.get("status", "")
                    if status == "000":
                        list_data = data.get("list", [])
                        print(f"   ✅ API 응답 성공: {len(list_data)}개 항목")
                        return True
                    else:
                        print(f"   ❌ API 오류: {status} - {data.get('message', '')}")
                else:
                    print(f"   ❌ HTTP 오류: {resp.status}")
                    
    except Exception as e:
        print(f"   ❌ API 호출 실패: {e}")
    
    return False

if __name__ == "__main__":
    async def main():
        print("🔧 DART 연결 문제 진단 시작\n")
        
        # 1. 기본 연결 테스트
        await test_dart_connection()
        
        # 2. API 호출 테스트
        api_success = await test_simple_api_call()
        
        print(f"\n📋 진단 결과:")
        if api_success:
            print("✅ DART API 연결 정상")
            print("💡 권장사항: robust_team_downloader.py 사용")
        else:
            print("❌ DART API 연결 문제 있음")
            print("💡 권장사항:")
            print("   1. 인터넷 연결 확인")
            print("   2. 방화벽/보안 프로그램 확인")
            print("   3. 몇 분 후 재시도")
            print("   4. VPN 사용 중이면 해제 후 재시도")
    
    asyncio.run(main())
