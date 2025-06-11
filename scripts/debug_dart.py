"""디버깅용 DART 다운로드 스크립트"""

import asyncio
import os
import logging
from pathlib import Path
import aiohttp
import pandas as pd
from datetime import datetime

from dart_bulk_downloader import (
    fetch_corp_codes,
    filter_kospi_kosdaq_non_financial,
    DART_SINGLE_ACCOUNT_URL,
)

# 간단한 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def debug_single_request(api_key: str, corp_code: str, year: int) -> dict:
    """단일 API 요청 디버깅"""
    params = {
        "crtfc_key": api_key,
        "corp_code": corp_code,
        "bsns_year": year,
        "reprt_code": "11011",
        "fs_div": "CFS",
    }
    
    logger.info(f"API 요청 테스트: {corp_code} - {year}")
    logger.info(f"요청 파라미터: {params}")
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(DART_SINGLE_ACCOUNT_URL, params=params) as resp:
                logger.info(f"HTTP 상태: {resp.status}")
                
                if resp.status == 200:
                    data = await resp.json()
                    logger.info(f"API 응답 상태: {data.get('status')}")
                    logger.info(f"API 응답 메시지: {data.get('message')}")
                    
                    if data.get("status") == "000":
                        list_data = data.get("list", [])
                        logger.info(f"데이터 건수: {len(list_data)}")
                        if list_data:
                            logger.info(f"첫 번째 데이터 샘플: {list_data[0]}")
                        return {"success": True, "data": list_data, "count": len(list_data)}
                    else:
                        logger.warning(f"API 오류: {data.get('status')} - {data.get('message')}")
                        return {"success": False, "error": data.get('message'), "status": data.get('status')}
                else:
                    logger.error(f"HTTP 오류: {resp.status}")
                    return {"success": False, "error": f"HTTP {resp.status}"}
                    
        except Exception as e:
            logger.error(f"요청 예외: {e}")
            return {"success": False, "error": str(e)}

async def debug_multiple_companies(api_key: str, corp_codes: list, years: list) -> None:
    """여러 기업에 대한 디버깅"""
    logger.info(f"테스트 대상: {len(corp_codes)}개 기업, {len(years)}개 연도")
    
    success_count = 0
    total_requests = len(corp_codes) * len(years)
    results = []
    
    for corp_code in corp_codes[:5]:  # 처음 5개 기업만 테스트
        for year in years[:2]:  # 처음 2개 연도만 테스트
            result = await debug_single_request(api_key, corp_code, year)
            results.append({
                'corp_code': corp_code,
                'year': year,
                'success': result['success'],
                'count': result.get('count', 0)
            })
            
            if result['success']:
                success_count += 1
            
            # API 제한을 위한 짧은 대기
            await asyncio.sleep(0.1)
    
    logger.info(f"테스트 결과 요약:")
    logger.info(f"- 성공한 요청: {success_count}")
    logger.info(f"- 전체 테스트 요청: {len(results)}")
    
    # 성공한 요청들의 데이터 건수 분포
    successful_results = [r for r in results if r['success']]
    if successful_results:
        data_counts = [r['count'] for r in successful_results]
        logger.info(f"- 데이터 건수 통계: 최소={min(data_counts)}, 최대={max(data_counts)}, 평균={sum(data_counts)/len(data_counts):.1f}")
    
    # 실패한 요청들 상세 정보
    failed_results = [r for r in results if not r['success']]
    if failed_results:
        logger.warning(f"실패한 요청 {len(failed_results)}개:")
        for r in failed_results[:3]:  # 처음 3개만 출력
            logger.warning(f"  - {r['corp_code']} {r['year']}")

async def main():
    # API 키 확인
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("DART_API_KEY 환경변수가 설정되지 않았습니다")
        return
    
    logger.info("=== DART API 디버깅 시작 ===")
    
    # 기업 코드 가져오기
    logger.info("기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    # 테스트용 기업들 선택
    test_corp_codes = target_df["corp_code"].head(10).tolist()
    test_years = [2021, 2022]
    
    logger.info(f"선택된 테스트 기업: {test_corp_codes}")
    logger.info(f"테스트 연도: {test_years}")
    
    # 단일 요청 테스트 (삼성전자)
    samsung_code = "00126380"  # 삼성전자
    if samsung_code in target_df["corp_code"].values:
        logger.info("=== 삼성전자 단일 요청 테스트 ===")
        result = await debug_single_request(api_key, samsung_code, 2022)
        logger.info(f"삼성전자 테스트 결과: {result}")
    
    # 다중 요청 테스트
    logger.info("=== 다중 기업 요청 테스트 ===")
    await debug_multiple_companies(api_key, test_corp_codes, test_years)
    
    logger.info("=== 디버깅 완료 ===")

if __name__ == "__main__":
    asyncio.run(main())
