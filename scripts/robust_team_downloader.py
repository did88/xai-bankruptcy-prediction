"""네트워크 안정성이 개선된 DART 다운로더"""

import asyncio
import aiohttp
import time
from pathlib import Path
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial, fetch_bulk_statements, save_to_excel
from dotenv import load_dotenv
import os
import logging

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def robust_fetch_corp_codes(api_key: str, max_retries: int = 3) -> 'pd.DataFrame':
    """네트워크 재시도 로직이 있는 기업 코드 가져오기"""
    for attempt in range(max_retries):
        try:
            logger.info(f"기업 코드 다운로드 시도 {attempt + 1}/{max_retries}")
            
            # 연결 타임아웃과 읽기 타임아웃 설정
            timeout = aiohttp.ClientTimeout(total=60, connect=30)
            connector = aiohttp.TCPConnector(
                limit=10,  # 연결 수 제한
                limit_per_host=5,  # 호스트당 연결 수 제한
                ttl_dns_cache=300,  # DNS 캐시 시간
                use_dns_cache=True,
            )
            
            async with aiohttp.ClientSession(
                timeout=timeout, 
                connector=connector,
                headers={'User-Agent': 'DART-API-Client/1.0'}
            ) as session:
                corp_df = await fetch_corp_codes_with_session(session, api_key)
                logger.info(f"✅ 기업 코드 다운로드 성공: {len(corp_df)}개")
                return corp_df
                
        except Exception as e:
            logger.warning(f"❌ 시도 {attempt + 1} 실패: {e}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # 5초, 10초, 15초 대기
                logger.info(f"⏳ {wait_time}초 대기 후 재시도...")
                await asyncio.sleep(wait_time)
            else:
                raise e

async def fetch_corp_codes_with_session(session: aiohttp.ClientSession, api_key: str) -> 'pd.DataFrame':
    """기존 세션을 사용한 기업 코드 가져오기"""
    import io
    import zipfile
    import xml.etree.ElementTree as ET
    import pandas as pd
    
    DART_CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
    url = f"{DART_CORPCODE_URL}?crtfc_key={api_key}"
    
    async with session.get(url) as resp:
        resp.raise_for_status()
        data = await resp.read()

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        xml_data = zf.read("CORPCODE.xml").decode("utf-8")
    
    root = ET.fromstring(xml_data)
    data_list = []
    
    for corp in root.findall('.//list'):
        corp_dict = {}
        for child in corp:
            corp_dict[child.tag] = child.text
        data_list.append(corp_dict)
    
    df = pd.DataFrame(data_list)
    return df

async def robust_team_download(team_num: int, skip_validation: bool = True):
    """네트워크 안정성이 개선된 팀 다운로드"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return False
    
    try:
        # 안정적인 기업 코드 다운로드
        corp_df = await robust_fetch_corp_codes(api_key)
        target_df = filter_kospi_kosdaq_non_financial(corp_df)
        
        # 주식코드로 정렬
        target_df = target_df.sort_values('stock_code').reset_index(drop=True)
        logger.info(f"주식코드 순으로 정렬됨 (총 {len(target_df)}개 기업)")
        
        # 팀 설정
        chunk_size = 100
        start_idx = (team_num - 1) * chunk_size
        end_idx = min(start_idx + chunk_size, len(target_df))
        
        if start_idx >= len(target_df):
            logger.error(f"❌ 팀 {team_num}은 범위를 벗어났습니다 (최대 {len(target_df)//chunk_size + 1}팀)")
            return False
        
        team_corps = target_df.iloc[start_idx:end_idx]
        corp_codes = team_corps['corp_code'].tolist()
        years = range(2015, 2023)
        
        logger.info(f"팀 {team_num} 다운로드 설정:")
        logger.info(f"- 기업 수: {len(corp_codes)}개")
        logger.info(f"- 연도: 2015 ~ 2022")
        logger.info(f"- 예상 요청 수: {len(corp_codes) * len(years)}개")
        
        # 다운로드 실행 (워커 수 줄여서 안정성 향상)
        statements = await fetch_bulk_statements(
            api_key, 
            corp_codes, 
            years, 
            workers=5,  # 네트워크 안정성을 위해 줄임
            include_corp_names=True
        )
        
        if not statements.empty:
            # 파일 저장
            output_dir = Path(__file__).resolve().parent.parent / "data" / "team_downloads"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f"dart_statements_team_{team_num:02d}.xlsx"
            
            save_to_excel(statements, output_path)
            
            logger.info(f"✅ 팀 {team_num} 다운로드 완료")
            logger.info(f"📊 수집된 데이터: {len(statements):,}행")
            logger.info(f"💾 저장 위치: {output_path}")
            return True
        else:
            logger.error(f"❌ 팀 {team_num}: 데이터 수집 실패")
            return False
            
    except Exception as e:
        logger.error(f"❌ 팀 {team_num} 다운로드 실패: {e}")
        return False

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="안정성이 개선된 DART 팀 다운로더")
    parser.add_argument("--team", type=int, required=True, help="팀 번호")
    parser.add_argument("--skip-validation", action="store_true", help="유효성 검증 스킵")
    args = parser.parse_args()
    
    logger.info(f"🚀 팀 {args.team} 안정성 개선 다운로드 시작")
    
    success = await robust_team_download(args.team, args.skip_validation)
    
    if success:
        logger.info(f"🎉 팀 {args.team} 다운로드 성공!")
    else:
        logger.error(f"💥 팀 {args.team} 다운로드 실패!")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
