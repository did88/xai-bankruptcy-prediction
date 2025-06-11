"""캐시를 활용한 안정적인 DART 팀 다운로더"""

import asyncio
import os
import pickle
from pathlib import Path
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial, fetch_bulk_statements, save_to_excel
from dotenv import load_dotenv
import logging

# Load API key
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def get_corp_codes_with_cache(api_key: str, force_refresh: bool = False):
    """캐시를 사용한 기업 코드 가져오기"""
    cache_file = Path(__file__).resolve().parent.parent / "data" / "corp_codes_cache.pkl"
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 캐시 파일이 있고 강제 새로고침이 아닌 경우
    if cache_file.exists() and not force_refresh:
        try:
            logger.info("💾 캐시된 기업 코드 목록 로드 중...")
            with open(cache_file, 'rb') as f:
                corp_df = pickle.load(f)
            
            logger.info(f"✅ 캐시에서 {len(corp_df)}개 기업 로드 완료")
            return corp_df
            
        except Exception as e:
            logger.warning(f"⚠️ 캐시 파일 로드 실패: {e}, API에서 다시 다운로드합니다")
    
    # API에서 새로 다운로드
    logger.info("🌐 API에서 기업 코드 목록 다운로드 중...")
    corp_df = await fetch_corp_codes(api_key)
    
    # 캐시에 저장
    try:
        with open(cache_file, 'wb') as f:
            pickle.dump(corp_df, f)
        logger.info(f"💾 기업 코드 목록을 캐시에 저장: {cache_file}")
    except Exception as e:
        logger.warning(f"⚠️ 캐시 저장 실패: {e}")
    
    return corp_df

async def cached_team_download(team_num: int, skip_validation: bool = True, use_cache: bool = True):
    """캐시를 활용한 팀 다운로드"""
    api_key = os.getenv("DART_API_KEY")
    if not api_key:
        logger.error("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return False
    
    try:
        # 캐시를 사용한 기업 코드 가져오기
        corp_df = await get_corp_codes_with_cache(api_key, force_refresh=not use_cache)
        target_df = filter_kospi_kosdaq_non_financial(corp_df)
        
        # 주식코드로 정렬
        target_df = target_df.sort_values('stock_code').reset_index(drop=True)
        logger.info(f"주식코드 순으로 정렬됨 (총 {len(target_df)}개 기업)")
        
        # 팀 설정
        chunk_size = 100
        start_idx = (team_num - 1) * chunk_size
        end_idx = min(start_idx + chunk_size, len(target_df))
        
        if start_idx >= len(target_df):
            max_teams = len(target_df) // chunk_size + (1 if len(target_df) % chunk_size > 0 else 0)
            logger.error(f"❌ 팀 {team_num}은 범위를 벗어났습니다 (최대 {max_teams}팀)")
            return False
        
        team_corps = target_df.iloc[start_idx:end_idx]
        corp_codes = team_corps['corp_code'].tolist()
        years = range(2015, 2023)
        
        logger.info(f"팀 {team_num} 다운로드 설정:")
        logger.info(f"- 기업 수: {len(corp_codes)}개")
        logger.info(f"- 연도: 2015 ~ 2022")
        logger.info(f"- 예상 요청 수: {len(corp_codes) * len(years)}개")
        
        # 샘플 기업명 출력
        sample_names = team_corps['corp_name'].head(3).tolist()
        logger.info(f"- 샘플 기업: {', '.join(sample_names)}")
        
        # 다운로드 실행 (속도 제한 강화)
        statements = await fetch_bulk_statements(
            api_key, 
            corp_codes, 
            years, 
            workers=3,  # 워커 수 대폭 감소 (10→3)
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
            logger.info(f"📈 기업당 평균: {len(statements)//len(corp_codes):.0f}행")
            logger.info(f"💾 저장 위치: {output_path}")
            return True
        else:
            logger.error(f"❌ 팀 {team_num}: 데이터 수집 실패")
            return False
            
    except Exception as e:
        logger.error(f"❌ 팀 {team_num} 다운로드 실패: {e}")
        return False

def clear_cache():
    """캐시 파일 삭제"""
    cache_file = Path(__file__).resolve().parent.parent / "data" / "corp_codes_cache.pkl"
    if cache_file.exists():
        cache_file.unlink()
        print(f"🗑️ 캐시 파일 삭제: {cache_file}")
    else:
        print("ℹ️ 캐시 파일이 존재하지 않습니다")

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="캐시를 활용한 DART 팀 다운로더")
    parser.add_argument("--team", type=int, required=True, help="팀 번호")
    parser.add_argument("--skip-validation", action="store_true", help="유효성 검증 스킵")
    parser.add_argument("--no-cache", action="store_true", help="캐시 사용하지 않음")
    parser.add_argument("--clear-cache", action="store_true", help="캐시 파일 삭제")
    args = parser.parse_args()
    
    if args.clear_cache:
        clear_cache()
        return
    
    logger.info(f"🚀 팀 {args.team} 캐시 활용 다운로드 시작")
    
    success = await cached_team_download(
        args.team, 
        args.skip_validation, 
        use_cache=not args.no_cache
    )
    
    if success:
        logger.info(f"🎉 팀 {args.team} 다운로드 성공!")
    else:
        logger.error(f"💥 팀 {args.team} 다운로드 실패!")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
