"""실제 팀 수 확인 스크립트"""
import asyncio
import os
from dart_bulk_downloader import fetch_corp_codes, filter_kospi_kosdaq_non_financial

def split_corps_for_teams(corp_codes, chunk_size=100):
    chunks = []
    for i in range(0, len(corp_codes), chunk_size):
        team_num = i // chunk_size + 1
        chunk = corp_codes[i:i + chunk_size]
        chunks.append((team_num, chunk))
    return chunks

async def check_actual_teams():
    api_key = os.getenv("DART_API_KEY") 
    if not api_key:
        api_key = "ea93b96a1c65f8122dc6a71957dbf871655ef780"
    
    print("📋 실제 팀 수 확인 중...")
    corp_df = await fetch_corp_codes(api_key)
    target_df = filter_kospi_kosdaq_non_financial(corp_df)
    
    all_corp_codes = target_df["corp_code"].unique().tolist()
    team_chunks = split_corps_for_teams(all_corp_codes, chunk_size=100)
    
    print(f"✅ 전체 대상 기업: {len(all_corp_codes):,}개")
    print(f"✅ 실제 총 팀 수: {len(team_chunks)}팀")
    print(f"   팀 1 ~ 팀 {len(team_chunks)}")
    
    return len(team_chunks)

if __name__ == "__main__":
    actual_teams = asyncio.run(check_actual_teams())
