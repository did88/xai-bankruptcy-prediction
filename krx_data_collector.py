import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
import os
import json
import pickle
import warnings
from tqdm import tqdm
import random
warnings.filterwarnings('ignore')

class RobustKRXCollector:
    def __init__(self):
        self.start_date = "20150101"
        self.end_date = "20231231"
        
        # 진행 상황 저장 파일들
        self.progress_file = "krx_collection_progress.json"
        self.data_cache_file = "krx_data_cache.pickle"
        self.ticker_list_file = "krx_ticker_list.json"
        
        # API 호출 제한 설정
        self.min_delay = 0.5  # 최소 지연 시간
        self.max_delay = 2.0  # 최대 지연 시간
        self.retry_count = 3  # 재시도 횟수
        self.batch_size = 50  # 배치 크기
        
        # 금융업 키워드
        self.financial_keywords = [
            '은행', '증권', '보험', '카드', '캐피탈', '리츠', 'REIT', 
            '금융지주', '투자', '자산운용', '신탁', '대부', '여신',
            '파이낸셜', '페이', '핀테크', 'P2P'
        ]
        
        # 수집된 데이터 저장
        self.collected_data = {}
        self.failed_tickers = []
        self.non_financial_tickers = []
        
    def smart_delay(self):
        """랜덤 지연으로 API 호출 제한 회피"""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
    
    def save_progress(self, current_index, total_count, phase="collecting"):
        """진행 상황 저장"""
        progress = {
            'current_index': current_index,
            'total_count': total_count,
            'phase': phase,
            'timestamp': datetime.now().isoformat(),
            'collected_count': len(self.collected_data),
            'failed_count': len(self.failed_tickers)
        }
        
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress, f, ensure_ascii=False, indent=2)
    
    def load_progress(self):
        """진행 상황 불러오기"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                return None
        return None
    
    def save_data_cache(self):
        """수집된 데이터 캐시 저장"""
        cache_data = {
            'collected_data': self.collected_data,
            'non_financial_tickers': self.non_financial_tickers,
            'failed_tickers': self.failed_tickers,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.data_cache_file, 'wb') as f:
            pickle.dump(cache_data, f)
    
    def load_data_cache(self):
        """수집된 데이터 캐시 불러오기"""
        if os.path.exists(self.data_cache_file):
            try:
                with open(self.data_cache_file, 'rb') as f:
                    cache_data = pickle.load(f)
                    self.collected_data = cache_data.get('collected_data', {})
                    self.non_financial_tickers = cache_data.get('non_financial_tickers', [])
                    self.failed_tickers = cache_data.get('failed_tickers', [])
                    return True
            except:
                return False
        return False
    
    def save_ticker_list(self, kospi_tickers, kosdaq_tickers):
        """티커 목록 저장"""
        ticker_data = {
            'kospi_tickers': kospi_tickers,
            'kosdaq_tickers': kosdaq_tickers,
            'timestamp': datetime.now().isoformat()
        }
        
        with open(self.ticker_list_file, 'w', encoding='utf-8') as f:
            json.dump(ticker_data, f, ensure_ascii=False, indent=2)
    
    def load_ticker_list(self):
        """티커 목록 불러오기"""
        if os.path.exists(self.ticker_list_file):
            try:
                with open(self.ticker_list_file, 'r', encoding='utf-8') as f:
                    ticker_data = json.load(f)
                    return ticker_data['kospi_tickers'], ticker_data['kosdaq_tickers']
            except:
                return None, None
        return None, None
    
    def get_company_name_with_retry(self, stock, ticker):
        """재시도 기능이 있는 회사명 조회"""
        for attempt in range(self.retry_count):
            try:
                self.smart_delay()
                return stock.get_market_ticker_name(ticker)
            except Exception as e:
                if attempt == self.retry_count - 1:
                    print(f"      ❌ 회사명 조회 실패 (최종): {ticker}")
                    return f"Unknown_{ticker}"
                else:
                    print(f"      ⚠️ 회사명 조회 재시도 {attempt+1}/{self.retry_count}: {ticker}")
                    time.sleep(1 + attempt)
        return f"Unknown_{ticker}"
    
    def get_stock_data_with_retry(self, stock, ticker):
        """재시도 기능이 있는 주식 데이터 조회"""
        for attempt in range(self.retry_count):
            try:
                self.smart_delay()
                data = stock.get_market_ohlcv(self.start_date, self.end_date, ticker)
                return data
            except Exception as e:
                if attempt == self.retry_count - 1:
                    print(f"      ❌ 데이터 조회 실패 (최종): {ticker} - {str(e)}")
                    return pd.DataFrame()
                else:
                    print(f"      ⚠️ 데이터 조회 재시도 {attempt+1}/{self.retry_count}: {ticker}")
                    time.sleep(2 + attempt * 2)  # 점진적 지연 증가
        return pd.DataFrame()
    
    def is_financial_company(self, company_name):
        """금융업 여부 판단"""
        return any(keyword in company_name for keyword in self.financial_keywords)
    
    def get_all_tickers(self):
        """전체 티커 목록 가져오기 (캐시 활용)"""
        print("📋 상장 기업 목록 수집 중...")
        
        # 기존 캐시 확인
        kospi_tickers, kosdaq_tickers = self.load_ticker_list()
        
        if kospi_tickers and kosdaq_tickers:
            print("✅ 캐시된 티커 목록 사용")
            print(f"   KOSPI: {len(kospi_tickers)}개, KOSDAQ: {len(kosdaq_tickers)}개")
            return kospi_tickers, kosdaq_tickers
        
        try:
            from pykrx import stock
            
            # 새로 수집
            print("🔄 새로운 티커 목록 수집 중...")
            
            kospi_tickers = stock.get_market_ticker_list("20231229", market="KOSPI")
            self.smart_delay()
            
            kosdaq_tickers = stock.get_market_ticker_list("20231229", market="KOSDAQ")
            self.smart_delay()
            
            # 캐시 저장
            self.save_ticker_list(kospi_tickers, kosdaq_tickers)
            
            print(f"✅ 수집 완료: KOSPI {len(kospi_tickers)}개, KOSDAQ {len(kosdaq_tickers)}개")
            return kospi_tickers, kosdaq_tickers
            
        except Exception as e:
            print(f"❌ 티커 목록 수집 실패: {str(e)}")
            return [], []
    
    def filter_non_financial_companies(self, all_tickers):
        """비금융 기업 필터링"""
        print("\n🏦 비금융 기업 필터링 중...")
        
        # 기존 진행 상황 확인
        progress = self.load_progress()
        start_index = 0
        
        if progress and progress.get('phase') == 'filtering':
            print(f"✅ 이전 진행 상황 발견: {progress['current_index']}/{progress['total_count']}")
            start_index = progress['current_index']
            self.load_data_cache()
        
        try:
            from pykrx import stock
            
            # 진행률 바 설정
            pbar = tqdm(total=len(all_tickers), 
                       initial=start_index,
                       desc="🔍 기업 분류",
                       unit="개",
                       bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
            
            for i, ticker in enumerate(all_tickers[start_index:], start_index):
                try:
                    # 회사명 조회
                    company_name = self.get_company_name_with_retry(stock, ticker)
                    
                    # 금융업 여부 판단
                    if not self.is_financial_company(company_name):
                        self.non_financial_tickers.append({
                            'ticker': ticker,
                            'name': company_name,
                            'market': 'KOSPI' if ticker in self.kospi_tickers else 'KOSDAQ'
                        })
                        pbar.set_postfix({'비금융': len(self.non_financial_tickers), '진행': f"{i+1}/{len(all_tickers)}"})
                    
                    # 진행 상황 저장 (매 10개마다)
                    if (i + 1) % 10 == 0:
                        self.save_progress(i + 1, len(all_tickers), "filtering")
                        self.save_data_cache()
                    
                    pbar.update(1)
                    
                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 요청 - 진행 상황 저장 중...")
                    self.save_progress(i, len(all_tickers), "filtering")
                    self.save_data_cache()
                    pbar.close()
                    return
                except Exception as e:
                    pbar.set_postfix({'오류': str(e)[:20]})
                    continue
            
            pbar.close()
            
            print(f"\n✅ 필터링 완료:")
            print(f"   전체 검토: {len(all_tickers)}개")
            print(f"   비금융 기업: {len(self.non_financial_tickers)}개")
            
            # 완료 후 진행 상황 업데이트
            self.save_progress(len(all_tickers), len(all_tickers), "filtering_complete")
            self.save_data_cache()
            
        except Exception as e:
            print(f"❌ 필터링 중 오류: {str(e)}")
    
    def collect_stock_data(self):
        """주식 데이터 수집"""
        print(f"\n📈 주식 데이터 수집 시작 (총 {len(self.non_financial_tickers)}개 기업)")
        
        # 기존 진행 상황 확인
        progress = self.load_progress()
        start_index = 0
        
        if progress and progress.get('phase') == 'collecting':
            print(f"✅ 이전 진행 상황 발견: {progress['current_index']}/{progress['total_count']}")
            start_index = progress['current_index']
            self.load_data_cache()
        
        try:
            from pykrx import stock
            
            # 진행률 바 설정
            pbar = tqdm(total=len(self.non_financial_tickers),
                       initial=start_index,
                       desc="📊 데이터 수집",
                       unit="개",
                       bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]')
            
            for i, company_info in enumerate(self.non_financial_tickers[start_index:], start_index):
                ticker = company_info['ticker']
                company_name = company_info['name']
                
                try:
                    # 이미 수집된 데이터 스킵
                    sheet_name = f"{company_name}_{ticker}"
                    if sheet_name in self.collected_data:
                        pbar.set_postfix({'성공': len(self.collected_data), '실패': len(self.failed_tickers)})
                        pbar.update(1)
                        continue
                    
                    # 주식 데이터 수집
                    ohlcv = self.get_stock_data_with_retry(stock, ticker)
                    
                    if not ohlcv.empty and len(ohlcv) > 100:  # 최소 100일 이상
                        # 데이터 정리
                        ohlcv.columns = ['시가', '고가', '저가', '종가', '거래량']
                        ohlcv.index = ohlcv.index.strftime('%Y-%m-%d')
                        
                        # 시트명 길이 제한 (Excel 31자 제한)
                        safe_sheet_name = sheet_name[:30] if len(sheet_name) > 30 else sheet_name
                        self.collected_data[safe_sheet_name] = ohlcv
                        
                        pbar.set_postfix({
                            '성공': len(self.collected_data), 
                            '실패': len(self.failed_tickers),
                            '현재': company_name[:10]
                        })
                    else:
                        self.failed_tickers.append({
                            'ticker': ticker,
                            'name': company_name,
                            'reason': f'데이터 부족 ({len(ohlcv)}개)'
                        })
                    
                    # 진행 상황 저장 (매 5개마다)
                    if (i + 1) % 5 == 0:
                        self.save_progress(i + 1, len(self.non_financial_tickers), "collecting")
                        self.save_data_cache()
                    
                    pbar.update(1)
                    
                except KeyboardInterrupt:
                    print(f"\n⚠️ 사용자 중단 요청 - 진행 상황 저장 중...")
                    self.save_progress(i, len(self.non_financial_tickers), "collecting")
                    self.save_data_cache()
                    pbar.close()
                    return
                except Exception as e:
                    self.failed_tickers.append({
                        'ticker': ticker,
                        'name': company_name,
                        'reason': str(e)
                    })
                    pbar.set_postfix({'오류': str(e)[:20]})
                    continue
            
            pbar.close()
            
            print(f"\n✅ 데이터 수집 완료:")
            print(f"   성공: {len(self.collected_data)}개")
            print(f"   실패: {len(self.failed_tickers)}개")
            
            # 완료 후 진행 상황 업데이트
            self.save_progress(len(self.non_financial_tickers), len(self.non_financial_tickers), "collecting_complete")
            self.save_data_cache()
            
        except Exception as e:
            print(f"❌ 데이터 수집 중 오류: {str(e)}")
    
    def save_to_excel(self):
        """엑셀 파일로 저장"""
        if not self.collected_data:
            print("❌ 저장할 데이터가 없습니다.")
            return
        
        filename = f"KOSPI_KOSDAQ_비금융기업_전체_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        print(f"\n💾 엑셀 파일 저장 중: {filename}")
        print(f"   저장 대상: {len(self.collected_data)}개 기업")
        
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # 1. 전체 요약 시트
                summary_data = []
                for sheet_name, df in self.collected_data.items():
                    parts = sheet_name.split('_')
                    ticker = parts[-1]
                    company_name = '_'.join(parts[:-1])
                    
                    if len(df) > 0:
                        first_price = df.iloc[0]['종가']
                        last_price = df.iloc[-1]['종가']
                        return_rate = ((last_price / first_price) - 1) * 100
                        
                        # 시장 구분
                        market = next((info['market'] for info in self.non_financial_tickers 
                                     if info['ticker'] == ticker), 'Unknown')
                        
                        summary_data.append({
                            '시장': market,
                            '기업명': company_name,
                            '종목코드': ticker,
                            '시작일': df.index[0],
                            '종료일': df.index[-1],
                            '시작가': first_price,
                            '종료가': last_price,
                            '9년수익률(%)': round(return_rate, 2),
                            '연평균수익률(%)': round((((last_price / first_price) ** (1/9)) - 1) * 100, 2),
                            '데이터수': len(df)
                        })
                
                # 요약 시트 저장
                summary_df = pd.DataFrame(summary_data)
                summary_df = summary_df.sort_values('9년수익률(%)', ascending=False)
                summary_df.to_excel(writer, sheet_name='전체요약', index=False)
                
                # 2. 실패 목록 시트
                if self.failed_tickers:
                    failed_df = pd.DataFrame(self.failed_tickers)
                    failed_df.to_excel(writer, sheet_name='수집실패목록', index=False)
                
                # 3. 개별 기업 시트들 (상위 100개만)
                saved_count = 0
                for sheet_name, df in list(self.collected_data.items())[:100]:
                    try:
                        df.to_excel(writer, sheet_name=sheet_name, index=True)
                        saved_count += 1
                    except Exception as e:
                        print(f"   ⚠️ 시트 저장 실패: {sheet_name} - {str(e)}")
                        continue
                
                print(f"   ✅ 저장 완료: {saved_count}개 기업 시트")
            
            print(f"\n🎉 저장 성공!")
            print(f"📁 파일명: {filename}")
            print(f"📊 포함 내용:")
            print(f"   • 전체요약 시트: {len(summary_data)}개 기업")
            print(f"   • 개별기업 시트: {min(100, len(self.collected_data))}개")
            print(f"   • 실패목록 시트: {len(self.failed_tickers)}개")
            
            # 상위 10개 기업 미리보기
            if summary_data:
                print(f"\n🏆 9년 수익률 상위 10개 기업:")
                for i, item in enumerate(summary_data[:10], 1):
                    print(f"   {i:2d}. {item['기업명']} ({item['종목코드']}): {item['9년수익률(%)']}%")
            
        except Exception as e:
            print(f"❌ 엑셀 저장 실패: {str(e)}")
    
    def run_full_collection(self):
        """전체 수집 프로세스 실행"""
        print("🚀 KOSPI/KOSDAQ 상장 비금융 기업 전체 데이터 수집 시작!")
        print("="*60)
        
        try:
            from pykrx import stock
            print("✅ PyKRX 라이브러리 로드 완료")
        except ImportError:
            print("❌ PyKRX 설치 필요: pip install pykrx")
            return
        
        # 이전 진행 상황 확인
        progress = self.load_progress()
        if progress:
            print(f"📋 이전 진행 상황 발견:")
            print(f"   단계: {progress['phase']}")
            print(f"   진행률: {progress['current_index']}/{progress['total_count']}")
            print(f"   수집된 기업: {progress.get('collected_count', 0)}개")
            print(f"   시간: {progress['timestamp']}")
            
            resume = input("이어서 진행하시겠습니까? (y/n): ").lower()
            if resume != 'y':
                print("새로 시작합니다.")
                # 진행 파일들 삭제
                for file in [self.progress_file, self.data_cache_file]:
                    if os.path.exists(file):
                        os.remove(file)
            else:
                self.load_data_cache()
        
        # 1단계: 전체 티커 목록 가져오기
        self.kospi_tickers, self.kosdaq_tickers = self.get_all_tickers()
        all_tickers = self.kospi_tickers + self.kosdaq_tickers
        
        if not all_tickers:
            print("❌ 티커 목록을 가져올 수 없습니다.")
            return
        
        # 2단계: 비금융 기업 필터링
        if not self.non_financial_tickers or progress is None or progress.get('phase') in ['filtering']:
            self.filter_non_financial_companies(all_tickers)
        
        # 3단계: 주식 데이터 수집
        if self.non_financial_tickers:
            self.collect_stock_data()
        
        # 4단계: 엑셀 파일 저장
        if self.collected_data:
            self.save_to_excel()
        
        # 정리
        print("\n" + "="*60)
        print("🎯 수집 완료!")
        print(f"📊 최종 결과:")
        print(f"   • 전체 상장 기업: {len(all_tickers)}개")
        print(f"   • 비금융 기업: {len(self.non_financial_tickers)}개")
        print(f"   • 데이터 수집 성공: {len(self.collected_data)}개")
        print(f"   • 데이터 수집 실패: {len(self.failed_tickers)}개")
        
        # 임시 파일 정리 여부 묻기
        cleanup = input("\n임시 파일들을 삭제하시겠습니까? (y/n): ").lower()
        if cleanup == 'y':
            for file in [self.progress_file, self.data_cache_file, self.ticker_list_file]:
                if os.path.exists(file):
                    os.remove(file)
                    print(f"🗑️ {file} 삭제됨")

def main():
    print("💼 KOSPI/KOSDAQ 상장 비금융 기업 전체 데이터 수집기")
    print("📋 특징:")
    print("  • API 호출 제한 회피 (랜덤 지연)")
    print("  • 중간 중단/재개 기능")
    print("  • 실시간 진행률 표시")
    print("  • 자동 데이터 캐싱")
    print("  • 금융업 자동 제외")
    print("")
    
    # 패키지 설치 확인
    try:
        import tqdm
    except ImportError:
        print("❌ 필요한 패키지 설치:")
        print("pip install tqdm")
        return
    
    collector = RobustKRXCollector()
    collector.run_full_collection()

if __name__ == "__main__":
    main()
