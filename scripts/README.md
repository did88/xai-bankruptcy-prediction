# DART 재무제표 대량 다운로드 스크립트

이 디렉토리는 한국 전자공시시스템(DART) API를 사용하여 상장기업의 재무제표 데이터를 대량으로 다운로드하는 스크립트들을 포함합니다.

## 📁 파일 구조

### 🔧 핵심 모듈
- **`dart_bulk_downloader.py`**: DART API 호출 및 데이터 처리를 위한 핵심 유틸리티 모듈 (개선된 재시도 로직 포함)
- **`team_dart_downloader_fixed.py`**: 팀별 분할 다운로드를 위한 메인 스크립트 (레거시)
- **`cached_team_downloader.py`**: 캐시 기능이 포함된 안정적인 다운로더 (권장)
- **`robust_team_downloader.py`**: 네트워크 안정성이 개선된 다운로더

### 🧪 테스트 스크립트
- **`test_dart_comprehensive.py`**: 광범위한 DART API 테스트 (실제 데이터가 있는 기업 찾기)
- **`test_known_good.py`**: 성공이 확인된 기업들로 다운로드 테스트
- **`debug_dart_api.py`**: 기본 DART API 디버깅 스크립트
- **`test_connection.py`**: DART API 연결 상태 진단 도구

### 📊 자동화 스크립트
- **`download_by_person.bat`**: 담당자별 팀 다운로드 배치 파일 (4명 분산 처리)
- **`download_teams_2_to_10.bat`**: 팀 2-10 자동 다운로드 배치 파일
- **`download_all_teams.bat`**: 전체 팀 자동 다운로드 배치 파일

## 🚀 사용법

### 1. 환경 설정

**API 키 설정**: 프로젝트 루트에 `.env` 파일 생성
```bash
DART_API_KEY=your_dart_api_key_here
```

**의존성 설치**: 
```bash
pip install aiohttp pandas openpyxl python-dotenv tqdm
```

### 2. 기본 테스트

**연결 상태 확인**:
```bash
python test_connection.py
```

**성공 확인된 기업들로 테스트**:
```bash
python test_known_good.py
```

### 3. 대량 다운로드 (권장 방법)

**캐시 활용 다운로더 사용** (권장):
```bash
# 첫 실행 시 (캐시 생성)
python cached_team_downloader.py --team 4 --skip-validation

# 이후 실행 시 (캐시 활용으로 빠른 시작)
python cached_team_downloader.py --team 5 --skip-validation
python cached_team_downloader.py --team 6 --skip-validation
```

**4명 분산 처리** (Windows):
```bash
# 대화형 메뉴로 담당자별 실행
download_by_person.bat
```

**기존 스크립트** (캐시 없음):
```bash
# 레거시 방식
python team_dart_downloader_fixed.py --team 4 --skip-validation
```

### 4. 캐시 관리

**캐시 상태 확인**:
```bash
# 캐시 파일 위치: C:\apps\xai-bankruptcy-prediction\data\corp_codes_cache.pkl
dir C:\apps\xai-bankruptcy-prediction\data\corp_codes_cache.pkl
```

**캐시 관리 명령어**:
```bash
# 캐시 삭제
python cached_team_downloader.py --clear-cache

# 캐시 없이 강제 새로고침
python cached_team_downloader.py --team 4 --no-cache
```

### 5. 결과 확인 및 병합

**팀 목록 확인**:
```bash
python team_dart_downloader_fixed.py --list-teams
```

**모든 팀 파일 병합**:
```bash
python team_dart_downloader_fixed.py --merge-only
```

## 📊 데이터 구조

### 수집되는 데이터
- **기업 정보**: 기업코드, 기업명, 주식코드
- **연도**: 2015-2022년 (8년간)
- **재무제표 구분**: 연결재무제표(CFS) 또는 개별재무제표(OFS)
- **계정 항목**: 유동자산, 비유동자산, 자산총계, 부채, 자본 등
- **금액 정보**: 당기, 전기, 전전기 금액

### 출력 파일
- **개별 팀 파일**: `data/team_downloads/dart_statements_team_XX.xlsx`
- **병합 파일**: `data/dart_statements_merged.xlsx`
- **테스트 파일**: `data/dart_test_success.xlsx`
- **캐시 파일**: `data/corp_codes_cache.pkl` (기업 코드 목록)

### 데이터 컬럼 (22개)
```
corp_code, corp_name, stock_code, bsns_year, rcept_no, reprt_code, 
fs_div, fs_nm, sj_div, sj_nm, account_nm, thstrm_nm, thstrm_dt, 
thstrm_amount, frmtrm_nm, frmtrm_dt, frmtrm_amount, bfefrmtrm_nm, 
bfefrmtrm_dt, bfefrmtrm_amount, ord, currency
```

## ⚙️ 성능 및 제약사항

### API 제한
- **속도 제한**: 분당 800회 요청
- **일일 제한**: 20,000회 요청/API키
- **동시 연결**: 최대 10개 워커
- **재시도 로직**: 네트워크 오류 시 자동 재시도 (최대 3회)

### 처리 규모
- **전체 기업 수**: 약 3,653개 비금융 상장기업
- **유효 기업 수**: 약 2,509개 (사업보고서 보유)
- **팀별 크기**: 100개 기업씩
- **총 팀 수**: 26개 팀
- **총 API 요청 수**: 약 20,072회

### 성능 지표
- **팀당 소요시간**: 1-2분 (캐시 활용 시)
- **전체 소요시간**: 26-52분 (1명), 7-13분 (4명 병렬)
- **데이터 크기**: 팀당 약 15,000-25,000행
- **캐시 효과**: 첫 실행 후 30-60초 단축

## 🎯 4명 분산 처리 계획

### 현재 진행 상황 (2025-06-11 기준)
- ✅ **완료된 팀**: 1, 2, 3 (11.5% 완료)
- ⏳ **남은 팀**: 4-26 (23개 팀)

### 담당자별 분배
| 담당자 | 팀 범위 | 팀 수 | 예상 시간 |
|--------|---------|-------|-----------|
| **담당자 1** | 팀 4-9 | 6팀 | 6-12분 |
| **담당자 2** | 팀 10-15 | 6팀 | 6-12분 |
| **담당자 3** | 팀 16-21 | 6팀 | 6-12분 |
| **담당자 4** | 팀 22-26 | 5팀 | 5-10분 |

### API 사용량 (일일 20,000회 제한 기준)
- **1명당 API 사용량**: 약 4,600-5,600회 (제한 내 ✅)
- **전체 완료 시간**: 병렬 처리 시 **12-15분**

## 🔧 문제 해결

### 일반적인 문제
1. **"조회된 데이타가 없습니다" (API 오류 013)**: 정상적인 현상 (해당 기업/연도에 데이터 없음)
2. **네트워크 연결 오류**: `cached_team_downloader.py` 사용 또는 잠시 대기 후 재시도
3. **ZIP 파일 오류**: 재시도 로직이 자동으로 처리 (최대 3회)
4. **API 키 오류**: `.env` 파일의 API 키 확인

### 디버깅 도구
- **연결 진단**: `python test_connection.py`
- **API 테스트**: `python debug_dart_api.py`
- **광범위 테스트**: `python test_dart_comprehensive.py`

### 캐시 관련 문제
- **캐시 파일 손상**: `python cached_team_downloader.py --clear-cache`
- **오래된 캐시**: `python cached_team_downloader.py --no-cache`

## 📈 사용 예시

### 권장 워크플로우 (캐시 활용)
```bash
# 1. 연결 테스트
python test_connection.py

# 2. 캐시 생성과 함께 첫 팀 실행
python cached_team_downloader.py --team 4 --skip-validation

# 3. 이후 팀들은 캐시 활용으로 빠른 실행
python cached_team_downloader.py --team 5 --skip-validation
python cached_team_downloader.py --team 6 --skip-validation

# 4. 또는 자동화 스크립트 사용
download_by_person.bat
```

### 4명 동시 실행 (최고 효율)
```bash
# 각자 다른 터미널에서 동시 실행
담당자1: download_by_person.bat (선택지 1)
담당자2: download_by_person.bat (선택지 2)  
담당자3: download_by_person.bat (선택지 3)
담당자4: download_by_person.bat (선택지 4)

# 모든 완료 후 병합
download_by_person.bat (선택지 5)
```

### 레거시 방식 (캐시 없음)
```bash
# 기존 방식 (매번 기업 코드 목록 다운로드)
python team_dart_downloader_fixed.py --team 4 --skip-validation
```

## 🎯 최종 결과물

성공적인 실행 후 다음을 얻을 수 있습니다:
- **총 데이터량**: 약 **50만-100만 행**의 재무제표 데이터
- **파일 크기**: 약 **100-200MB**
- **기업 수**: **2,509개** 상장기업
- **연도 범위**: **2015-2022 (8년)**
- **계정 항목**: 기업당 연도당 평균 **25개** 재무 항목

## 🆕 최신 기능

### 캐시 시스템 (v2.0)
- ✅ **기업 코드 목록 캐싱**: 첫 실행 후 로컬 저장
- ✅ **빠른 재시작**: 30-60초 시간 단축
- ✅ **네트워크 안정성**: 연결 문제 시에도 캐시 활용

### 개선된 오류 처리
- ✅ **자동 재시도**: 네트워크 오류 시 최대 3회 재시도
- ✅ **점진적 대기**: 재시도 간격 자동 조정
- ✅ **상세한 로그**: 문제 진단을 위한 자세한 정보

## 📞 지원

문제 발생 시:
1. **연결 테스트** 먼저 실행: `python test_connection.py`
2. **캐시 초기화**: `python cached_team_downloader.py --clear-cache`
3. **로그 메시지** 확인하여 원인 파악
4. **GitHub Issues**에 오류 로그와 함께 문제 보고

## 🏃‍♂️ 빠른 시작

**지금 당장 시작하기**:
```bash
# 캐시 생성과 함께 팀 4 다운로드
python cached_team_downloader.py --team 4 --skip-validation
```

---

> **참고**: 이 스크립트는 공개된 DART API를 사용하며, 금융감독원의 이용약관을 준수합니다. 상업적 이용 시 별도 허가가 필요할 수 있습니다.

> **업데이트**: 2025-06-11 - 캐시 시스템 추가, 4명 분산 처리 계획 수립, 팀 1-3 완료 반영
