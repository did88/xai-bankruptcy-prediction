# DART 재무제표 대량 다운로드 스크립트

이 디렉토리는 한국 전자공시시스템(DART) API를 사용하여 상장기업의 재무제표 데이터를 대량으로 다운로드하는 스크립트들을 포함합니다.

## 📁 파일 구조

### 🔧 핵심 모듈
- **`dart_bulk_downloader.py`**: DART API 호출 및 데이터 처리를 위한 핵심 유틸리티 모듈
- **`team_dart_downloader_fixed.py`**: 팀별 분할 다운로드를 위한 메인 스크립트 (개선된 버전)
- **`robust_team_downloader.py`**: 네트워크 안정성이 개선된 다운로더

### 🧪 테스트 스크립트
- **`test_dart_comprehensive.py`**: 광범위한 DART API 테스트 (실제 데이터가 있는 기업 찾기)
- **`test_known_good.py`**: 성공이 확인된 기업들로 다운로드 테스트
- **`debug_dart_api.py`**: 기본 DART API 디버깅 스크립트
- **`test_connection.py`**: DART API 연결 상태 진단 도구

### 📊 자동화 스크립트
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

### 3. 대량 다운로드

**팀별 수동 다운로드** (권장):
```bash
# 팀 1 다운로드 (유효성 검증 포함, 첫 실행 시)
python team_dart_downloader_fixed.py --team 1

# 팀 2부터는 유효성 검증 스킵하여 빠른 진행
python team_dart_downloader_fixed.py --team 2 --skip-validation
python team_dart_downloader_fixed.py --team 3 --skip-validation
```

**네트워크 문제 시 안정성 개선 버전**:
```bash
python robust_team_downloader.py --team 5 --skip-validation
```

**자동화 다운로드** (Windows):
```bash
# 팀 2-10 자동 다운로드
download_teams_2_to_10.bat

# 전체 팀 자동 다운로드 (팀 2-26 + 병합)
download_all_teams.bat
```

### 4. 결과 확인 및 병합

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
- **동시 연결**: 최대 10개 워커
- **재시도 로직**: 네트워크 오류 시 자동 재시도

### 처리 규모
- **전체 기업 수**: 약 3,653개 비금융 상장기업
- **유효 기업 수**: 약 2,509개 (사업보고서 보유)
- **팀별 크기**: 100개 기업씩
- **예상 총 팀 수**: 26개 팀

### 성능 지표
- **팀당 소요시간**: 1-2분 (유효성 검증 스킵 시)
- **전체 소요시간**: 25-30분 (26개 팀)
- **데이터 크기**: 팀당 약 15,000-25,000행

## 🔧 문제 해결

### 일반적인 문제
1. **"조회된 데이타가 없습니다" (API 오류 013)**: 정상적인 현상 (해당 기업/연도에 데이터 없음)
2. **네트워크 연결 오류**: `robust_team_downloader.py` 사용 또는 잠시 대기 후 재시도
3. **API 키 오류**: `.env` 파일의 API 키 확인

### 디버깅 도구
- **연결 진단**: `python test_connection.py`
- **API 테스트**: `python debug_dart_api.py`
- **광범위 테스트**: `python test_dart_comprehensive.py`

## 📈 사용 예시

### 완전 자동화 워크플로우
```bash
# 1. 연결 테스트
python test_connection.py

# 2. 팀 1 다운로드 (유효성 검증 포함)
python team_dart_downloader_fixed.py --team 1

# 3. 자동화 스크립트 실행
download_all_teams.bat

# 4. 결과 확인
python team_dart_downloader_fixed.py --list-teams
```

### 수동 제어 워크플로우
```bash
# 1-5팀 수동 다운로드
for i in {2..5}; do
    python team_dart_downloader_fixed.py --team $i --skip-validation
done

# 병합
python team_dart_downloader_fixed.py --merge-only
```

## 🎯 최종 결과물

성공적인 실행 후 다음을 얻을 수 있습니다:
- **약 50만-100만 행**의 재무제표 데이터
- **2,500개 기업 × 8년 × 평균 25개 계정항목**
- **Excel 형태**로 저장되어 즉시 분석 가능
- **기업명, 재무제표 구분, 연도별** 체계적 정리

## 📞 지원

문제 발생 시:
1. **연결 테스트** 먼저 실행
2. **로그 메시지** 확인
3. **안정성 개선 버전** 시도
4. **GitHub Issues**에 오류 로그와 함께 문제 보고

---

> **참고**: 이 스크립트는 공개된 DART API를 사용하며, 금융감독원의 이용약관을 준수합니다. 상업적 이용 시 별도 허가가 필요할 수 있습니다.
