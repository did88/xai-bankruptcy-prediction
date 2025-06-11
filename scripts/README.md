# DART 재무제표 다운로드 시스템

한국 DART(전자공시시스템)에서 상장기업의 재무제표를 효율적으로 다운로드하는 시스템입니다.

## 🚀 주요 기능

- **비동기 병렬 처리**: 여러 기업의 재무제표를 동시에 다운로드
- **팀별 작업 분할**: 대용량 데이터를 팀 단위로 나누어 처리
- **속도 제한**: DART API 제한사항을 준수한 안정적 다운로드
- **재시도 메커니즘**: 실패한 요청에 대한 자동 재시도
- **포괄적 로깅**: 상세한 로그 기록으로 문제 추적 가능
- **데이터 검증**: 다운로드된 데이터의 유효성 검사

## 📁 파일 구조

```
scripts/
├── dart_bulk_downloader.py      # 핵심 다운로드 엔진
├── team_dart_downloader_fixed.py # 개선된 팀별 다운로드 스크립트
├── config.py                     # 설정 파일
├── utils.py                      # 공통 유틸리티 함수
├── CORPCODE.xml                  # 기업 코드 데이터
└── README.md                     # 이 문서
```

## 🛠️ 설치 및 설정

### 1. 필요한 패키지 설치

```bash
pip install pandas aiohttp python-dotenv openpyxl tqdm requests
```

### 2. API 키 설정

프로젝트 루트에 `.env` 파일을 생성하고 DART API 키를 추가:

```bash
DART_API_KEY=your_api_key_here
```

> DART API 키는 [DART 오픈API](https://opendart.fss.or.kr/)에서 발급받을 수 있습니다.

## 📖 사용법

### 기본 사용법

#### 1. 팀 분할 정보 확인

```bash
python team_dart_downloader_fixed.py --list-teams
```

#### 2. 특정 팀 데이터 다운로드

```bash
# 팀 1 다운로드
python team_dart_downloader_fixed.py --team 1

# 팀 2 다운로드 (더 많은 워커 사용)
python team_dart_downloader_fixed.py --team 2 --workers 15
```

#### 3. 팀별 파일 병합

```bash
python team_dart_downloader_fixed.py --merge-only
```

### 고급 옵션

```bash
# 연도 범위 설정
python team_dart_downloader_fixed.py --team 1 --start-year 2018 --end-year 2023

# 팀당 기업 수 조정
python team_dart_downloader_fixed.py --team 1 --chunk-size 50

# 유효성 검증 건너뛰기 (빠른 테스트용)
python team_dart_downloader_fixed.py --team 1 --skip-validation
```

## ⚙️ 설정 옵션

`config.py`에서 다음 설정들을 조정할 수 있습니다:

- `DEFAULT_WORKERS`: 기본 동시 작업 수 (10)
- `DEFAULT_CHUNK_SIZE`: 팀당 기업 수 (100)
- `API_RATE_LIMIT`: 분당 API 요청 수 (800)
- `DEFAULT_START_YEAR`: 기본 시작 연도 (2015)
- `DEFAULT_END_YEAR`: 기본 종료 연도 (2022)

## 📊 출력 파일

### 팀별 파일
- 위치: `data/team_downloads/`
- 형식: `dart_statements_team_01.xlsx`, `dart_statements_team_02.xlsx`, ...

### 병합된 파일
- 위치: `data/dart_statements_merged.xlsx`
- 내용: 모든 팀 데이터가 합쳐진 최종 파일

### 로그 파일
- 위치: `logs/`
- 형식: `dart_downloader_YYYYMMDD_HHMMSS.log`

## 🔍 데이터 구조

다운로드된 Excel 파일은 다음과 같은 컬럼을 포함합니다:

- `corp_code`: 기업 코드
- `bsns_year`: 사업연도
- `account_nm`: 계정명
- `fs_div`: 재무제표 구분
- `thstrm_amount`: 당기금액
- `frmtrm_amount`: 전기금액
- 기타 재무제표 관련 컬럼들...

## ⚡ 성능 최적화 팁

1. **워커 수 조정**: 네트워크 환경에 따라 `--workers` 값을 조정
2. **팀 크기 최적화**: 메모리 사용량에 따라 `--chunk-size` 조정
3. **유효성 검증**: 테스트 시에는 `--skip-validation` 사용
4. **재시도 설정**: `config.py`에서 `MAX_RETRIES` 값 조정

## 🚨 주의사항

- DART API는 분당 요청 수 제한이 있으므로 속도 제한을 준수해야 합니다
- 대용량 데이터 다운로드 시 충분한 디스크 공간을 확보하세요
- API 키는 절대 공개하지 마세요 (`.env` 파일을 `.gitignore`에 추가)

## 🐛 문제 해결

### 일반적인 오류

1. **API 키 오류**
   ```
   EnvironmentError: DART_API_KEY 환경변수를 설정하세요
   ```
   → `.env` 파일에 올바른 API 키를 설정했는지 확인

2. **속도 제한 오류**
   ```
   API error 020 for [corp_code] [year]: 호출횟수 제한 초과
   ```
   → `config.py`에서 `API_RATE_LIMIT` 값을 낮춰보세요

3. **메모리 부족**
   → `--chunk-size` 값을 줄이거나 `--workers` 수를 줄여보세요

### 로그 확인

상세한 오류 정보는 `logs/` 디렉토리의 로그 파일에서 확인할 수 있습니다.

## 📈 예상 처리 시간

기업 수와 연도에 따른 대략적인 처리 시간:

- 100개 기업 × 5년 = 약 10-15분
- 500개 기업 × 8년 = 약 1-2시간
- 전체 상장기업 × 8년 = 약 4-8시간

> 실제 시간은 네트워크 상태와 서버 응답 속도에 따라 달라질 수 있습니다.

## 🤝 기여하기

버그 리포트나 기능 개선 제안은 이슈로 등록해주세요.

## 📄 라이선스

이 프로젝트는 교육 및 연구 목적으로 사용되며, 상업적 사용 시 DART API 이용약관을 준수해야 합니다.
