# 팀별 DART 재무제표 다운로드 가이드

## 📋 준비사항

1. **환경 설정**
   ```bash
   # .env 파일에 DART API 키 설정
   DART_API_KEY=your_api_key_here
   ```

2. **필요한 파일 구조**
   ```
   project/
   ├── .env
   ├── scripts/
   │   ├── dart_bulk_downloader.py  # 기존 파일
   │   └── team_dart_downloader.py  # 새로 만든 파일
   └── data/
       └── team_downloads/  # 팀별 다운로드 파일 저장 위치
   ```

## 🚀 사용법

### 1. 팀 정보 확인
먼저 전체 기업이 어떻게 나뉘어져 있는지 확인합니다:

```bash
python team_dart_downloader.py --list-teams
```

출력 예시:
```
📊 팀별 할당 정보:
   팀 1: 100개 기업 (인덱스 0 ~ 99)
   팀 2: 100개 기업 (인덱스 100 ~ 199)
   팀 3: 100개 기업 (인덱스 200 ~ 299)
   ...
```

### 2. 팀별 다운로드
각 팀원은 자신의 팀 번호로 다운로드를 실행합니다:

```bash
# 팀 1 담당자
python team_dart_downloader.py --team 1

# 팀 2 담당자
python team_dart_downloader.py --team 2

# 팀 3 담당자
python team_dart_downloader.py --team 3
```

### 3. 고급 옵션 사용
```bash
# 동시 작업 수를 20으로 늘려서 더 빠르게 다운로드 (네트워크가 안정적일 때)
python team_dart_downloader.py --team 1 --workers 20

# 특정 연도만 다운로드 (2020~2023년)
python team_dart_downloader.py --team 1 --start-year 2020 --end-year 2023
```

### 4. 다운로드 완료 후 병합
모든 팀이 다운로드를 완료한 후:

```bash
python team_dart_downloader.py --merge-only
```

## 📁 출력 파일

- **팀별 파일**: `data/team_downloads/dart_statements_team_01.xlsx`, `dart_statements_team_02.xlsx`, ...
- **병합된 파일**: `data/dart_statements_merged.xlsx`

## ⚡ 성능 팁

1. **동시 작업 수 조정**
   - 안정적인 네트워크: `--workers 20`
   - 불안정한 네트워크: `--workers 5`

2. **API 제한**
   - DART API는 분당 1000건 제한이 있습니다
   - 여러 팀이 동시에 실행하면 제한에 걸릴 수 있으니 시간차를 두고 실행하세요

3. **예상 소요 시간**
   - 100개 기업 × 10년 = 1,000건 요청
   - 약 5~10분 소요 (네트워크 상황에 따라 다름)

## 🔧 문제 해결

1. **다운로드 중단된 경우**
   - 같은 명령어를 다시 실행하면 처음부터 다시 시작합니다
   - 기존 파일은 덮어쓰여집니다

2. **특정 기업만 재다운로드**
   - 현재는 팀 단위로만 다운로드 가능합니다
   - 필요시 코드를 수정하여 특정 기업만 선택할 수 있습니다

## 💡 팀 협업 예시

```bash
# 팀원 A (팀 1 담당)
python team_dart_downloader.py --team 1

# 팀원 B (팀 2 담당)
python team_dart_downloader.py --team 2

# 팀원 C (팀 3 담당)
python team_dart_downloader.py --team 3

# 팀장 (모든 팀 완료 후)
python team_dart_downloader.py --merge-only
```

## 📊 데이터 확인

다운로드된 엑셀 파일에는 다음 정보가 포함됩니다:
- `corp_code`: 기업 코드
- `bsns_year`: 사업 연도
- 각종 재무제표 항목들

병합된 파일은 모든 팀의 데이터를 포함하며, 중복 없이 정리됩니다.