# DART 다운로드 설정 파일
# 이 파일을 config.py로 저장하고 각 스크립트에서 import해서 사용하세요

# API 설정
DART_SINGLE_ACCOUNT_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json"
DEFAULT_WORKERS = 10
DEFAULT_CHUNK_SIZE = 100
API_RATE_LIMIT = 800  # 분당 요청 수
API_RATE_PERIOD = 60.0  # 초
API_TIMEOUT = 10  # 초
MAX_RETRIES = 2

# 데이터 필터링 설정
FINANCIAL_KEYWORDS = [
    "금융", "은행", "보험", "증권", "캐피탈", "투자", 
    "자산운용", "신용", "저축", "카드", "리스", "신탁", "펀드"
]

# 기본 연도 범위
DEFAULT_START_YEAR = 2015
DEFAULT_END_YEAR = 2022

# 로그 설정
MAX_ERROR_LOGS = 5
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# 파일 경로 설정
DATA_DIR = "data"
TEAM_DOWNLOADS_DIR = "team_downloads"
LOGS_DIR = "logs"
