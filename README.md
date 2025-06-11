# 부실예측 모델

이 프로젝트는 머신 러닝과 딥 러닝 모델을 활용해 기업의 파산 위험을 예측합니다. XAI 기능은 제외하였으며 단순히 예측 모델 학습 및 평가에 집중합니다.

## 주요 기능
- 사용 모델: 로지스틱 회귀(LR), 다중 판별 분석(MDA), LightGBM, RNN, LSTM, GRU

## 설치
```bash
pip install -r requirements.txt
```

## 사용법
```bash
python src/bankruptcy_models.py your_data.xlsx
```

### DART 재무제표 수집

다음 스크립트는 DART API를 이용해 KOSPI/KOSDAQ 비금융기업의 재무제표를 병렬로 내려받습니다.

```bash
python -m src.dart_bulk_downloader
```

#### 데이터 수집 기준

1. `https://opendart.fss.or.kr/api/corpCode.xml` 엔드포인트로 전체 기업 코드 목록을 받아옵니다.
2. 6자리 주식코드가 존재하는 기업 중 KOSPI/KOSDAQ 상장사를 선택합니다.
3. 기업명에 `은행`, `보험`, `증권` 등 금융 관련 키워드가 포함된 경우 제외하여 비금융 기업만 남깁니다.
4. 각 `corp_code`에 대해 `https://opendart.fss.or.kr/api/fnlttSinglAcnt.json`을 호출합니다.
   - `bsns_year`는 기본적으로 2015~2022년을 대상으로 합니다.
   - `reprt_code`는 사업보고서(`11011`)를 사용합니다.
   - 우선 연결재무제표(`fs_div=CFS`)를 시도하고, 데이터가 없으면 개별재무제표(`fs_div=OFS`)를 조회합니다.
5. 요청 속도는 분당 600회 이하로 제한하여 DART의 이용 제한에 저촉되지 않도록 합니다.
6. 최종 데이터에는 기업코드, 기업명, 주식코드, 연도, 재무제표 구분 등 22개 컬럼이 포함됩니다.

인터랙티브 예제는 `notebooks/` 디렉터리를 참고하세요.

## 향후 계획

추후 GPT API 연동과 데이터 수집 자동화 등을 검토하고 있습니다. 현재 저장소에는 예제 모델 학습 코드와 DART 데이터 다운로드 스크립트만 포함되어 있습니다.
