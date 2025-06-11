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

인터랙티브 예제는 `notebooks/` 디렉터리를 참고하세요.

## 향후 계획

추후 GPT API 연동과 데이터 수집 자동화 등을 검토하고 있습니다. 현재 저장소에는 예제 모델 학습 코드와 DART 데이터 다운로드 스크립트만 포함되어 있습니다.
