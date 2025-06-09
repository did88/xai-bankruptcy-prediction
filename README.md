# XAI 파산 예측

이 프로젝트는 머신 러닝과 딥 러닝 모델을 이용해 기업의 파산 위험을 예측합니다. 또한 XAI(설명 가능한 인공지능) 기법을 적용해 모델 결과를 해석합니다.

## 주요 기능
- 회귀 및 분류 모델: 로지스틱 회귀, 베이지안 릿지, 랜덤포레스트, XGBoost, DNN, CNN, LSTM
- XAI 방법: SHAP, LIME, 통합 그래디언트, DeepLIFT

## 설치
```bash
pip install -r requirements.txt
```

## 사용법
```bash
python src/trainer.py
```

인터랙티브 예제는 `notebooks/` 디렉터리를 참고하세요.
