# XAI 기반 기업 부실위험 예측

이 프로젝트는 머신러닝과 딥러닝 모델을 활용하여 기업의 부실위험을 예측하고, SHAP, LIME, Integrated Gradients, DeepLIFT와 같은 XAI 기법을 통해 예측 결과를 설명합니다.

## 주요 기능
- Logistic Regression, Bayesian Ridge, RandomForest, XGBoost, DNN, CNN, LSTM 모델 지원
- SHAP, LIME, Integrated Gradients, DeepLIFT를 이용한 설명 가능성 확보
- 데이터 전처리, 모델 학습, 성능 평가, 설명 생성 모듈 제공

## 설치 방법
```bash
pip install -r requirements.txt
```

## 실행 예시
```bash
python src/trainer.py
```

자세한 사용 방법과 예제는 `notebooks/` 폴더의 노트북을 참고하세요.
