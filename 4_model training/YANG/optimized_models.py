import pandas as pd
from sklearn.model_selection import GridSearchCV, StratifiedKFold
from sklearn.metrics import f1_score, make_scorer, classification_report
from sklearn.ensemble import RandomForestClassifier
from lightgbm import LGBMClassifier
from imblearn.over_sampling import SMOTE
from imblearn.pipeline import Pipeline


def load_data():
    """Load training and validation sets."""
    train = pd.read_csv('../../3_Post-Feature Engineering/YANG/train_data_with_smote.csv')
    val = pd.read_csv('../../3_Post-Feature Engineering/YANG/val_data.csv')
    X_train = train.drop(columns='is_defaulted')
    y_train = train['is_defaulted']
    X_val = val.drop(columns='is_defaulted')
    y_val = val['is_defaulted']
    return X_train, X_val, y_train, y_val


def optimize_random_forest(X, y):
    pipeline = Pipeline([
        ('smote', SMOTE(random_state=42)),
        ('rf', RandomForestClassifier(random_state=42))
    ])
    param_grid = {
        'rf__n_estimators': [200, 300, 500],
        'rf__max_depth': [None, 10, 20, 30],
        'rf__min_samples_split': [2, 5, 10],
        'rf__min_samples_leaf': [1, 2, 4],
        'rf__class_weight': [None, 'balanced']
    }
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    grid = GridSearchCV(pipeline, param_grid, scoring='f1', cv=cv, n_jobs=-1, verbose=2)
    grid.fit(X, y)
    return grid


# def optimize_lightgbm(X, y):
#     pipeline = Pipeline([
#         ('smote', SMOTE(random_state=42)),
#         ('lgb', LGBMClassifier(random_state=42))
#     ])
#     param_grid = {
#         'lgb__n_estimators': [200, 300, 500],
#         'lgb__learning_rate': [0.01, 0.05, 0.1],
#         'lgb__num_leaves': [31, 63, 127],
#         'lgb__max_depth': [-1, 10, 20],
#         'lgb__min_child_samples': [20, 40, 60],
#         'lgb__subsample': [0.8, 0.9, 1.0],
#         'lgb__colsample_bytree': [0.8, 0.9, 1.0],
#         'lgb__class_weight': [None, 'balanced']
#     }
#     cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
#     grid = GridSearchCV(pipeline, param_grid, scoring='f1', cv=cv, n_jobs=-1, verbose=2)
#     grid.fit(X, y)
#     return grid


def evaluate(model, X_val, y_val, name):
    preds = model.predict(X_val)
    f1 = f1_score(y_val, preds)
    print(f"\n{name} best params: {model.best_params_}")
    print(f"{name} validation F1: {f1:.5f}")
    print(classification_report(y_val, preds))


def main():
    X_train, X_val, y_train, y_val = load_data()
    rf_grid = optimize_random_forest(X_train, y_train)
    evaluate(rf_grid, X_val, y_val, 'RandomForest')

    lgb_grid = optimize_lightgbm(X_train, y_train)
    evaluate(lgb_grid, X_val, y_val, 'LightGBM')


if __name__ == '__main__':
    main()
