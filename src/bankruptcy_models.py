import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.linear_model import LogisticRegression
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from lightgbm import LGBMClassifier
import torch
from torch import nn
from torch.utils.data import DataLoader, TensorDataset


def load_dataset(path: str):
    """Load dataset from an Excel file. The file must contain a 'target' column."""
    df = pd.read_excel(path)
    if 'target' not in df.columns:
        raise ValueError("Excel file must contain a 'target' column")
    X = df.drop('target', axis=1).values
    y = df['target'].values
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    return X_train, X_test, y_train, y_test


# ----- Traditional ML models -----

def train_logistic_regression(X_train, y_train):
    model = LogisticRegression(max_iter=1000)
    model.fit(X_train, y_train)
    return model


def train_mda(X_train, y_train):
    model = LinearDiscriminantAnalysis()
    model.fit(X_train, y_train)
    return model


def train_lightgbm(X_train, y_train):
    model = LGBMClassifier()
    model.fit(X_train, y_train)
    return model


# ----- PyTorch helpers -----

class SimpleRNN(nn.Module):
    def __init__(self, input_dim, hidden_dim=16):
        super().__init__()
        self.rnn = nn.RNN(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        x = x.unsqueeze(1)
        out, _ = self.rnn(x)
        out = self.fc(out[:, -1])
        return torch.sigmoid(out).squeeze()


class SimpleLSTM(nn.Module):
    def __init__(self, input_dim, hidden_dim=16):
        super().__init__()
        self.rnn = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        x = x.unsqueeze(1)
        out, _ = self.rnn(x)
        out = self.fc(out[:, -1])
        return torch.sigmoid(out).squeeze()


class SimpleGRU(nn.Module):
    def __init__(self, input_dim, hidden_dim=16):
        super().__init__()
        self.rnn = nn.GRU(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, 1)

    def forward(self, x):
        x = x.unsqueeze(1)
        out, _ = self.rnn(x)
        out = self.fc(out[:, -1])
        return torch.sigmoid(out).squeeze()


def train_torch_model(model, X_train, y_train, epochs=10):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    model.to(device)
    X_tensor = torch.tensor(X_train, dtype=torch.float32)
    y_tensor = torch.tensor(y_train, dtype=torch.float32)
    dataset = TensorDataset(X_tensor, y_tensor)
    loader = DataLoader(dataset, batch_size=32, shuffle=True)
    criterion = nn.BCELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

    model.train()
    for _ in range(epochs):
        for xb, yb in loader:
            xb, yb = xb.to(device), yb.to(device)
            optimizer.zero_grad()
            preds = model(xb)
            loss = criterion(preds, yb)
            loss.backward()
            optimizer.step()
    return model


# ----- Convenience function -----

def evaluate(model, X_test, y_test):
    if isinstance(model, nn.Module):
        model.eval()
        device = next(model.parameters()).device
        X_tensor = torch.tensor(X_test, dtype=torch.float32).to(device)
        with torch.no_grad():
            preds = model(X_tensor).cpu().numpy() > 0.5
    else:
        preds = model.predict(X_test)
    return accuracy_score(y_test, preds)


def train_all_models(path: str):
    X_train, X_test, y_train, y_test = load_dataset(path)

    models = {
        'logistic_regression': train_logistic_regression(X_train, y_train),
        'mda': train_mda(X_train, y_train),
        'lightgbm': train_lightgbm(X_train, y_train),
    }

    input_dim = X_train.shape[1]
    rnn = train_torch_model(SimpleRNN(input_dim), X_train, y_train)
    lstm = train_torch_model(SimpleLSTM(input_dim), X_train, y_train)
    gru = train_torch_model(SimpleGRU(input_dim), X_train, y_train)

    models.update({'rnn': rnn, 'lstm': lstm, 'gru': gru})

    scores = {name: evaluate(m, X_test, y_test) for name, m in models.items()}
    return models, scores


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Train bankruptcy prediction models from Excel data')
    parser.add_argument('excel_path', type=str, help='Path to Excel file with training data')
    args = parser.parse_args()

    models, scores = train_all_models(args.excel_path)
    for name, score in scores.items():
        print(f'{name}: {score:.4f}')

