import torch
import torch.nn as nn

class TransformerClassifier(nn.Module):
    """간단한 트랜스포머 기반 분류기."""

    def __init__(self, input_dim: int, nhead: int, num_layers: int, num_classes: int):
        super().__init__()
        self.embedding = nn.Linear(input_dim, input_dim)
        encoder_layer = nn.TransformerEncoderLayer(d_model=input_dim, nhead=nhead)
        self.transformer = nn.TransformerEncoder(encoder_layer, num_layers=num_layers)
        self.classifier = nn.Linear(input_dim, num_classes)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        """전방 계산."""
        # x: (batch, seq_len, input_dim)
        x = self.embedding(x)
        x = x.permute(1, 0, 2)  # (seq_len, batch, input_dim)
        x = self.transformer(x)
        x = x.mean(dim=0)
        return self.classifier(x)
