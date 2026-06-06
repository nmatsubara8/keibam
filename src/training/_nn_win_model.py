"""Layer1 base学習器②: ニューラルネット（PyTorch）勝率モデル。

血統・各種 ID を Entity Embedding し、数値特徴と結合して勝率（3着以内/1着）を出力する。
GBDT が苦手な「複雑な非線形関係（血統の組合せ・過去数戦パターン）」を担う。

StackingModel から `fit` / `predict_proba` を通じて base学習器として利用される
（sklearn 互換 API）。torch は遅延 import し、未インストール環境でも他モジュールの
import を壊さない（依存の隔離）。隠れ層の過剰追加は過学習を招くため浅め既定（KB 5.2）。
"""

from __future__ import annotations

import numpy as np


class NnWinModel:
    """Entity Embedding + MLP の勝率モデル（sklearn 互換）。

    Parameters
    ----------
    categorical_cardinalities : {列インデックス: カテゴリ数}。Embedding 対象。
    n_numeric : 数値特徴量の数。
    hidden_dims : 隠れ層のユニット数（浅めを既定として過学習を抑制）。
    epochs, lr, batch_size : 学習設定。
    """

    def __init__(
        self,
        categorical_cardinalities: dict | None = None,
        n_numeric: int = 0,
        hidden_dims=(64, 32),
        epochs: int = 20,
        lr: float = 1e-3,
        batch_size: int = 256,
        seed: int = 100,
    ) -> None:
        self._cat_cards = categorical_cardinalities or {}
        self._n_numeric = n_numeric
        self._hidden_dims = tuple(hidden_dims)
        self._epochs = epochs
        self._lr = lr
        self._batch_size = batch_size
        self._seed = seed
        self._net = None

    def _build_net(self):
        import torch
        from torch import nn

        torch.manual_seed(self._seed)
        cat_indices = sorted(self._cat_cards)
        embeddings = nn.ModuleList(
            [nn.Embedding(self._cat_cards[i], _embedding_dim(self._cat_cards[i])) for i in cat_indices]
        )
        emb_out = sum(_embedding_dim(self._cat_cards[i]) for i in cat_indices)
        in_dim = emb_out + self._n_numeric

        layers: list = []
        prev = in_dim
        for h in self._hidden_dims:
            layers += [nn.Linear(prev, h), nn.ReLU(), nn.Dropout(0.2)]
            prev = h
        layers += [nn.Linear(prev, 1)]

        class _Net(nn.Module):
            def __init__(self, embs, mlp, cat_idx, num_idx):
                super().__init__()
                self.embs = embs
                self.mlp = nn.Sequential(*mlp)
                self.cat_idx = cat_idx
                self.num_idx = num_idx

            def forward(self, x):
                parts = []
                for k, ci in enumerate(self.cat_idx):
                    parts.append(self.embs[k](x[:, ci].long()))
                if self.num_idx:
                    parts.append(x[:, self.num_idx])
                h = torch.cat(parts, dim=1) if parts else x
                return self.mlp(h).squeeze(-1)

        num_idx = [i for i in range(in_dim_total(self._cat_cards, self._n_numeric)) if i not in cat_indices]
        return _Net(embeddings, layers, cat_indices, num_idx)

    def fit(self, x, y) -> "NnWinModel":
        import torch
        from torch import nn

        x_t = torch.as_tensor(np.asarray(x, dtype=np.float32))
        y_t = torch.as_tensor(np.asarray(y, dtype=np.float32))
        self._net = self._build_net()
        opt = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        loss_fn = nn.BCEWithLogitsLoss()
        n = len(x_t)
        for _ in range(self._epochs):
            perm = torch.randperm(n)
            for start in range(0, n, self._batch_size):
                idx = perm[start : start + self._batch_size]
                opt.zero_grad()
                logits = self._net(x_t[idx])
                loss = loss_fn(logits, y_t[idx])
                loss.backward()
                opt.step()
        return self

    def predict_proba(self, x) -> np.ndarray:
        import torch

        if self._net is None:
            raise RuntimeError("fit を先に呼んでください。")
        self._net.eval()
        with torch.no_grad():
            logits = self._net(torch.as_tensor(np.asarray(x, dtype=np.float32)))
            p = torch.sigmoid(logits).numpy()
        return np.column_stack([1.0 - p, p])


def _embedding_dim(cardinality: int) -> int:
    """カテゴリ数に応じた埋め込み次元（経験則: min(50, (n+1)//2)）。"""
    return min(50, (cardinality + 1) // 2)


def in_dim_total(cat_cards: dict, n_numeric: int) -> int:
    """入力テンソルの列数（カテゴリ列 + 数値列）。列配置の整合に用いる。"""
    return len(cat_cards) + n_numeric
