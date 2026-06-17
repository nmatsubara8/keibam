"""Layer1 base学習器②: ニューラルネット（PyTorch）勝率モデル。

血統・各種 ID を Entity Embedding し、数値特徴と結合して勝率（1着/top-N）を出力する。
GBDT が苦手な「複雑な非線形関係（血統の組合せ・過去数戦パターン）」を担う。

StackingModel から `fit` / `predict_proba` を通じて base学習器として利用される
（sklearn 互換 API）。torch は遅延 import し、未インストール環境でも他モジュールの
import を壊さない（依存の隔離）。

KB 追加（§2）:
- Batch Normalization（各隠れ層後）で内部共変量シフトを抑制（KB shard-21）。
- BCEWithLogitsLoss(pos_weight) で class imbalance に対応（KB context）。
- model.train() / model.eval() の明示切替（KB shard-38）。OOF 生成で fit/predict を
  交互に呼ぶため BatchNorm/Dropout の挙動を確実に切り替える。
- CosineAnnealingLR で学習率スケジューリング（KB shard-10）。
- Early Stopping（patience/min_delta）で過学習を抑制（KB shard-09）。
- rank_threshold で学習ターゲットを 1着のみ / top-N に切替（KB shard-20）。
隠れ層の過剰追加は過学習を招くため浅め既定（KB 5.2）。
"""

from __future__ import annotations

from typing import Any

import numpy as np


class NnWinModel:
    """Entity Embedding + MLP の勝率モデル（sklearn 互換）。

    Parameters
    ----------
    categorical_cardinalities : {列インデックス: カテゴリ数}。Embedding 対象。
    n_numeric : 数値特徴量の数。
    hidden_dims : 隠れ層のユニット数（浅めを既定として過学習を抑制）。
    epochs, lr, batch_size : 学習設定。
    pos_weight : BCEWithLogitsLoss の正例重み（n_negative/n_positive）。None で無効。
    rank_threshold : 学習ターゲットを top-N 着とみなす閾値（1=1着のみ）。
        fit に渡す y が生の着順の場合に `y <= rank_threshold` で二値化する。
        既に二値化済みの y（0/1）の場合はそのまま使用される。
    patience, min_delta : Early Stopping 設定（検証 loss が min_delta 以上改善しない
        エポックが patience 回続いたら打ち切り）。
    val_ratio : 内部検証ホールドアウト割合（Early Stopping 用）。0 で無効。
    """

    def __init__(
        self,
        categorical_cardinalities: dict | None = None,
        n_numeric: int = 0,
        hidden_dims=(128,),
        epochs: int = 20,
        lr: float = 1e-3,
        batch_size: int = 256,
        seed: int = 100,
        pos_weight: float | None = None,
        rank_threshold: int = 1,
        patience: int = 10,
        min_delta: float = 1e-4,
        val_ratio: float = 0.2,
        max_train_rows: int | None = None,
    ) -> None:
        self._cat_cards = categorical_cardinalities or {}
        self._n_numeric = n_numeric
        self._hidden_dims = tuple(hidden_dims)
        self._epochs = epochs
        self._lr = lr
        self._batch_size = batch_size
        self._seed = seed
        self._pos_weight = pos_weight
        self._rank_threshold = rank_threshold
        self._patience = patience
        self._min_delta = min_delta
        self._val_ratio = val_ratio
        # メモリ・学習時間の上限。学習行数がこれを超えたら（時系列順を保って）部分標本化する。
        self._max_train_rows = max_train_rows
        self._net: Any = None

    def _build_net(self) -> "Any":  # type: ignore[return]
        import torch
        from torch import nn

        torch.manual_seed(self._seed)
        cat_indices = sorted(self._cat_cards)
        embeddings = nn.ModuleList(
            [nn.Embedding(self._cat_cards[i], _embedding_dim(self._cat_cards[i])) for i in cat_indices]
        )
        emb_out = sum(_embedding_dim(self._cat_cards[i]) for i in cat_indices)
        in_dim = emb_out + self._n_numeric

        # アーキテクチャ: Linear → BatchNorm1d → ReLU → Dropout（KB shard-21）
        layers: list = []
        prev = in_dim
        for h in self._hidden_dims:
            layers += [nn.Linear(prev, h), nn.BatchNorm1d(h), nn.ReLU(), nn.Dropout(0.2)]
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
                    # 学習/推論でカテゴリ集合がずれてコードが Embedding サイズを超える/
                    # 負になる場合に備え、有効範囲 [0, num_embeddings-1] にクランプする。
                    idx = x[:, ci].long().clamp_(0, self.embs[k].num_embeddings - 1)
                    parts.append(self.embs[k](idx))
                if self.num_idx:
                    parts.append(x[:, self.num_idx])
                h = torch.cat(parts, dim=1) if parts else x
                return self.mlp(h).squeeze(-1)

        num_idx = [i for i in range(in_dim_total(self._cat_cards, self._n_numeric)) if i not in cat_indices]
        return _Net(embeddings, layers, cat_indices, num_idx)

    def _binarize_targets(self, y: np.ndarray) -> np.ndarray:
        """rank_threshold で y を二値化する。

        y が既に {0,1} のみなら二値化済みとみなしてそのまま返す。
        生の着順（>=1 の整数）が含まれる場合は `y <= rank_threshold` で 0/1 化する。
        """
        y_arr = np.asarray(y, dtype=float)
        unique = np.unique(y_arr)
        already_binary = np.all(np.isin(unique, [0.0, 1.0]))
        if already_binary:
            return y_arr
        return (y_arr <= self._rank_threshold).astype(np.float32)

    def fit(self, x, y, sample_weight=None) -> "NnWinModel":
        import torch
        from torch import nn

        x_arr = np.asarray(x, dtype=np.float32)
        y_arr = self._binarize_targets(y).astype(np.float32)
        w_arr = np.asarray(sample_weight, dtype=np.float32) if sample_weight is not None else None

        # メモリ上限: 学習行数を時系列順を保って部分標本化（早期 val split の整合のため sort）
        if self._max_train_rows is not None and len(x_arr) > self._max_train_rows:
            rng = np.random.default_rng(self._seed)
            idx = np.sort(rng.choice(len(x_arr), self._max_train_rows, replace=False))
            x_arr, y_arr = x_arr[idx], y_arr[idx]
            if w_arr is not None:
                w_arr = w_arr[idx]

        x_full = torch.as_tensor(x_arr)
        y_full = torch.as_tensor(y_arr)
        w_full = torch.as_tensor(w_arr) if w_arr is not None else None

        # Early Stopping 用の内部検証ホールドアウト分割（時系列順は呼び出し側で担保済み）
        n = len(x_full)
        n_val = int(n * self._val_ratio) if self._val_ratio > 0 else 0
        if n_val > 0 and n - n_val > 0:
            x_tr, y_tr = x_full[: n - n_val], y_full[: n - n_val]
            x_val, y_val = x_full[n - n_val :], y_full[n - n_val :]
            w_tr = w_full[: n - n_val] if w_full is not None else None
        else:
            x_tr, y_tr, w_tr = x_full, y_full, w_full
            x_val = y_val = None

        self._net = self._build_net()
        opt = torch.optim.Adam(self._net.parameters(), lr=self._lr)
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=max(self._epochs, 1))

        pw = (
            torch.tensor([self._pos_weight], dtype=torch.float32)
            if self._pos_weight is not None
            else None
        )
        # sample_weight を使う場合は per-sample 損失を取りたいので reduction='none'
        reduction = "none" if w_tr is not None else "mean"
        loss_fn = nn.BCEWithLogitsLoss(pos_weight=pw, reduction=reduction)

        best_val = float("inf")
        best_state = None
        epochs_no_improve = 0
        n_tr = len(x_tr)

        for _ in range(self._epochs):
            self._net.train()  # BatchNorm/Dropout を学習モードに（KB shard-38）
            perm = torch.randperm(n_tr)
            for start in range(0, n_tr, self._batch_size):
                idx = perm[start : start + self._batch_size]
                # BatchNorm1d は batch サイズ 1 だと分散計算で失敗するためスキップ
                if len(idx) <= 1:
                    continue
                opt.zero_grad()
                logits = self._net(x_tr[idx])
                loss = loss_fn(logits, y_tr[idx])
                if w_tr is not None:
                    loss = (loss * w_tr[idx]).mean()
                loss.backward()
                opt.step()
            scheduler.step()

            # Early Stopping 判定（検証ホールドアウトがある場合のみ）
            if x_val is not None and len(x_val) > 1:
                self._net.eval()
                with torch.no_grad():
                    val_logits = self._net(x_val)
                    val_loss_fn = nn.BCEWithLogitsLoss(pos_weight=pw)
                    val_loss = float(val_loss_fn(val_logits, y_val))
                if best_val - val_loss > self._min_delta:
                    best_val = val_loss
                    best_state = {k: v.clone() for k, v in self._net.state_dict().items()}
                    epochs_no_improve = 0
                else:
                    epochs_no_improve += 1
                    if epochs_no_improve >= self._patience:
                        break

        # ベスト状態を復元（早期打ち切り時の過学習回避）
        if best_state is not None:
            self._net.load_state_dict(best_state)
        return self

    def predict_proba(self, x) -> np.ndarray:
        if self._net is None:
            raise RuntimeError("fit を先に呼んでください。")
        import torch

        self._net.eval()  # BatchNorm/Dropout を評価モードに（KB shard-38）
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
