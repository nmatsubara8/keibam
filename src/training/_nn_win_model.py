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

import logging
from typing import Any

import numpy as np

_log = logging.getLogger(__name__)


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
        arch: str = "mlp",
        dropout: float = 0.2,
        conv_channels=(32, 64),
        kernel_size: int = 3,
        pre_norm: str | None = None,
        weight_decay: float = 0.0,
    ) -> None:
        self._cat_cards = categorical_cardinalities or {}
        self._n_numeric = n_numeric
        self._hidden_dims = tuple(hidden_dims)
        # アーキテクチャ種別: "mlp"（既定）/ "cnn"（Embedding+数値ベクトルを 1D 系列とみなす Conv1d）
        self._arch = arch
        self._dropout = dropout
        self._conv_channels = tuple(conv_channels)
        self._kernel_size = kernel_size
        # concat 後の結合ベクトルに適用する正規化: "layer_norm" / "batch_norm" / None（既定）
        self._pre_norm = pre_norm
        self._epochs = epochs
        self._lr = lr
        self._batch_size = batch_size
        # Adam の L2 正則化（重み減衰）。0.0=無効（既定）。過学習抑制の探索ノブ。
        self._weight_decay = weight_decay
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
        num_idx = [i for i in range(in_dim_total(self._cat_cards, self._n_numeric)) if i not in cat_indices]

        # concat 後の正規化層（Embedding スケールと数値スケールの不揃いを補正）
        if self._pre_norm == "layer_norm":
            norm_layer: "nn.Module | None" = nn.LayerNorm(in_dim)
        elif self._pre_norm == "batch_norm":
            norm_layer = nn.BatchNorm1d(in_dim)
        else:
            norm_layer = None

        if self._arch == "cnn":
            head = _build_cnn_head(in_dim, self._conv_channels, self._kernel_size, self._dropout)
        else:
            # MLP: Linear → BatchNorm1d → ReLU → Dropout（KB shard-21）
            layers: list = []
            prev = in_dim
            for h in self._hidden_dims:
                layers += [nn.Linear(prev, h), nn.BatchNorm1d(h), nn.ReLU(), nn.Dropout(self._dropout)]
                prev = h
            layers += [nn.Linear(prev, 1)]
            head = nn.Sequential(*layers)

        is_cnn = self._arch == "cnn"

        class _Net(nn.Module):
            def __init__(self, embs, norm, head, cat_idx, num_idx, is_cnn):
                super().__init__()
                self.embs = embs
                self.norm = norm  # None または LayerNorm/BatchNorm1d
                self.head = head
                self.cat_idx = cat_idx
                self.num_idx = num_idx
                self.is_cnn = is_cnn

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
                # concat 直後に正規化（Embedding 出力と標準化済み数値のスケール差を補正）
                if self.norm is not None:
                    h = self.norm(h)
                if self.is_cnn:
                    # 結合特徴ベクトルを 1ch の 1D 系列 (B, 1, in_dim) として畳み込む
                    h = h.unsqueeze(1)
                return self.head(h).squeeze(-1)

        return _Net(embeddings, norm_layer, head, cat_indices, num_idx, is_cnn)

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

        # GPU があれば使う（無ければ CPU）。VPS/CI 等 GPU 無し環境は自動で CPU にフォールバック。
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

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

        x_full = torch.as_tensor(x_arr).to(device)
        y_full = torch.as_tensor(y_arr).to(device)
        w_full = torch.as_tensor(w_arr).to(device) if w_arr is not None else None

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

        self._net = self._build_net().to(device)
        opt = torch.optim.Adam(self._net.parameters(), lr=self._lr, weight_decay=self._weight_decay)
        scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(opt, T_max=max(self._epochs, 1))

        pw = (
            torch.tensor([self._pos_weight], dtype=torch.float32).to(device)
            if self._pos_weight is not None
            else None
        )
        # sample_weight を使う場合は per-sample 損失を取りたいので reduction='none'
        reduction = "none" if w_tr is not None else "mean"
        loss_fn = nn.BCEWithLogitsLoss(pos_weight=pw, reduction=reduction)

        best_val = float("inf")
        best_state = None
        best_epoch = -1
        epochs_no_improve = 0
        n_tr = len(x_tr)
        _log.info(
            "[NN] fit 開始: train=%d val=%d epochs=%d batch=%d lr=%g device=%s",
            n_tr,
            len(x_val) if x_val is not None else 0,
            self._epochs,
            self._batch_size,
            self._lr,
            device.type,
        )

        epoch_run = 0
        for epoch in range(self._epochs):
            epoch_run = epoch + 1
            self._net.train()  # BatchNorm/Dropout を学習モードに（KB shard-38）
            perm = torch.randperm(n_tr, device=device)
            train_loss_sum = 0.0
            train_batches = 0
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
                train_loss_sum += float(loss.detach())
                train_batches += 1
            scheduler.step()
            train_loss = train_loss_sum / max(train_batches, 1)

            # Early Stopping 判定（検証ホールドアウトがある場合のみ）
            if x_val is not None and len(x_val) > 1:
                self._net.eval()
                with torch.no_grad():
                    val_logits = self._net(x_val)
                    val_loss_fn = nn.BCEWithLogitsLoss(pos_weight=pw)
                    val_loss = float(val_loss_fn(val_logits, y_val))
                improved = best_val - val_loss > self._min_delta
                _log.info(
                    "[NN] epoch %d/%d train_loss=%.4f val_loss=%.4f%s",
                    epoch_run,
                    self._epochs,
                    train_loss,
                    val_loss,
                    " *" if improved else "",
                )
                if improved:
                    best_val = val_loss
                    best_state = {k: v.clone() for k, v in self._net.state_dict().items()}
                    best_epoch = epoch_run
                    epochs_no_improve = 0
                else:
                    epochs_no_improve += 1
                    if epochs_no_improve >= self._patience:
                        _log.info("[NN] Early Stopping（patience=%d）", self._patience)
                        break
            else:
                _log.info("[NN] epoch %d/%d train_loss=%.4f", epoch_run, self._epochs, train_loss)

        # ベスト状態を復元（早期打ち切り時の過学習回避）
        if best_state is not None:
            self._net.load_state_dict(best_state)
        _log.info(
            "[NN] fit 完了: 実行 %d epoch / best epoch=%d best_val_loss=%.4f",
            epoch_run,
            best_epoch,
            best_val,
        )
        return self

    def predict_proba(self, x) -> np.ndarray:
        if self._net is None:
            raise RuntimeError("fit を先に呼んでください。")
        import torch

        self._net.eval()  # BatchNorm/Dropout を評価モードに（KB shard-38）
        x_in = np.nan_to_num(np.asarray(x, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
        dev = next(self._net.parameters()).device      # 学習時のデバイス（cuda/cpu）に合わせる
        with torch.no_grad():
            logits = self._net(torch.as_tensor(x_in).to(dev))
            p = torch.sigmoid(logits).cpu().numpy()
        # 万一 NaN が出ても meta 学習器（NaN 不可）を壊さないよう 0.5 に丸める
        p = np.nan_to_num(p, nan=0.5, posinf=1.0, neginf=0.0)
        return np.column_stack([1.0 - p, p])


def _build_cnn_head(in_dim: int, conv_channels: tuple, kernel_size: int, dropout: float):
    """結合特徴ベクトル (B, 1, in_dim) を畳み込む 1D-CNN ヘッドを構築する。

    Conv1d(→ch) → BatchNorm1d → ReLU → Dropout を conv_channels 個積み、
    AdaptiveMaxPool1d で系列長を 1 に潰してから Linear(→1) で logit を出す。
    入力長 in_dim に依存しない構造（pad='same' 相当 + Adaptive pooling）にして
    特徴数の変化に頑健にする。
    """
    from torch import nn

    pad = kernel_size // 2
    convs: list = []
    prev_ch = 1
    for ch in conv_channels:
        convs += [
            nn.Conv1d(prev_ch, ch, kernel_size=kernel_size, padding=pad),
            nn.BatchNorm1d(ch),
            nn.ReLU(),
            nn.Dropout(dropout),
        ]
        prev_ch = ch
    convs += [nn.AdaptiveMaxPool1d(1), nn.Flatten(), nn.Linear(prev_ch, 1)]
    return nn.Sequential(*convs)


def _embedding_dim(cardinality: int) -> int:
    """カテゴリ数に応じた埋め込み次元（経験則: min(50, (n+1)//2)）。"""
    return min(50, (cardinality + 1) // 2)


def in_dim_total(cat_cards: dict, n_numeric: int) -> int:
    """入力テンソルの列数（カテゴリ列 + 数値列）。列配置の整合に用いる。"""
    return len(cat_cards) + n_numeric
