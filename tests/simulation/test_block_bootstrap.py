"""block_bootstrap_ci（開催日ブロック・ブートストラップ）の単体テスト。"""
from __future__ import annotations

import numpy as np

from src.simulation._model_compare import block_bootstrap_ci


def test_constant_values_zero_width_ci():
    r = block_bootstrap_ci([2.0] * 50, ["d1"] * 25 + ["d2"] * 25, n_boot=500)
    assert r["mean"] == 2.0 and abs(r["lo"] - 2.0) < 1e-9 and abs(r["hi"] - 2.0) < 1e-9
    assert r["n"] == 50 and r["n_blocks"] == 2


def test_mean_and_ci_brackets_true_mean():
    rng = np.random.default_rng(0)
    vals = rng.normal(-0.01, 1.0, 2000)
    blocks = np.repeat(np.arange(200), 10)          # 200 開催日×10レース
    r = block_bootstrap_ci(vals, blocks, n_boot=1000, seed=1)
    assert abs(r["mean"] - vals.mean()) < 1e-9
    assert r["lo"] < r["mean"] < r["hi"]            # CI が平均を挟む
    assert r["n_blocks"] == 200


def test_block_bootstrap_wider_than_iid_when_intra_block_correlated():
    # 同一ブロック内が完全相関（全レース同符号）だと、ブロックBootの分散は iid より大きい
    rng = np.random.default_rng(2)
    day_effect = rng.normal(0, 1.0, 100)
    vals = np.repeat(day_effect, 20)                # 100日×20レース・日内は同値（相関1）
    blocks = np.repeat(np.arange(100), 20)
    blk = block_bootstrap_ci(vals, blocks, n_boot=2000, seed=3)
    iid = block_bootstrap_ci(vals, np.arange(len(vals)), n_boot=2000, seed=3)  # 各レース=1ブロック
    assert (blk["hi"] - blk["lo"]) > (iid["hi"] - iid["lo"])


def test_empty():
    r = block_bootstrap_ci([], [], n_boot=10)
    assert r["n"] == 0 and r["n_blocks"] == 0
