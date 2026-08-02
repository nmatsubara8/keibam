"""block_bootstrap_ci（開催日ブロック・ブートストラップ）＋ holm_correction の単体テスト。"""
from __future__ import annotations

import numpy as np

from src.simulation._model_compare import block_bootstrap_ci, holm_correction


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


def test_p_improve_strong_improvement_small():
    # 全ブロックが明確に負(改善)＝中心化帰無分布で観測平均以下は稀→p は小(≒下限 1/(B+1))
    vals = np.full(200, -0.5)
    blocks = np.repeat(np.arange(20), 10)
    r = block_bootstrap_ci(vals, blocks, n_boot=1000, seed=0)
    assert 0.0 < r["p_improve"] < 0.01    # +1/(B+1) 補正で厳密0にはならない


def test_p_improve_no_improvement_near_one():
    # 全ブロックが正(悪化)なら p_improve は 1
    vals = np.full(200, 0.5)
    blocks = np.repeat(np.arange(20), 10)
    r = block_bootstrap_ci(vals, blocks, n_boot=1000, seed=0)
    assert r["p_improve"] == 1.0


def test_p_improve_symmetric_noise_near_half():
    rng = np.random.default_rng(1)
    vals = rng.normal(0.0, 1.0, 4000)
    vals = vals - vals.mean()             # 標本平均を厳密に0へ（帰無中心と一致）
    blocks = np.repeat(np.arange(400), 10)
    r = block_bootstrap_ci(vals, blocks, n_boot=3000, seed=2)
    assert 0.35 < r["p_improve"] < 0.65   # 平均0 付近は概ね半々


def test_single_block_rejected():
    # 1ブロックは再標本化分布が退化して推論不能＝ValueError（最小pを「強い証拠」と誤認しない）
    import pytest
    with pytest.raises(ValueError):
        block_bootstrap_ci([-0.3, -0.2, -0.4], ["A", "A", "A"], n_boot=999)


def test_two_blocks_allowed_but_warns():
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("error")               # <30 ブロックは警告を出す
        import pytest
        with pytest.raises(UserWarning):
            block_bootstrap_ci([-0.3, -0.2, 0.1, 0.2], ["A", "A", "B", "B"], n_boot=200)


def test_p_improve_matches_manual_centered_formula():
    # 実装の p_improve が「中心化帰無 ≤ 観測平均」の式と一致することを、同一seedで再現して確認
    rng = np.random.default_rng(7)
    vals = rng.normal(-0.05, 1.0, 1000)
    blocks = np.repeat(np.arange(100), 10)
    r = block_bootstrap_ci(vals, blocks, n_boot=1500, seed=11)
    # 同一手順を手で再現（block_bootstrap_ci と同じ重み付き平均・同一seed）
    v = np.asarray(vals, float)
    uniq, inv = np.unique(np.asarray(blocks), return_inverse=True)
    nb = len(uniq)
    bsum = np.bincount(inv, weights=v, minlength=nb)
    bcnt = np.bincount(inv, minlength=nb).astype(float)
    g = np.random.default_rng(11)
    means = np.empty(1500)
    for i in range(1500):
        idx = g.integers(0, nb, nb)
        means[i] = bsum[idx].sum() / bcnt[idx].sum()
    obs = v.mean()
    expected = (1 + int(np.sum((means - obs) <= obs))) / (1500 + 1)
    assert abs(r["p_improve"] - expected) < 1e-12


def test_holm_orders_and_scales():
    pairs = [("a", 0.01), ("b", 0.04), ("c", 0.03)]
    out = holm_correction(pairs, alpha=0.05)
    # 昇順に並ぶ
    assert [o["name"] for o in out] == ["a", "c", "b"]
    # 段階的 Bonferroni: a=0.01*3, c=max(0.03, 0.03*2)=0.06, b=max(0.06,0.04*1)=0.06
    assert abs(out[0]["p_holm"] - 0.03) < 1e-12
    assert abs(out[1]["p_holm"] - 0.06) < 1e-12
    assert abs(out[2]["p_holm"] - 0.06) < 1e-12
    assert out[0]["reject"] is True and out[1]["reject"] is False


def test_holm_monotone_nondecreasing():
    pairs = [("a", 0.001), ("b", 0.002), ("c", 0.9), ("d", 0.95), ("e", 0.99)]
    out = holm_correction(pairs, alpha=0.05)
    ph = [o["p_holm"] for o in out]
    assert all(ph[i] <= ph[i + 1] + 1e-12 for i in range(len(ph) - 1))  # 単調非減少
    assert all(p <= 1.0 for p in ph)                                     # 1 でクリップ


def test_holm_clamps_at_one():
    out = holm_correction([("a", 0.5), ("b", 0.6)], alpha=0.05)
    assert all(o["p_holm"] <= 1.0 for o in out)
    assert all(o["reject"] is False for o in out)


def test_holm_family_m5_with_source_missing_padding():
    # 事前登録5カテゴリの family を維持: 検定できた4つ + SOURCE_MISSING を p=1.0 で帳簿。
    # m=5 でも 脚質(0.007×5=0.035)・調教(0.009×4=0.036) は棄却、厩舎(0.0375×3=0.1125) は非棄却。
    pairs = [("脚質", 0.007), ("調教", 0.009), ("厩舎", 0.0375), ("ラップ", 1.0), ("ペース", 1.0)]
    out = holm_correction(pairs, alpha=0.05)
    by = {o["name"]: o for o in out}
    assert by["脚質"]["reject"] is True
    assert by["調教"]["reject"] is True
    assert by["厩舎"]["reject"] is False
    assert by["ラップ"]["reject"] is False and by["ペース"]["reject"] is False
    # p=1.0 の帳簿は他カテゴリの棄却を弱めない（末尾に来るだけ）
    assert abs(by["脚質"]["p_holm"] - 0.035) < 1e-9
    assert abs(by["調教"]["p_holm"] - 0.036) < 1e-9
