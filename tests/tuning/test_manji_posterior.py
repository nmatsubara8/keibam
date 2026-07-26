"""①.5b ベイズ事後分布ストアのテスト（収縮・忘却割引・min_n・as_of前進安全）。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.tuning._manji_posterior import (
    PosteriorConfig,
    build_posterior_store,
    calibrate_points_bayes,
    default_half_lives,
    factor_posterior,
    global_sigma2,
    load_posterior_store,
    save_posterior_store,
)


def _rows(umaban, win_frac, odds, n, start="2020-01-01"):
    """馬番 umaban のバケットに n 行。win_frac の割合を着1(オッズodds)・残りを着2。"""
    n_win = int(round(n * win_frac))
    ranks = [1] * n_win + [2] * (n - n_win)
    dates = pd.date_range(start, periods=n, freq="D")
    return pd.DataFrame({
        "race_id": [f"{umaban}_{i}" for i in range(n)],
        ResultsCols.UMABAN: umaban,
        ResultsCols.RANK: ranks,
        ResultsCols.TANSHO_ODDS: odds,
        "date": dates,
    })


def _featured(*frames):
    df = pd.concat(frames, ignore_index=True)
    return df.set_index("race_id")


def test_point_sign_follows_edge():
    # 奇数馬番: 回収 1.5（>1 → 加点）／偶数馬番: 回収 0.6（<1 → 減点）
    odd = _rows(1, win_frac=0.5, odds=3.0, n=300)   # 0.5*3 = 1.5
    even = _rows(2, win_frac=0.2, odds=3.0, n=300)  # 0.2*3 = 0.6
    feat = _featured(odd, even)
    cfg = PosteriorConfig(min_n=30)
    post = factor_posterior(feat, "umaban_parity", cfg)
    assert post.loc["odd", "point"] > 0
    assert post.loc["even", "point"] < 0
    # 事後平均は 1 と標本平均の間（収縮）: 1 < post_mean(odd) < 1.5
    assert 1.0 < post.loc["odd", "post_mean"] < 1.5
    # 卍妙味度は基準100。加点側 odd は >100、減点側 even は <100。point=妙味度−100。
    assert post.loc["odd", "myoumido"] > 100 > post.loc["even", "myoumido"]
    assert abs(post.loc["odd", "point"] - (post.loc["odd", "myoumido"] - 100)) < 1e-6


def test_shrinkage_more_data_moves_point_further_from_zero():
    """同じ回収率でも n が多いほど事前(=0点)から離れる。"""
    small = _featured(_rows(1, 0.5, 3.0, 40))
    large = _featured(_rows(1, 0.5, 3.0, 400))
    cfg = PosteriorConfig(min_n=30)
    s2 = 1.0  # σ² を固定して n の効果だけを見る
    p_small = factor_posterior(small, "umaban_parity", cfg, sigma2=s2).loc["odd", "point"]
    p_large = factor_posterior(large, "umaban_parity", cfg, sigma2=s2).loc["odd", "point"]
    assert 0 < p_small < p_large


def test_min_n_gate_marks_point_nan_and_calibrate_drops_it():
    tiny = _featured(_rows(1, 0.5, 3.0, 10))  # n=10 < min_n
    cfg = PosteriorConfig(min_n=30, universality_slices=1)
    post = factor_posterior(tiny, "umaban_parity", cfg)
    assert np.isnan(post.loc["odd", "point"])
    pts = calibrate_points_bayes(tiny, ["umaban_parity"], cfg=cfg)
    assert "umaban_parity" not in pts  # 採用バケット無し


def test_forgetting_discount_weights_recent_evidence():
    """古い証拠=負け・最近=勝ちのバケットは、忘却割引で加点方向に動く。"""
    old = _rows(1, win_frac=0.0, odds=2.0, n=150, start="2010-01-01")   # 全敗 → 回収0
    recent = _rows(1, win_frac=1.0, odds=2.0, n=150, start="2023-01-01")  # 全勝 → 回収2.0
    feat = _featured(old, recent)
    cfg = PosteriorConfig(min_n=30)
    s2 = 1.0
    no_disc = factor_posterior(feat, "umaban_parity", cfg, sigma2=s2).loc["odd", "point"]
    with_disc = factor_posterior(
        feat, "umaban_parity", cfg, sigma2=s2, half_life_days=180.0
    ).loc["odd", "point"]
    # 割引なしは新旧半々で ~中立、割引ありは最近(勝ち)が支配 → より大きい
    assert with_disc > no_disc


def test_default_half_lives_targets_recency_factors():
    hl = default_half_lives(500.0)
    assert hl["recent3_form"] == 500.0
    assert "career_form" not in hl  # 全過去依拠は割引しない


def test_build_posterior_store_as_of_is_forward_safe(tmp_path):
    old = _rows(1, 0.5, 3.0, 100, start="2018-01-01")
    new = _rows(1, 0.9, 3.0, 100, start="2024-01-01")  # as_of で除外される未来
    feat = _featured(old, new)
    store = build_posterior_store(
        feat, ["umaban_parity"], cfg=PosteriorConfig(min_n=30, universality_slices=1),
        as_of="2020-01-01",
    )
    assert set(["factor", "bucket", "n", "n_eff", "post_mean", "post_var", "point"]) <= set(store.columns)
    # as_of=2020 より前は old(100件)のみ。未来の new は混入しない。
    row = store[store["bucket"] == "odd"].iloc[0]
    assert row["n"] == 100

    # 保存・読込ラウンドトリップ
    p = tmp_path / "posterior_store.pkl"
    save_posterior_store(store, str(p))
    back = load_posterior_store(str(p))
    assert back is not None and len(back) == len(store)


def test_global_sigma2_positive():
    feat = _featured(_rows(1, 0.5, 3.0, 50), _rows(2, 0.2, 4.0, 50))
    assert global_sigma2(feat) > 0


def test_informative_prior_shifts_small_sample_bucket():
    """卍の方向性事前で、小標本バケットの妙味度が中立100から事前方向へ動く。"""
    # 奇数馬番だけ・小標本（n=31, 勝ちほぼ無し→観測は妙味度低め）
    df = _featured(_rows(1, win_frac=0.03, odds=3.0, n=31))
    cfg = PosteriorConfig(min_n=30)
    s2 = 1.0
    # 事前なし
    neutral = factor_posterior(df, "umaban_parity", cfg, sigma2=s2).loc["odd", "myoumido"]
    # 事前 odd に +30 妙味度（テスト用の強めオフセット）
    withpr = factor_posterior(df, "umaban_parity", cfg, sigma2=s2,
                              prior_offsets={"odd": 30.0}).loc["odd", "myoumido"]
    assert withpr > neutral  # 事前方向（上）へ動く


def test_informative_prior_overridden_by_large_data():
    """データが十分なら事前は上書きされる（実測が支配）。"""
    small = _featured(_rows(1, win_frac=0.5, odds=3.0, n=31))     # 回収1.5
    large = _featured(_rows(1, win_frac=0.5, odds=3.0, n=3000))   # 回収1.5・大標本
    cfg = PosteriorConfig(min_n=30)
    s2 = 1.0
    off = {"odd": -40.0}  # 実測(+)と逆向きの強い事前
    m_small = factor_posterior(small, "umaban_parity", cfg, sigma2=s2, prior_offsets=off).loc["odd", "myoumido"]
    m_large = factor_posterior(large, "umaban_parity", cfg, sigma2=s2, prior_offsets=off).loc["odd", "myoumido"]
    # 大標本の方が実測(妙味度>100)に近く、事前(負)の影響が小さい
    assert m_large > m_small


def test_calibrate_points_bayes_accepts_factor_priors():
    df = _featured(_rows(1, 0.5, 3.0, 200), _rows(2, 0.2, 3.0, 200))
    pts = calibrate_points_bayes(
        df, ["umaban_parity"], cfg=PosteriorConfig(min_n=30, universality_slices=1),
        factor_priors={"umaban_parity": {"odd": 5.0, "even": -5.0}},
    )
    assert "umaban_parity" in pts


def test_implied_recovery_robust_to_longshot_flukes():
    """均等払戻（recovery_mode=implied）は 1 本の大穴的中で回収率が跳ねにくい。

    奇数バケット: implied 相当（オッズ2.0で50%勝ち＝中立）＋大穴(オッズ101)を1本的中。
    flat（均等買い）は大穴で大きく加点、implied（1/oddsの賭け金）は跳ねを抑える。
    """
    base = _rows(1, win_frac=0.5, odds=2.0, n=50)              # 2.0×0.5 = 1.0（中立）
    fluke = pd.DataFrame({                                      # 大穴を1本的中
        "race_id": ["fluke"], ResultsCols.UMABAN: 1,
        ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 101.0,
        "date": [pd.Timestamp("2020-06-01")],
    })
    feat = _featured(base, fluke)
    s2 = 1.0
    flat = factor_posterior(
        feat, "umaban_parity", PosteriorConfig(min_n=30, recovery_mode="flat"), sigma2=s2
    ).loc["odd", "point"]
    implied = factor_posterior(
        feat, "umaban_parity", PosteriorConfig(min_n=30, recovery_mode="implied"), sigma2=s2
    ).loc["odd", "point"]
    assert flat > implied  # 大穴の跳ねを implied が抑える
