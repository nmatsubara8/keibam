"""①.5 シナリオ compile のテスト（前進安全な補正列・①不変・one-hot・共有事後）。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import FACTORS
from src.tuning._manji_factor_store import build_factor_table
from src.tuning._manji_posterior import PosteriorConfig
from src.tuning._manji_scenario import (
    SCENARIOS,
    Scenario,
    align_buckets,
    build_block_posteriors,
    build_scenario_training_data,
    compile_correction,
    time_blocks,
)


def _featured(n_per_block=120, n_blocks=6):
    """奇数馬番=回収1.5（加点方向）/ 偶数馬番=回収0.6 を通期一定にした合成 featured。

    ブロック順に日付を進め、各ブロックに奇偶両バケットを入れる。
    """
    frames = []
    for bi in range(n_blocks):
        start = pd.Timestamp("2015-01-01") + pd.Timedelta(days=bi * 400)
        for uma, win_frac in ((1, 0.5), (2, 0.2)):  # 0.5*3=1.5, 0.2*3=0.6
            nwin = int(round(n_per_block * win_frac))
            ranks = [1] * nwin + [2] * (n_per_block - nwin)
            frames.append(pd.DataFrame({
                "race_id": [f"b{bi}u{uma}r{i}" for i in range(n_per_block)],
                "horse_id": [f"h{bi}_{uma}_{i}" for i in range(n_per_block)],
                ResultsCols.UMABAN: uma,
                ResultsCols.RANK: ranks,
                ResultsCols.TANSHO_ODDS: 3.0,
                "date": pd.date_range(start, periods=n_per_block, freq="h"),
            }))
    return pd.concat(frames, ignore_index=True).set_index("race_id")


def _scn():
    return Scenario("t", factors=("umaban_parity",))


def test_time_blocks_partition_all_rows_once():
    feat = _featured()
    blocks = time_blocks(feat, n_blocks=6)
    assert len(blocks) == 6
    union = np.zeros(len(feat), dtype=int)
    for _, mask in blocks:
        union += mask.astype(int)
    assert (union == 1).all()  # 各行ちょうど1ブロック


def test_first_block_posterior_is_empty_forward_safe():
    feat = _featured()
    bp = build_block_posteriors(feat, ["umaban_parity"], n_blocks=6,
                                cfg=PosteriorConfig(min_n=30, universality_slices=1))
    # 最初のブロックは過去が無い → points 空
    assert bp[0][1] == {}
    # 後続ブロックは奇数バケットに正の点が付く
    later = bp[-1][1]
    assert "umaban_parity" in later
    assert later["umaban_parity"].get("odd", 0.0) > 0


def test_compile_correction_is_forward_safe_and_signed():
    feat = _featured()
    ft = build_factor_table(feat, ["umaban_parity"])
    aligned = align_buckets(feat, ft)
    bp = build_block_posteriors(feat, ["umaban_parity"], n_blocks=6,
                                cfg=PosteriorConfig(min_n=30, universality_slices=1))
    corr = compile_correction(aligned, bp, _scn())
    assert len(corr) == len(feat)

    parity = pd.to_numeric(feat[ResultsCols.UMABAN], errors="coerce").to_numpy() % 2
    first_mask = bp[0][0]
    # 最初のブロックは補正0（未来を覗かない）
    assert np.allclose(corr[first_mask], 0.0)
    # 後続ブロックでは奇数(=加点)>偶数(=減点)
    later_mask = bp[-1][0]
    odd_later = corr[later_mask & (parity == 1)]
    even_later = corr[later_mask & (parity == 0)]
    assert odd_later.mean() > 0 > even_later.mean()


def test_build_training_data_keeps_featured_immutable_and_adds_numeric_cols():
    feat = _featured()
    before_cols = list(feat.columns)
    ft = build_factor_table(feat, ["umaban_parity"])
    bp = build_block_posteriors(feat, ["umaban_parity"], n_blocks=6,
                                cfg=PosteriorConfig(min_n=30, universality_slices=1))
    out = build_scenario_training_data(feat, _scn(), factor_table=ft, block_posteriors=bp)

    # ① は不変
    assert list(feat.columns) == before_cols
    assert "manji_score" not in feat.columns
    # 出力は行数・index 保存、manji_score（数値）と one-hot 列を持つ
    assert len(out) == len(feat)
    assert out.index.equals(feat.index)
    assert out["manji_score"].dtype.kind == "f"
    onehot = [c for c in out.columns if c.startswith("manji_bkt_umaban_parity__")]
    assert {"manji_bkt_umaban_parity__odd", "manji_bkt_umaban_parity__even"} <= set(onehot)
    # one-hot は 0/1 の数値
    for c in onehot:
        assert set(np.unique(out[c].to_numpy())) <= {0.0, 1.0}


def test_registry_scenarios_reference_known_factors():
    for name, s in SCENARIOS.items():
        for f in s.factors:
            assert f.split("*")[0] in FACTORS, f"{name}: {f}"


def test_value_jinba_scenario_registered():
    assert "value_jinba" in SCENARIOS
    s = SCENARIOS["value_jinba"]
    assert not s.include_bucket_features  # 高カードは manji_score のみ
    assert "jockey*race_type" in s.factors


def test_high_cardinality_factor_is_not_one_hot_expanded():
    """騎手のような高カード因子は one-hot 化されず manji_score にのみ集約される。"""
    # 30 人の騎手 × 各1レースの合成 featured（>MAX_ONEHOT_CARD=24）
    n = 30
    feat = pd.DataFrame({
        "race_id": [f"r{i}" for i in range(n)],
        "horse_id": [f"h{i}" for i in range(n)],
        ResultsCols.UMABAN: 1,
        ResultsCols.RANK: 1,
        ResultsCols.TANSHO_ODDS: 3.0,
        ResultsCols.JOCKEY: [f"騎手{i}" for i in range(n)],
        "date": pd.date_range("2020-01-01", periods=n, freq="D"),
    }).set_index("race_id")
    ft = build_factor_table(feat, ["jockey"])
    bp = build_block_posteriors(feat, ["jockey"], n_blocks=3,
                                cfg=PosteriorConfig(min_n=5, universality_slices=1))
    scn = Scenario("hc", factors=("jockey",), include_bucket_features=True)
    out = build_scenario_training_data(feat, scn, factor_table=ft, block_posteriors=bp)
    assert "manji_score" in out.columns
    assert not [c for c in out.columns if c.startswith("manji_bkt_jockey__")]
