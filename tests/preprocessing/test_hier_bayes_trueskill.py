"""階層ベイズ TrueSkill（src/preprocessing/_hier_bayes_trueskill.py）のユニットテスト。

de-vig・市場スキル・3 段精度加重・不確実性縮小・as-of リーク無し・群縮小・merger 結合・
ライブ。EV エッジ保護の中核 hb_vs_market（=エッジ）の挙動も検証する。
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from src.constants._feature_cols import HB_FEATURE_COLS
from src.constants._feature_cols import HB_TAU_GROUP
from src.constants._feature_cols import HB_TAU_MARKET
from src.constants._feature_cols import TS_MU
from src.constants._feature_cols import TS_SIGMA
from src.preprocessing._hier_bayes_trueskill import combine_levels
from src.preprocessing._hier_bayes_trueskill import compute_hier_bayes_history
from src.preprocessing._hier_bayes_trueskill import implied_probabilities
from src.preprocessing._hier_bayes_trueskill import market_skills


# ──────────────────────────────────────────
# de-vig / 市場スキル
# ──────────────────────────────────────────


def test_implied_probabilities_sum_to_one():
    probs = implied_probabilities([2.0, 4.0, 4.0])
    assert sum(probs) == pytest.approx(1.0)
    # 低オッズほど高い implied
    assert probs[0] > probs[1]


def test_implied_probabilities_devig_removes_overround():
    # 控除率込みオッズ（合計 inv > 1）でも正規化で合計 1
    probs = implied_probabilities([1.5, 3.0, 6.0, 6.0])
    assert sum(probs) == pytest.approx(1.0)


def test_implied_probabilities_invalid():
    probs = implied_probabilities([2.0, float("nan"), 1.0, 0.0])
    assert probs[0] == probs[0]          # valid
    assert math.isnan(probs[1])          # NaN
    assert math.isnan(probs[2])          # odds <= 1
    assert math.isnan(probs[3])          # 0


def test_implied_probabilities_all_invalid():
    probs = implied_probabilities([float("nan"), 0.0])
    assert all(math.isnan(p) for p in probs)


def test_market_skills_favorite_higher():
    skills = market_skills([2.0, 5.0, 10.0])
    assert skills[0] > skills[1] > skills[2]
    # レース内中心化により平均は概ね TS_MU
    assert sum(skills) / len(skills) == pytest.approx(TS_MU, abs=1e-6)


def test_market_skills_invalid_is_nan():
    skills = market_skills([2.0, float("nan"), 4.0])
    assert math.isnan(skills[1])


# ──────────────────────────────────────────
# combine_levels（3 段精度加重）
# ──────────────────────────────────────────


def test_combine_levels_data_only():
    """市場・群が無ければ個体そのもの（縮小度 0）。"""
    post, shrink = combine_levels(30.0, 2.0, None, None)
    assert post == pytest.approx(30.0)
    assert shrink == pytest.approx(0.0)


def test_combine_levels_shrinks_toward_priors_for_coldstart():
    """出走浅い（σ 大）馬は市場・群事前へ強く寄る。"""
    # 個体 mu=20、市場=30、群=28、σ 大（コールドスタート）
    post_cold, shrink_cold = combine_levels(20.0, TS_SIGMA, 30.0, 28.0)
    # σ 小（実績豊富）だと個体に寄る
    post_exp, shrink_exp = combine_levels(20.0, 1.0, 30.0, 28.0)
    assert post_cold > post_exp          # cold は priors(>20) へ寄り高い
    assert shrink_cold > shrink_exp      # cold ほど事前依存度が高い
    assert 0.0 < shrink_exp < shrink_cold < 1.0


def test_combine_levels_market_pulls_more_than_group():
    """τ_market < τ_group なので市場の方が強く引く。"""
    post_m, _ = combine_levels(20.0, TS_SIGMA, 30.0, None)  # 市場のみ
    post_g, _ = combine_levels(20.0, TS_SIGMA, None, 30.0)  # 群のみ
    assert post_m > post_g  # 同じ事前値 30 でも市場の方が強く引く
    assert HB_TAU_MARKET < HB_TAU_GROUP


# ──────────────────────────────────────────
# compute_hier_bayes_history
# ──────────────────────────────────────────


def _make_df(rows):
    return pd.DataFrame(rows).set_index("race_id")


def _row(rid, date, hid, mu, sigma, *, odds=None, sire=None, umaban=1):
    r = {"race_id": rid, "date": date, "horse_id": hid, "馬番": umaban,
         "ts_mu": mu, "ts_sigma": sigma}
    if odds is not None:
        r["単勝"] = odds
    if sire is not None:
        r["peds_0"] = sire
    return r


def test_history_columns_and_index():
    df = _make_df([
        _row("R1", "2020-01-01", "A", 26.0, 5.0, odds=2.0, sire="S1", umaban=1),
        _row("R1", "2020-01-01", "B", 24.0, 5.0, odds=4.0, sire="S2", umaban=2),
    ])
    feats, groups = compute_hier_bayes_history(df)
    assert list(feats.columns) == list(HB_FEATURE_COLS)
    assert feats.index.equals(df.index)
    assert "__global__" in groups


def test_history_no_trueskill_returns_prior():
    """ts_mu/ts_sigma が無ければ 0（prior）を返しクラッシュしない。"""
    df = pd.DataFrame(
        {"horse_id": ["A"], "date": ["2020-01-01"], "馬番": [1]},
        index=pd.Index(["R1"], name="race_id"),
    )
    feats, groups = compute_hier_bayes_history(df)
    assert list(feats.columns) == list(HB_FEATURE_COLS)
    assert (feats == 0.0).all().all()
    assert groups == {}


def test_history_vs_market_is_edge_sign():
    """hb_vs_market = ts_mu - 市場スキル。実力 > 市場評価なら正（エッジ）。"""
    # A: 自前 mu 高い(30) が人気薄(odds 10) → 市場が過小評価 → vs_market 正
    # B: 自前 mu 低い(20) だが 1 番人気(odds 1.5) → 市場が過大評価 → vs_market 負
    df = _make_df([
        _row("R1", "2020-01-01", "A", 30.0, 3.0, odds=10.0, umaban=1),
        _row("R1", "2020-01-01", "B", 20.0, 3.0, odds=1.5, umaban=2),
    ])
    feats, _ = compute_hier_bayes_history(df)
    a = feats.loc[feats.index == "R1"].iloc[0]
    b = feats.loc[feats.index == "R1"].iloc[1]
    assert a["hb_vs_market"] > 0   # 市場より高評価（妙味）
    assert b["hb_vs_market"] < 0   # 市場より低評価（人気だが疑問）


def test_history_no_odds_vs_market_zero():
    """オッズ列が無ければ市場項なし → hb_vs_market=0、hb_skill は個体⊕群。"""
    df = _make_df([
        _row("R1", "2020-01-01", "A", 28.0, 4.0, sire="S1", umaban=1),
        _row("R1", "2020-01-01", "B", 22.0, 4.0, sire="S1", umaban=2),
    ])
    feats, _ = compute_hier_bayes_history(df)
    assert (feats["hb_vs_market"] == 0.0).all()


def test_history_group_shrinkage_across_sire():
    """同じ種牡馬の先行群が強いと、後出の同種牡馬コールドスタート馬が引き上がる。"""
    rows = []
    # S1 産駒が複数レースで高 mu を蓄積
    for i in range(1, 6):
        rows += [
            _row(f"R{i}", f"2020-01-0{i}", f"P{i}", 35.0, 2.0, sire="S1", umaban=1),
            _row(f"R{i}", f"2020-01-0{i}", f"Q{i}", 15.0, 2.0, sire="S2", umaban=2),
        ]
    # 最終レース: S1 産駒の新馬（mu=prior, σ 大）と S2 産駒の新馬
    rows += [
        _row("R9", "2020-01-09", "NEW_S1", TS_MU, TS_SIGMA, sire="S1", umaban=1),
        _row("R9", "2020-01-09", "NEW_S2", TS_MU, TS_SIGMA, sire="S2", umaban=2),
    ]
    feats, groups = compute_hier_bayes_history(_make_df(rows))
    r9 = feats.loc[feats.index == "R9"]
    # S1 産駒（強い群）の新馬は S2 産駒（弱い群）の新馬より hb_skill が高い
    assert r9.iloc[0]["hb_skill"] > r9.iloc[1]["hb_skill"]
    assert groups["S1"]["mean"] > groups["S2"]["mean"]


def test_history_as_of_group_no_leak():
    """群平均は当該レースより前の産駒のみ（最初の S1 馬は群事前を持たない＝全体平均）。"""
    rows = [
        _row("R1", "2020-01-01", "A", 40.0, TS_SIGMA, sire="S1", umaban=1),
        _row("R2", "2020-01-08", "B", TS_MU, TS_SIGMA, sire="S1", umaban=1),
    ]
    feats, _ = compute_hier_bayes_history(_make_df(rows))
    # R1 の A は S1 群が空 → 全体平均(=初期 TS_MU) へ縮小（A 自身の 40 は使わない）
    a = feats.loc[feats.index == "R1"].iloc[0]
    # R2 の B は S1 群（A の 40）を事前に持つ → prior(25) より引き上がる
    b = feats.loc[feats.index == "R2"].iloc[0]
    assert b["hb_skill"] > a["hb_skill"]


# ──────────────────────────────────────────
# merger 結合 / ライブ
# ──────────────────────────────────────────


def test_datamerger_merge_hier_bayes_adds_columns():
    from src.preprocessing._data_merger import DataMerger

    rows = []
    for r in range(1, 4):
        rows += [
            _row(f"R{r}", f"2020-01-0{r}", "A", 28.0, 4.0, odds=2.0, sire="S1", umaban=1),
            _row(f"R{r}", f"2020-01-0{r}", "B", 24.0, 4.0, odds=3.0, sire="S2", umaban=2),
        ]
    md = _make_df(rows)
    obj = object.__new__(DataMerger)
    obj._merged_data = md
    obj.horse_hier_bayes_groups = {}
    obj._merge_horse_hier_bayes()
    for col in HB_FEATURE_COLS:
        assert col in obj._merged_data.columns
    assert "__global__" in obj.horse_hier_bayes_groups


def test_shutuba_merger_live_hier_bayes(tmp_path, monkeypatch):
    import json

    from src.constants import _local_paths
    from src.preprocessing._shutuba_data_merger import ShutubaDataMerger

    groups = {"__global__": {"mean": 25.0, "count": 100}, "S1": {"mean": 33.0, "count": 20}}
    gpath = tmp_path / "hier_bayes_groups.json"
    gpath.write_text(json.dumps(groups))
    monkeypatch.setattr(_local_paths.LocalPaths, "HIER_BAYES_GROUPS_PATH", str(gpath))

    md = pd.DataFrame(
        {"horse_id": ["A", "B"], "馬番": [1, 2], "ts_mu": [25.0, 25.0],
         "ts_sigma": [TS_SIGMA, TS_SIGMA], "単勝": [2.0, 5.0], "peds_0": ["S1", "S9"]},
        index=pd.Index(["R1", "R1"], name="race_id"),
    )
    obj = object.__new__(ShutubaDataMerger)
    obj._merged_data = md
    obj._merge_horse_hier_bayes()
    out = obj._merged_data
    for col in HB_FEATURE_COLS:
        assert col in out.columns
    # A は強い種牡馬群(S1=33)＋1番人気 → B（未知群 S9=全体25・人気薄）より hb_skill 高い
    assert out.loc[out["horse_id"] == "A", "hb_skill"].iloc[0] > \
        out.loc[out["horse_id"] == "B", "hb_skill"].iloc[0]
    # A は 1 番人気 → 市場が高評価、自前 mu は平凡 → hb_vs_market は負寄り
    assert out.loc[out["horse_id"] == "A", "hb_vs_market"].iloc[0] < \
        out.loc[out["horse_id"] == "B", "hb_vs_market"].iloc[0]
