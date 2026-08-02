"""run_voi_cmi の純部（カテゴリ帰属解決）の単体テスト。

featured 実データは無しで動く部分だけを検査する（帰属の a-priori 性・相互排他・市場/outcome除外）。
"""
from __future__ import annotations

import numpy as np

from scripts.run_voi_cmi import (
    CATEGORY_ORDER,
    _effective_rank,
    _market_or_outcome,
    _nonmissing_rate,
    _within_race_var_fraction,
    resolve_membership,
)


def test_market_and_outcome_excluded():
    assert _market_or_outcome("単勝odds") is True
    assert _market_or_outcome("impl_prob") is True
    assert _market_or_outcome("kijun_odds") is True
    assert _market_or_outcome("ninki_rank") is True
    assert _market_or_outcome("d_rank_win") is True       # outcome/leak
    assert _market_or_outcome("race_pace") is True         # 実現ペース=outcome
    assert _market_or_outcome("jrdb_chokyo_idx") is False  # 特徴は通す


def test_membership_assigns_by_keyword():
    cols = ["chokyo_idx", "pace_yosou", "mae3f_rel", "kyusha_win", "kyakushitsu_nige"]
    m, overlaps = resolve_membership(cols)
    assert "chokyo_idx" in m["調教"]
    assert "pace_yosou" in m["ペース"]
    assert "mae3f_rel" in m["ラップ"]
    assert "kyusha_win" in m["厩舎"]
    assert "kyakushitsu_nige" in m["脚質"]


def test_membership_excludes_market_columns():
    cols = ["chokyo_idx", "単勝_impl", "odds_rank", "kijun_odds"]
    m, _ = resolve_membership(cols)
    all_assigned = [c for cat in CATEGORY_ORDER for c in m[cat]]
    assert "chokyo_idx" in all_assigned
    assert "単勝_impl" not in all_assigned
    assert "odds_rank" not in all_assigned
    assert "kijun_odds" not in all_assigned


def test_membership_mutually_exclusive_first_match():
    # 'nige' はキーワード上 脚質 と（もし）他にも当たり得るが、first-match で1カテゴリのみに入る
    cols = ["senko_ratio"]  # ペース(senko_ratio) と 脚質(senko) の両方のキーワードに当たる
    m, overlaps = resolve_membership(cols)
    assigned = [cat for cat in CATEGORY_ORDER if "senko_ratio" in m[cat]]
    assert len(assigned) == 1                 # 相互排他（複数カテゴリに重複しない）
    assert assigned[0] == "ペース"             # first-match（ペースが脚質より前）
    assert "senko_ratio" in overlaps          # 重複は監査用に記録される


def test_no_hit_column_dropped():
    m, _ = resolve_membership(["totally_unrelated_feature", "elo_speed"])
    assigned = [c for cat in CATEGORY_ORDER for c in m[cat]]
    assert "totally_unrelated_feature" not in assigned
    assert "elo_speed" not in assigned


def test_empty_columns():
    m, overlaps = resolve_membership([])
    assert all(m[cat] == [] for cat in CATEGORY_ORDER)
    assert overlaps == {}


# ---- 結果不変の診断ヘルパー -------------------------------------------------------------

def test_nonmissing_rate():
    assert _nonmissing_rate([1.0, 2.0, np.nan, 4.0]) == 0.75
    assert _nonmissing_rate([np.nan, np.nan]) == 0.0
    assert _nonmissing_rate([]) == 0.0


def test_within_race_var_fraction_constant_is_zero():
    # レース内定数（course_lap_length を模す）→ 分散あり率 0
    vals = [5.0, 5.0, 5.0, 7.0, 7.0, 7.0]        # race A 全部5, race B 全部7
    rids = ["A", "A", "A", "B", "B", "B"]
    assert _within_race_var_fraction(vals, rids) == 0.0


def test_within_race_var_fraction_varying_is_one():
    vals = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    rids = ["A", "A", "A", "B", "B", "B"]
    assert _within_race_var_fraction(vals, rids) == 1.0


def test_within_race_var_fraction_mixed():
    vals = [1.0, 2.0, 5.0, 5.0]                  # race A 変動, race B 定数
    rids = ["A", "A", "B", "B"]
    assert _within_race_var_fraction(vals, rids) == 0.5


def test_within_race_var_fraction_nan_dropped():
    vals = [1.0, np.nan, 3.0, 3.0]
    rids = ["A", "A", "B", "B"]
    # race A は有限値1つ→分散判定不可(除外), race B 定数→var 0 ⇒ 0/2
    assert _within_race_var_fraction(vals, rids) == 0.0


def test_effective_rank_independent_columns_full():
    rng = np.random.default_rng(0)
    X = rng.normal(size=(500, 3))
    er = _effective_rank(X)
    assert er["n_features"] == 3
    assert er["numerical_rank"] == 3
    assert er["effective_rank"] > 2.5          # ほぼ full


def test_effective_rank_duplicate_column_drops():
    rng = np.random.default_rng(1)
    a = rng.normal(size=(500, 1))
    b = rng.normal(size=(500, 1))
    X = np.hstack([a, b, a])                     # 3列だが実質2次元（raw/z 重複を模す）
    er = _effective_rank(X)
    assert er["n_features"] == 3
    assert er["numerical_rank"] == 2            # 数値rankは2に落ちる
    assert er["effective_rank"] < 3.0
    assert er["cond"] > 1e6                      # 共線→条件数が大


def test_effective_rank_empty():
    er = _effective_rank(np.zeros((0, 4)))
    assert er["n_features"] == 4 and er["numerical_rank"] == 0
