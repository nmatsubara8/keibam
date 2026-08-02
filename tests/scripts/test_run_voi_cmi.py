"""run_voi_cmi の純部（カテゴリ帰属解決）の単体テスト。

featured 実データは無しで動く部分だけを検査する（帰属の a-priori 性・相互排他・市場/outcome除外）。
"""
from __future__ import annotations

from scripts.run_voi_cmi import (
    CATEGORY_ORDER,
    _market_or_outcome,
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
