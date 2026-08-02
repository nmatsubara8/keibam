"""JRDB42 in-sample 参考ハーネスの純部テスト（rolling-origin fold の leak 安全・selection 域）。"""
from __future__ import annotations

from scripts.run_jrdb42_insample_reference import rolling_folds


def test_folds_are_past_only_and_within_development():
    folds = rolling_folds(range(2015, 2027), first_eval_year=2018)
    assert folds, "fold が空"
    for tr, ey in folds:
        assert ey <= 2024                    # selection 域（2025+ を eval にしない）
        assert all(y < ey for y in tr)       # train は必ず eval より過去（leak なし）
        assert all(y >= 2015 for y in tr)    # 2015 未満は使わない
    assert min(ey for _, ey in folds) == 2018
    assert max(ey for _, ey in folds) == 2024   # 2025/2026 は fold に入らない


def test_first_eval_year_respected():
    folds = rolling_folds(range(2015, 2025), first_eval_year=2020)
    assert min(ey for _, ey in folds) == 2020
    tr0 = folds[0][0]
    assert tr0 == [2015, 2016, 2017, 2018, 2019]   # eval=2020 の train は全過去年


def test_excludes_2025_and_later_entirely():
    folds = rolling_folds([2015, 2016, 2017, 2025, 2026], first_eval_year=2016)
    for tr, ey in folds:
        assert ey <= 2024 and all(y <= 2024 for y in tr)   # 2025/2026 は train/test 双方から排除
