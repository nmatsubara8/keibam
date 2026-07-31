"""stacking split の single-class 診断（#23）の純ロジックテスト。

meta 学習器が「one class」で不透明に落ちる前に、各 split のクラス分布・期間を集計し、
base_train / meta_train が single-class なら検出できることを固定する。
"""
from __future__ import annotations

import pandas as pd

from src.training._data_splitter import single_class_splits, stacking_class_balance


def _df(ranks, dates):
    return pd.DataFrame({"rank": ranks, "date": dates},
                        index=[f"r{i}" for i in range(len(ranks))])


def test_class_balance_counts_pos_neg_nan_and_dates():
    base = _df([1, 0, 1, 0], ["2020-01-01", "2020-01-02", "2020-02-01", "2020-02-02"])
    meta = _df([1, 0, None], ["2020-03-01", "2020-03-02", "2020-03-03"])
    rep = stacking_class_balance([("base_train", base), ("meta_train", meta)], "rank")
    b, m = rep[0], rep[1]
    assert b["split"] == "base_train" and b["rows"] == 4 and b["pos"] == 2 and b["neg"] == 2
    assert b["date_range"] == "2020-01-01..2020-02-02" and b["races"] == 4
    assert m["pos"] == 1 and m["neg"] == 1 and m["nan"] == 1  # target 欠損もカウント


def test_class_balance_skips_none_and_missing_target():
    rep = stacking_class_balance(
        [("base_train", None), ("meta_train", _df([1], ["2020-01-01"]).drop(columns=["rank"]))],
        "rank",
    )
    assert rep == []  # df=None / target 欠の split は集計対象外


def test_single_class_meta_is_flagged():
    # meta_train が全 0（末尾 slice に top3 が無い＝#23 の実症状）
    base = _df([1, 0, 1, 0], ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04"])
    meta = _df([0, 0, 0], ["2020-02-01", "2020-02-02", "2020-02-03"])
    rep = stacking_class_balance([("base_train", base), ("meta_train", meta)], "rank")
    bad = single_class_splits(rep)
    assert [r["split"] for r in bad] == ["meta_train"]
    assert bad[0]["pos"] == 0 and bad[0]["neg"] == 3


def test_calib_holdout_not_guarded():
    # calib_holdout が single-class でも guarded 対象外（meta 学習に使わないため）
    base = _df([1, 0], ["2020-01-01", "2020-01-02"])
    meta = _df([1, 0], ["2020-02-01", "2020-02-02"])
    calib = _df([0, 0], ["2020-03-01", "2020-03-02"])
    rep = stacking_class_balance(
        [("base_train", base), ("meta_train", meta), ("calib_holdout", calib)], "rank")
    assert single_class_splits(rep) == []  # base/meta は両クラスあり → 停止しない


def test_both_classes_present_is_clean():
    base = _df([1, 0, 1], ["2020-01-01", "2020-01-02", "2020-01-03"])
    meta = _df([0, 1], ["2020-02-01", "2020-02-02"])
    rep = stacking_class_balance([("base_train", base), ("meta_train", meta)], "rank")
    assert single_class_splits(rep) == []
