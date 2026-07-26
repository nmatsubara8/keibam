"""ResultsProcessor の二値ラベル生成（rank=top3 / rank_win=1着）のユニットテスト。

lightgbm 非依存（preprocessing 層）なので本コンテナでも実行可能。
"""

from __future__ import annotations

import pandas as pd

from src.constants._results_cols import ResultsCols as Cols
from src.preprocessing._results_processor import ResultsProcessor


def _proc():
    # __init__（ファイル I/O）をバイパスしてメソッドだけ使う
    return object.__new__(ResultsProcessor)


def _raw(chakujun):
    return pd.DataFrame({Cols.RANK: chakujun})


class TestRankLabels:
    def test_rank_top3_and_rank_win(self):
        proc = _proc()
        out = proc._preprocess_rank(_raw([1, 2, 3, 4, 5]))
        # rank = 着順<4（top3）
        assert out["rank"].tolist() == [1, 1, 1, 0, 0]
        # rank_win = 着順==1
        assert out["rank_win"].tolist() == [1, 0, 0, 0, 0]

    def test_win_is_subset_of_top3(self):
        proc = _proc()
        out = proc._preprocess_rank(_raw([1, 2, 3, 4]))
        # 1着は必ず top3。rank_win<=rank が常に成立
        assert (out["rank_win"] <= out["rank"]).all()

    def test_non_numeric_rank_dropped(self):
        proc = _proc()
        out = proc._preprocess_rank(_raw([1, "中止", 2]))
        # 数値化できない着順は落ちる（rank/rank_win は残った行のみ）
        assert len(out) == 2
        assert out["rank_win"].tolist() == [1, 0]
