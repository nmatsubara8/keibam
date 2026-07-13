"""ResultsProcessor の出走頭数 n_horses 算出の回帰テスト。

旧実装 ``df.index.map(df.index.value_counts())`` は、生 pickle が
RangeIndex（race_id は通常列）の形状だと各行 index が一意になり n_horses が
全馬 1 に縮退した。この縮退は featured_data 本流の _rel_rank（着順/頭数）を
壊す（§10 データ欠陥調査）。race_id ベースの実頭数で数えることを保証する。

lightgbm 非依存（preprocessing 層）なので本コンテナでも実行可能。
"""

from __future__ import annotations

import pandas as pd

from src.preprocessing._results_processor import ResultsProcessor


def _proc():
    # __init__（ファイル I/O）をバイパスしてメソッドだけ使う
    return object.__new__(ResultsProcessor)


def _raw_range_index_race_id_col():
    """スクレイプ直後の形状: RangeIndex + race_id 通常列。"""
    return pd.DataFrame(
        {
            "race_id": ["A", "A", "A", "B", "B"],
            "馬番": [1, 2, 3, 1, 2],
        }
    )


def _raw_race_id_index():
    """processor 往復後の形状: race_id がインデックス。"""
    df = _raw_range_index_race_id_col()
    return df.set_index("race_id")


class TestNHorses:
    def test_range_index_does_not_degenerate(self):
        # 旧バグの再現形状: この形状で全馬 1 に潰れていた
        proc = _proc()
        out = proc._add_n_horses(_raw_range_index_race_id_col())
        # race_id A は 3 頭 / B は 2 頭
        assert out["n_horses"].tolist() == [3, 3, 3, 2, 2]
        # 少なくとも 1 レースは 1 より大きい（縮退していない）
        assert out["n_horses"].max() > 1

    def test_race_id_as_index(self):
        proc = _proc()
        out = proc._add_n_horses(_raw_race_id_index())
        assert out["n_horses"].tolist() == [3, 3, 3, 2, 2]

    def test_single_horse_race_is_one(self):
        # 実際に 1 頭立てのレースだけは 1 で正しい（縮退との区別）
        proc = _proc()
        raw = pd.DataFrame({"race_id": ["A", "A", "C"], "馬番": [1, 2, 1]})
        out = proc._add_n_horses(raw)
        assert out["n_horses"].tolist() == [2, 2, 1]
