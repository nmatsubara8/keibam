"""_backfill の取得済み判定ヘルパ _done_horse_ids の単体テスト。

保存済み horse_results テーブルは transfer で reset_index されるため index は連番になり、
horse_id は列に入る。取得済み判定を index から取ると done=連番集合になって未取得の
horse_id と交わらず、全頭を再取得し続けて無限ストールする（実際に発生した不具合）。
本テストは horse_id 列を優先し、旧形式（horse_id を index に持つ）にもフォールバックする
ことを固定する。
"""
from __future__ import annotations

import pandas as pd

from src.pipeline.commands._backfill import _done_horse_ids


def test_done_uses_horse_id_column_not_rangeindex():
    # 実際の保存形: reset_index 済み（index=連番）＋ horse_id 列（1頭に複数過去走）
    hr = pd.DataFrame(
        {"horse_id": ["2015101014", "2015101014", "2016100151"], "着順": [1, 3, 2]}
    )  # 連番 index 0,1,2
    assert _done_horse_ids(hr) == {"2015101014", "2016100151"}
    # index(連番)を誤用していないこと
    assert "0" not in _done_horse_ids(hr)


def test_done_falls_back_to_index_for_legacy_form():
    # 旧形式: horse_id を index に持ち列が無い
    hr = pd.DataFrame({"着順": [1, 2]}, index=["2015101014", "2016100151"])
    assert _done_horse_ids(hr) == {"2015101014", "2016100151"}


def test_done_empty_frame():
    assert _done_horse_ids(pd.DataFrame()) == set()
    assert _done_horse_ids(None) == set()


def test_ids_minus_done_shrinks():
    # 結合的健全性: results の horse_id から done を引くと取得済みが正しく消える
    ids = {"2015101014", "2016100151", "2017100251"}
    hr = pd.DataFrame({"horse_id": ["2015101014", "2016100151"], "着順": [1, 1]})
    remaining = ids - _done_horse_ids(hr)
    assert remaining == {"2017100251"}      # 未取得の1頭だけ残る（=ストールしない）
