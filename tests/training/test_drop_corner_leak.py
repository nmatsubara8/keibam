"""通過（コーナー通過順）が学習・予測入力から除外されることの回帰テスト。

§10 で ResultsProcessor が過去走の脚質(first_corner)復元のため results に「通過」を
保持するようになったが、当該レースの通過は着順確定後にしか分からない post-race 情報
（リーク列）であり、かつ "7-8-3-3" のような生文字列で LightGBM の数値変換を落とす。
学習(_DROP_FOR_TRAIN/_TEST) と予測(_DROP_FOR_PREDICT) の双方で除外されることを保証する。
"""

from __future__ import annotations

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols
from src.policies._score_policy import _DROP_FOR_PREDICT, _coerce_for_predict
from src.training._data_splitter import _DROP_FOR_TEST, _DROP_FOR_TRAIN

CORNER = HorseResultsCols.CORNER  # '通過'


def test_corner_in_train_drop_lists():
    assert CORNER in _DROP_FOR_TRAIN
    assert CORNER in _DROP_FOR_TEST


def test_corner_in_predict_drop_list():
    # 学習と予測の除外列は対で同期している必要がある
    assert CORNER in _DROP_FOR_PREDICT


def test_dropping_corner_yields_numeric_frame():
    # 通過の生文字列を含む featured を drop すると数値のみになり float 化できる
    df = pd.DataFrame(
        {
            CORNER: ["7-8-3-3", "1-1-1-1", "5-4"],
            "feat_a": [0.1, 0.2, 0.3],
            "rank": [1, 0, 0],
        }
    )
    x = _coerce_for_predict(df.drop(_DROP_FOR_PREDICT, axis=1, errors="ignore"))
    assert CORNER not in x.columns
    # 残った列はすべて数値化できる（float 変換で例外を出さない）
    x.astype(float)
