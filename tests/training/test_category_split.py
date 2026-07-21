"""featured_data のカテゴリ分割（_category_split）のテスト。"""

import pandas as pd

from src.constants._master import Master
from src.training._category_split import category_race_counts
from src.training._category_split import category_series
from src.training._category_split import recover_race_type
from src.training._category_split import split_featured_by_category


def _featured_with_dummies():
    """race_type ワンホット + race_id インデックスを持つ featured 風 DataFrame。

    - central_turf: race_id 20240501xxxx（場=05）× 芝、2 レース
    - local_dirt:   race_id 20244401xxxx（場=44）× ダート、1 レース
    """
    rows = []
    index = []

    def add_race(race_id, race_type, n_horses=6):
        for _ in range(n_horses):
            rows.append(
                {
                    "feat_a": 1.0,
                    "race_type__芝": 1 if race_type == Master.RACE_TYPE_TURF else 0,
                    "race_type__ダート": 1 if race_type == Master.RACE_TYPE_DIRT else 0,
                    "race_type__障害": 1 if race_type == Master.RACE_TYPE_HURDLE else 0,
                }
            )
            index.append(race_id)

    add_race("202405010101", Master.RACE_TYPE_TURF)
    add_race("202405010102", Master.RACE_TYPE_TURF)
    add_race("202444010101", Master.RACE_TYPE_DIRT)
    df = pd.DataFrame(rows)
    df.index = index
    return df


def test_recover_race_type_from_dummies():
    df = _featured_with_dummies()
    rt = recover_race_type(df)
    assert rt.iloc[0] == Master.RACE_TYPE_TURF
    assert rt.iloc[-1] == Master.RACE_TYPE_DIRT


def test_recover_race_type_from_plain_column():
    df = pd.DataFrame({"race_type": [Master.RACE_TYPE_HURDLE, Master.RACE_TYPE_TURF]})
    rt = recover_race_type(df)
    assert list(rt) == [Master.RACE_TYPE_HURDLE, Master.RACE_TYPE_TURF]


def test_category_series_assigns_expected_slugs():
    df = _featured_with_dummies()
    cats = category_series(df)
    assert set(cats.unique()) == {"central_turf", "local_dirt"}


def test_split_groups_by_category():
    df = _featured_with_dummies()
    groups = split_featured_by_category(df)
    assert set(groups) == {"central_turf", "local_dirt"}
    # central_turf は 2 レース × 6 頭 = 12 行
    assert len(groups["central_turf"]) == 12
    assert groups["central_turf"].index.nunique() == 2
    assert groups["local_dirt"].index.nunique() == 1


def test_category_race_counts():
    counts = category_race_counts(_featured_with_dummies())
    assert counts == {"central_turf": 2, "local_dirt": 1}


def test_rows_with_no_known_race_type_are_dropped():
    df = pd.DataFrame(
        {
            "feat_a": [1.0, 1.0],
            "race_type__芝": [0, 0],
            "race_type__ダート": [0, 0],
            "race_type__障害": [0, 0],
        },
        index=["202405010101", "202405010101"],
    )
    assert split_featured_by_category(df) == {}
