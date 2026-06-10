"""ResultsProcessor / HorseResultsProcessor / RaceInfoProcessor /
HorseInfoProcessor / ShutubaTableProcessor の単体テスト（ファイル I/O なし）。

pd.read_pickle をモックして各プロセッサの変換ロジックを検証する。
"""

from __future__ import annotations

import unittest.mock

import pandas as pd
import pytest

from src.constants._horse_results_cols import HorseResultsCols as HRCols
from src.constants._results_cols import ResultsCols as Cols
from src.preprocessing._horse_info_processor import HorseInfoProcessor
from src.preprocessing._horse_results_processor import HorseResultsProcessor
from src.preprocessing._race_info_processor import RaceInfoProcessor
from src.preprocessing._results_processor import ResultsProcessor
from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor


# ──────────────────────────────────────────────────────
# 共通ヘルパー
# ──────────────────────────────────────────────────────


def _make_processor(cls, raw_df: pd.DataFrame):
    """pd.read_pickle をモックしてプロセッサを生成する（ファイル I/O なし）。"""
    with unittest.mock.patch("pandas.read_pickle", return_value=raw_df):
        return cls("dummy.pkl")


# ──────────────────────────────────────────────────────
# ResultsProcessor
# ──────────────────────────────────────────────────────

_RESULTS_COLS = [
    "race_id", Cols.RANK, Cols.WAKUBAN, Cols.UMABAN, Cols.KINRYO,
    Cols.TANSHO_ODDS, "horse_id", "jockey_id", "trainer_id", "owner_id",
    Cols.SEX_AGE, Cols.WEIGHT_AND_DIFF,
    "course_len", "date", "weather", "race_type", "ground_state",
    "around", "race_class",
]


def _make_results_raw(overrides: dict | None = None, race_id: str = "202301010101") -> pd.DataFrame:
    row = {
        "race_id": race_id,
        Cols.RANK: "1",
        Cols.WAKUBAN: 3,
        Cols.UMABAN: 5,
        Cols.KINRYO: 57.0,
        Cols.TANSHO_ODDS: "3.5",
        "horse_id": "H001",
        "jockey_id": "J001",
        "trainer_id": "T001",
        "owner_id": "O001",
        Cols.SEX_AGE: "牡4",
        Cols.WEIGHT_AND_DIFF: "480(+2)",
        "course_len": 1600.0,
        "date": "2023-01-01",
        "weather": "晴",
        "race_type": "芝",
        "ground_state": "良",
        "around": 0,
        "race_class": 1,
    }
    if overrides:
        row.update(overrides)
    df = pd.DataFrame([row])
    # raw_data は race_id をインデックスとして持つ（スクレイプ生データの形式）
    df.index = pd.Index([race_id], name="race_id")
    return df


def test_results_processor_index_is_race_id():
    rp = _make_processor(ResultsProcessor, _make_results_raw())
    assert rp.preprocessed_data.index.name == "race_id"


def test_results_processor_sex_extracted():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.SEX_AGE: "牝3"}))
    assert rp.preprocessed_data["性"].iloc[0] == "牝"


def test_results_processor_age_extracted():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.SEX_AGE: "牡5"}))
    assert rp.preprocessed_data["年齢"].iloc[0] == 5


def test_results_processor_weight_extracted():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.WEIGHT_AND_DIFF: "462(+4)"}))
    assert rp.preprocessed_data["体重"].iloc[0] == 462


def test_results_processor_weight_diff_extracted():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.WEIGHT_AND_DIFF: "462(+4)"}))
    assert rp.preprocessed_data["体重変化"].iloc[0] == 4


def test_results_processor_non_numeric_rank_dropped():
    raw = _make_results_raw()
    raw2 = _make_results_raw({Cols.RANK: "除"})
    df = pd.concat([raw, raw2], ignore_index=True)
    rp = _make_processor(ResultsProcessor, df)
    assert len(rp.preprocessed_data) == 1


def test_results_processor_rank_binary_top3():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.RANK: "2"}))
    assert rp.preprocessed_data["rank"].iloc[0] == 1


def test_results_processor_rank_binary_not_top3():
    rp = _make_processor(ResultsProcessor, _make_results_raw({Cols.RANK: "5"}))
    assert rp.preprocessed_data["rank"].iloc[0] == 0


def test_results_processor_n_horses_per_race():
    # 同じ race_id で 2 頭分の行を作る（raw_data のインデックスが race_id）
    r1 = _make_results_raw({Cols.UMABAN: 1, "horse_id": "H001"})
    r2 = _make_results_raw({Cols.UMABAN: 2, "horse_id": "H002"})
    raw = pd.concat([r1, r2])  # 同一 race_id インデックスが 2 行
    rp = _make_processor(ResultsProcessor, raw)
    assert rp.preprocessed_data["n_horses"].iloc[0] == 2


# ──────────────────────────────────────────────────────
# HorseResultsProcessor
# ──────────────────────────────────────────────────────

_HR_BASE = {
    "horse_id": "H001",
    HRCols.RANK: "1",
    HRCols.DATE: "2023/01/15",
    HRCols.PLACE: "東京",
    HRCols.WEATHER: "晴",
    HRCols.R: 5,
    HRCols.RACE_NAME: "テスト",
    HRCols.N_HORSES: 16,
    HRCols.WAKUBAN: 3,
    HRCols.UMABAN: 5,
    HRCols.TANSHO_ODDS: 3.5,
    HRCols.POPULARITY: 2,
    HRCols.JOCKEY: "武豊",
    HRCols.KINRYO: 57.0,
    HRCols.RACE_TYPE_COURSE_LEN: "芝1600",
    HRCols.GROUND_STATE: "良",
    HRCols.TIME: "1:35.2",
    HRCols.RANK_DIFF: -0.5,
    HRCols.CORNER: "3-3-3-2",
    HRCols.PACE: "M",
    HRCols.NOBORI: 35.0,
    HRCols.WEIGHT_AND_DIFF: "480(+2)",
    HRCols.PRIZE: 5000.0,
}


def _make_hr_raw(overrides: dict | None = None) -> pd.DataFrame:
    row = dict(_HR_BASE)
    if overrides:
        row.update(overrides)
    return pd.DataFrame([row])


def test_horse_results_processor_index_is_horse_id():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw())
    assert rp.preprocessed_data.index.name == "horse_id"


def test_horse_results_processor_rank_numeric():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.RANK: "3"}))
    assert rp.preprocessed_data[HRCols.RANK].iloc[0] == 3


def test_horse_results_processor_non_numeric_rank_dropped():
    raw = pd.concat([_make_hr_raw(), _make_hr_raw({HRCols.RANK: "中止"})], ignore_index=True)
    rp = _make_processor(HorseResultsProcessor, raw)
    assert len(rp.preprocessed_data) == 1


def test_horse_results_processor_rank_diff_negative_becomes_zero():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.RANK_DIFF: -1.0}))
    assert rp.preprocessed_data[HRCols.RANK_DIFF].iloc[0] == 0


def test_horse_results_processor_corner_final_parsed():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.CORNER: "5-4-3-2"}))
    assert rp.preprocessed_data["final_corner"].iloc[0] == 2


def test_horse_results_processor_corner_first_parsed():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.CORNER: "5-4-3-2"}))
    assert rp.preprocessed_data["first_corner"].iloc[0] == 5


def test_horse_results_processor_race_type_extracted():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.RACE_TYPE_COURSE_LEN: "ダ1400"}))
    assert rp.preprocessed_data["race_type"].iloc[0] == "ダート"


def test_horse_results_processor_course_len_bucketed():
    # 1600m → 1600 // 100 = 16
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.RACE_TYPE_COURSE_LEN: "芝1600"}))
    assert rp.preprocessed_data["course_len"].iloc[0] == 16


def test_horse_results_processor_prize_nan_filled():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.PRIZE: float("nan")}))
    assert rp.preprocessed_data[HRCols.PRIZE].iloc[0] == 0


def test_horse_results_processor_time_seconds_positive():
    rp = _make_processor(HorseResultsProcessor, _make_hr_raw({HRCols.TIME: "1:35.2"}))
    assert rp.preprocessed_data["time_seconds"].iloc[0] > 0


# ──────────────────────────────────────────────────────
# RaceInfoProcessor
# ──────────────────────────────────────────────────────


def _make_race_info_raw(overrides: dict | None = None) -> pd.DataFrame:
    row = {
        "race_id": "202301010101",
        "course_len": 1600.0,
        "date": "2023年01月15日",
        "place_id": 5,
        "age": "3",
        "sex": "牡",
    }
    if overrides:
        row.update(overrides)
    return pd.DataFrame([row])


def test_race_info_processor_index_is_race_id():
    rp = _make_processor(RaceInfoProcessor, _make_race_info_raw())
    assert rp.preprocessed_data.index.name == "race_id"


def test_race_info_processor_course_len_bucketed():
    rp = _make_processor(RaceInfoProcessor, _make_race_info_raw({"course_len": 1600.0}))
    assert rp.preprocessed_data["course_len"].iloc[0] == 16


def test_race_info_processor_date_is_datetime():
    rp = _make_processor(RaceInfoProcessor, _make_race_info_raw())
    assert pd.api.types.is_datetime64_any_dtype(rp.preprocessed_data["date"])


def test_race_info_processor_place_id_to_kaisai():
    rp = _make_processor(RaceInfoProcessor, _make_race_info_raw({"place_id": 5}))
    assert rp.preprocessed_data["開催"].iloc[0] == 5


def test_race_info_processor_drops_place_id_column():
    rp = _make_processor(RaceInfoProcessor, _make_race_info_raw())
    assert "place_id" not in rp.preprocessed_data.columns


# ──────────────────────────────────────────────────────
# HorseInfoProcessor
# ──────────────────────────────────────────────────────


def _make_horse_info_raw() -> pd.DataFrame:
    from src.constants._horse_info_cols import HorseInfoCols as HICols
    return pd.DataFrame([{
        "horse_id": "H001",
        HICols.BIRTHDAY: "2019年04月15日",
        "owner_id": "O001",
        "breeder_id": "B001",
    }])


def test_horse_info_processor_index_is_horse_id():
    rp = _make_processor(HorseInfoProcessor, _make_horse_info_raw())
    assert rp.preprocessed_data.index.name == "horse_id"


def test_horse_info_processor_birthday_is_datetime():
    rp = _make_processor(HorseInfoProcessor, _make_horse_info_raw())
    assert pd.api.types.is_datetime64_any_dtype(rp.preprocessed_data["birthday"])


def test_horse_info_processor_has_owner_id():
    rp = _make_processor(HorseInfoProcessor, _make_horse_info_raw())
    assert "owner_id" in rp.preprocessed_data.columns


# ──────────────────────────────────────────────────────
# ShutubaTableProcessor
# ──────────────────────────────────────────────────────


def _make_shutuba_raw() -> pd.DataFrame:
    return pd.DataFrame([{
        "race_id": "202301010101",
        Cols.RANK: "1",
        Cols.WAKUBAN: 3,
        Cols.UMABAN: 5,
        Cols.KINRYO: 57.0,
        Cols.TANSHO_ODDS: "3.5",
        "horse_id": "H001",
        "jockey_id": "J001",
        "trainer_id": "T001",
        Cols.SEX_AGE: "牡4",
        Cols.WEIGHT_AND_DIFF: "480(+2)",
        "course_len": 1600.0,
        "date": "2023-01-15",
        "weather": "晴",
        "race_type": "芝",
        "ground_state": "良",
        "around": 0,
        "race_class": 1,
    }])


def test_shutuba_table_processor_course_len_bucketed():
    rp = _make_processor(ShutubaTableProcessor, _make_shutuba_raw())
    assert rp.preprocessed_data["course_len"].iloc[0] == 16


def test_shutuba_table_processor_no_rank_column():
    """ShutubaTableProcessor は着順を持たない（出走表データ）。"""
    rp = _make_processor(ShutubaTableProcessor, _make_shutuba_raw())
    assert Cols.RANK not in rp.preprocessed_data.columns


def test_shutuba_table_processor_date_is_datetime():
    rp = _make_processor(ShutubaTableProcessor, _make_shutuba_raw())
    assert pd.api.types.is_datetime64_any_dtype(rp.preprocessed_data["date"])
