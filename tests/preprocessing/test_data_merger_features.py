"""DataMerger の新特徴量メソッドのユニットテスト。

DataMerger のヘルパーメソッドを直接インスタンス化して検証する。
ファイル I/O やフル processor を使わず、合成 DataFrame を注入する。
"""

from __future__ import annotations

import sys
import types
import numpy as np
import pandas as pd
import pytest

# ── tqdm スタブ（CI 環境で未インストールの場合に備えて）──
_tqdm_stub = types.ModuleType("tqdm")
_tqdm_auto_stub = types.ModuleType("tqdm.auto")
_tqdm_auto_stub.tqdm = lambda x, **kw: x  # no-op iterator wrapper
_tqdm_stub.auto = _tqdm_auto_stub
sys.modules.setdefault("tqdm", _tqdm_stub)
sys.modules.setdefault("tqdm.auto", _tqdm_auto_stub)


# ──────────────────────────────────────────
# DataMerger の軽量スタブ（ファイル I/O なし）
# ──────────────────────────────────────────

def _make_merger(results_df: pd.DataFrame, peds_df: pd.DataFrame | None = None) -> "DataMerger":
    """テスト用 DataMerger: __init__ をバイパスして属性を直接セット。"""
    from src.preprocessing._data_merger import DataMerger
    from src.constants._feature_cols import N_RACES_LIST

    obj = object.__new__(DataMerger)
    obj._results = results_df
    obj._race_info = pd.DataFrame()
    obj._horse_results = pd.DataFrame()
    obj._horse_info = pd.DataFrame()
    obj._peds = peds_df if peds_df is not None else pd.DataFrame()
    obj._target_cols = ["着順"]
    obj._group_cols = []
    obj._merged_data = pd.DataFrame()
    obj._separated_results_dict = {}
    obj._separated_horse_results_dict = {}
    obj._separated_hr_with_sire_dict = {}
    return obj


# ──────────────────────────────────────────
# テストデータファクトリ
# ──────────────────────────────────────────

def _horse_results_df():
    """horse_id を index とする過去成績 DataFrame。"""
    return pd.DataFrame(
        {
            "horse_id": [1, 1, 1, 2, 2, 3],
            "date": pd.to_datetime(
                ["2023-01-01", "2023-02-01", "2023-03-01",
                 "2023-01-15", "2023-02-15", "2023-01-20"]
            ),
            "着順": [1, 2, 1, 3, 5, 2],
            "頭数": [12, 12, 16, 10, 10, 8],
            "ペース": ["逃", "先", "差", "追", "逃", "先"],
            "race_type": ["芝", "芝", "ダート", "芝", "ダート", "芝"],
            "course_len": [16, 16, 18, 20, 16, 16],
        }
    ).set_index("horse_id")


def _results_df_with_jockey():
    """race_id を index とするレース結果 DataFrame（騎手 / 調教師 付き）。"""
    return pd.DataFrame(
        {
            "race_id": ["r01", "r01", "r02", "r02"],
            "horse_id": [1, 2, 1, 3],
            "jockey_id": ["J1", "J2", "J1", "J3"],
            "trainer_id": ["T1", "T2", "T1", "T3"],
            "rank": [1, 0, 0, 1],
            "着順": [1, 4, 5, 1],
            "n_horses": [8, 8, 8, 8],
            "date": pd.to_datetime(["2023-04-01", "2023-04-01", "2023-05-01", "2023-05-01"]),
        }
    ).set_index("race_id")


# ──────────────────────────────────────────
# §2i: _summarize (multi-stat)
# ──────────────────────────────────────────

class TestSummarize:
    def test_columns_contain_all_stats(self):
        from src.preprocessing._data_merger import DataMerger
        from src.constants._feature_cols import AGG_STATS

        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df()
        result = m._summarize(hr, ["着順"])
        for stat in AGG_STATS:
            assert f"着順_{stat}" in result.columns, f"Missing 着順_{stat}"

    def test_mean_value_correct(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df()
        result = m._summarize(hr, ["着順"])
        # horse 1: 着順 = [1, 2, 1] → mean = 4/3
        assert result.loc[1, "着順_mean"] == pytest.approx(4 / 3)

    def test_index_is_horse_id(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df()
        result = m._summarize(hr, ["着順"])
        assert result.index.name == "horse_id"


def _horse_results_rich():
    """新集計対象列（Phase 1）を含む過去成績 DataFrame。"""
    hr = _horse_results_df()
    hr["time_seconds"] = [80.0, 81.0, 95.0, 100.0, 82.0, 79.0]
    hr["上り"] = [35.0, 34.5, 38.0, 36.0, 35.5, 34.0]
    hr["first_to_rank"] = [0, 1, -1, 2, 0, 1]
    hr["final_to_rank"] = [1, 0, 0, 1, -1, 0]
    hr["first_to_final"] = [-1, 1, -1, 1, 1, 1]
    hr["オッズ"] = [2.5, 3.0, 5.0, 10.0, 4.0, 1.5]
    hr["人気"] = [1.0, 2.0, 3.0, 4.0, 2.0, 1.0]
    return hr


# ──────────────────────────────────────────
# Phase 1: 集計対象列の拡張（time_seconds/上り/展開/過去オッズ/人気）
# ──────────────────────────────────────────

class TestExpandedTargetAggregation:
    def test_new_target_aggregates_generated(self):
        from src.constants._feature_cols import AGG_TARGET_COLS

        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_rich()
        result = m._summarize(hr, AGG_TARGET_COLS)
        for col in ["time_seconds", "上り", "オッズ", "人気",
                    "first_to_rank", "final_to_rank", "first_to_final"]:
            assert f"{col}_mean" in result.columns, f"Missing {col}_mean"

    def test_odds_mean_correct(self):
        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_rich()
        result = m._summarize(hr, ["オッズ"])
        # horse 1 オッズ = [2.5, 3.0, 5.0] → mean = 10.5/3 = 3.5
        assert result.loc[1, "オッズ_mean"] == pytest.approx(3.5)

    def test_nan_excluded_from_mean(self):
        """to_numeric 由来の NaN は集計（mean）から除外される。"""
        m = _make_merger(_results_df_with_jockey())
        hr = pd.DataFrame(
            {"オッズ": [2.0, np.nan, 4.0]},
            index=pd.Index([1, 1, 1], name="horse_id"),
        )
        result = m._summarize(hr, ["オッズ"])
        assert result.loc[1, "オッズ_mean"] == pytest.approx(3.0)  # [2.0, 4.0]


# ──────────────────────────────────────────
# Phase 1: _add_horse_career_stats（馬自身の通算成績）
# ──────────────────────────────────────────

class TestAddHorseCareerStats:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": [1, 2]},
            index=pd.Index(["r99", "r99"], name="race_id"),
        )

    def test_career_cols_added(self):
        from src.constants._feature_cols import HORSE_CAREER_FEATURE_COLS

        m = _make_merger(_results_df_with_jockey())
        out = m._add_horse_career_stats(self._results(), _horse_results_df())
        for c in HORSE_CAREER_FEATURE_COLS:
            assert c in out.columns

    def test_values_correct(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_horse_career_stats(self._results(), _horse_results_df())
        # horse 1: 着順=[1,2,1] → n=3, win=2/3, quinella=3/3, place=3/3
        h1 = out[out["horse_id"] == 1].iloc[0]
        assert h1["n_career_starts"] == 3
        assert h1["career_win_rate"] == pytest.approx(2 / 3)
        assert h1["career_quinella_rate"] == pytest.approx(1.0)
        assert h1["career_place_rate"] == pytest.approx(1.0)
        # horse 2: 着順=[3,5] → n=2, win=0, quinella=0, place=1/2
        h2 = out[out["horse_id"] == 2].iloc[0]
        assert h2["n_career_starts"] == 2
        assert h2["career_win_rate"] == pytest.approx(0.0)
        assert h2["career_quinella_rate"] == pytest.approx(0.0)
        assert h2["career_place_rate"] == pytest.approx(0.5)

    def test_skips_when_rank_absent(self):
        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df().drop(columns=["着順"])
        out = m._add_horse_career_stats(self._results(), hr)
        assert "n_career_starts" not in out.columns

    def test_no_future_leak_via_prefiltered_hr(self):
        """horse_results は事前に date<当日 で絞られる前提。空なら NaN。"""
        m = _make_merger(_results_df_with_jockey())
        empty_hr = _horse_results_df().iloc[0:0]
        out = m._add_horse_career_stats(self._results(), empty_hr)
        assert out["n_career_starts"].isna().all()


# ──────────────────────────────────────────
# Phase 3: _speed_index_cutoff（基準タイムの train/test 境界）
# ──────────────────────────────────────────

class TestSpeedIndexCutoff:
    def _merger_with_dates(self, dates):
        m = _make_merger(_results_df_with_jockey())
        n = len(dates)
        m._results = pd.DataFrame(
            {"horse_id": range(n), "date": pd.to_datetime(dates)},
            index=pd.Index([f"r{i:02d}" for i in range(n)], name="race_id"),
        )
        m._speed_index_test_size = 0.2
        return m

    def test_cutoff_at_1_minus_test_size_boundary(self):
        # 10 レース、test_size=0.2 → boundary index round(10*0.8)=8 → 9番目(2020-01-09)
        dates = [f"2020-01-{d:02d}" for d in range(1, 11)]
        m = self._merger_with_dates(dates)
        cutoff = m._speed_index_cutoff()
        assert cutoff == pd.Timestamp("2020-01-09")

    def test_cutoff_none_when_no_date(self):
        m = _make_merger(_results_df_with_jockey())
        m._results = pd.DataFrame({"horse_id": [1]}, index=pd.Index(["r"], name="race_id"))
        m._speed_index_test_size = 0.2
        assert m._speed_index_cutoff() is None


# ──────────────────────────────────────────
# Phase 8: _add_prev_race_stats（前走単独）
# ──────────────────────────────────────────

class TestAddPrevRaceStats:
    def _hr(self):
        return pd.DataFrame(
            {
                "着順": [1, 3, 5], "着差": [0.0, 0.5, 1.2],
                "final_corner": [2, 4, 8], "上り": [34.5, 35.8, 37.0],
                "speed_index": [62.0, 55.0, 48.0],
                "date": pd.to_datetime(["2023-03-01", "2023-01-01", "2022-11-01"]),
            },
            index=pd.Index([1, 1, 1], name="horse_id"),
        )

    def test_prev_is_most_recent(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame({"horse_id": [1]}, index=pd.Index(["r"], name="race_id"))
        out = m._add_prev_race_stats(res, self._hr())
        # 最新走 = 2023-03-01（着順1, 4角2, 上り34.5, speed62）
        assert out["prev_rank"].iloc[0] == 1
        assert out["prev_final_corner"].iloc[0] == 2
        assert out["prev_speed_index"].iloc[0] == pytest.approx(62.0)

    def test_all_prev_cols(self):
        from src.constants._feature_cols import PREV_RACE_FEATURE_COLS

        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame({"horse_id": [1]}, index=pd.Index(["r"], name="race_id"))
        out = m._add_prev_race_stats(res, self._hr())
        for c in PREV_RACE_FEATURE_COLS:
            assert c in out.columns

    def test_skips_when_empty(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame({"horse_id": [1]}, index=pd.Index(["r"], name="race_id"))
        out = m._add_prev_race_stats(res, self._hr().iloc[0:0])
        assert "prev_rank" not in out.columns


# ──────────────────────────────────────────
# Phase 2: _add_jockey_change（乗り替わり / テン乗り）
# ──────────────────────────────────────────

class TestAddJockeyChange:
    def _hr(self):
        # horse 1: 田中→田中（最新 田中）、horse 2: 佐藤（最新 佐藤）
        return pd.DataFrame(
            {
                "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-01-15"]),
                "騎手": ["田中", "田中", "佐藤"],
            },
            index=pd.Index([1, 1, 2], name="horse_id"),
        )

    def _res(self, jockeys):
        return pd.DataFrame(
            {"horse_id": [1, 2, 3], "jockey_name": jockeys},
            index=pd.Index(["r", "r", "r"], name="race_id"),
        )

    def test_change_and_first_ride(self):
        m = _make_merger(_results_df_with_jockey())
        # h1 新騎手 佐藤（前走田中→change=1, 佐藤は騎乗歴なし→first_ride=1）
        # h2 佐藤継続（change=0, first_ride=0）
        # h3 履歴なし（両方 NaN）
        out = m._add_jockey_change(self._res(["佐藤", "佐藤", "新人"]), self._hr())
        assert out["jockey_change"].tolist()[:2] == [1.0, 0.0]
        assert out["first_ride"].tolist()[:2] == [1.0, 0.0]
        assert pd.isna(out["jockey_change"].iloc[2])
        assert pd.isna(out["first_ride"].iloc[2])

    def test_same_jockey_no_change(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_jockey_change(self._res(["田中", "佐藤", "x"]), self._hr())
        assert out["jockey_change"].iloc[0] == 0.0
        assert out["first_ride"].iloc[0] == 0.0

    def test_jockey_name_dropped(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_jockey_change(self._res(["田中", "佐藤", "x"]), self._hr())
        assert "jockey_name" not in out.columns

    def test_apprentice_marks_normalized(self):
        m = _make_merger(_results_df_with_jockey())
        # 前走 田中、今走 ☆田中 → マーク除去後は同一 → change=0
        out = m._add_jockey_change(self._res(["☆田中", "佐藤", "x"]), self._hr())
        assert out["jockey_change"].iloc[0] == 0.0

    def test_no_history_all_nan(self):
        m = _make_merger(_results_df_with_jockey())
        empty = self._hr().iloc[0:0]
        out = m._add_jockey_change(self._res(["田中", "佐藤", "x"]), empty)
        assert out["jockey_change"].isna().all()
        assert out["first_ride"].isna().all()
        assert "jockey_name" not in out.columns

    def test_skips_when_jockey_name_absent(self):
        m = _make_merger(_results_df_with_jockey())
        res = self._res(["田中", "佐藤", "x"]).drop(columns=["jockey_name"])
        out = m._add_jockey_change(res, self._hr())
        assert "jockey_change" not in out.columns


# ──────────────────────────────────────────
# §2d: _add_pace_stats
# ──────────────────────────────────────────

class TestAddPaceStats:
    def _make_results_for_pace(self):
        return pd.DataFrame(
            {
                "horse_id": [1, 2],
                "course_len": [16, 16],
            },
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def test_pace_median_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        assert "pace_median" in out.columns

    def test_leg_type_binary_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        assert "leg_type_binary" in out.columns

    def test_pace_at_distance_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        assert "pace_at_distance" in out.columns

    def test_pace_category_map_applied(self):
        """逃=0, 先=1 → horse 1 past [逃,先,差] → median=1 (先) → leg_type=0."""
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        # horse 1: ペース = [逃(0), 先(1), 差(2)] → median=1.0 → leg_type=0
        h1_row = out[out["horse_id"] == 1]
        assert h1_row["pace_median"].iloc[0] == pytest.approx(1.0)
        assert h1_row["leg_type_binary"].iloc[0] == pytest.approx(0.0)

    def test_skips_when_pace_col_absent(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df().drop(columns=["ペース"])
        out = m._add_pace_stats(results, hr)
        assert "pace_median" not in out.columns


# ──────────────────────────────────────────
# §2e: _add_course_condition_stats
# ──────────────────────────────────────────

class TestAddCourseConditionStats:
    def _make_results_for_course(self):
        return pd.DataFrame(
            {
                "horse_id": [1, 2],
                "course_len": [16, 20],
                "race_type": ["芝", "芝"],
            },
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def test_win_rate_at_distance_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_course()
        hr = _horse_results_df()
        out = m._add_course_condition_stats(results, hr)
        assert "win_rate_at_distance" in out.columns

    def test_avg_rank_at_course_type_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_course()
        hr = _horse_results_df()
        out = m._add_course_condition_stats(results, hr)
        assert "avg_rank_at_course_type" in out.columns

    def test_win_rate_correct_for_distance(self):
        """horse 1: past races at course_len 16(芝, 着順1), 16(芝, 着順2) → win_rate = 0.5."""
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_course()
        hr = _horse_results_df()
        out = m._add_course_condition_stats(results, hr)
        h1_row = out[out["horse_id"] == 1]
        # Past races at distance 16 for horse 1: 着順=[1, 2] → is_win=[1,0] → mean=0.5
        assert h1_row["win_rate_at_distance"].iloc[0] == pytest.approx(0.5)

    def test_skips_when_rank_col_absent(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_course()
        hr = _horse_results_df().drop(columns=["着順"])
        out = m._add_course_condition_stats(results, hr)
        assert "win_rate_at_distance" not in out.columns


# ──────────────────────────────────────────
# Phase 7: 競馬場別成績 / 母父(damsire) / 着差・賞金集計
# ──────────────────────────────────────────

class TestPlaceConditionStats:
    def test_win_rate_at_place(self):
        m = _make_merger(_results_df_with_jockey())
        # 過去: 東京(05)着順[1,2], 中山(06)着順[5]。現レース 開催=5(東京・Int64)
        hr = pd.DataFrame(
            {"着順": [1, 2, 5], "頭数": [10, 10, 10], "開催": ["05", "05", "06"],
             "course_len": [16, 16, 16], "race_type": ["芝", "芝", "芝"]},
            index=pd.Index([1, 1, 1], name="horse_id"),
        )
        res = pd.DataFrame(
            {"horse_id": [1], "course_len": [16], "race_type": ["芝"],
             "開催": pd.array([5], dtype="Int64")},
            index=pd.Index(["r"], name="race_id"),
        )
        out = m._add_course_condition_stats(res, hr)
        # 東京(05)のみ: 着順[1,2] → win_rate=0.5, avg_rank=(0.1+0.2)/2=0.15
        assert out["win_rate_at_place"].iloc[0] == pytest.approx(0.5)
        assert out["avg_rank_at_place"].iloc[0] == pytest.approx(0.15)

    def test_no_place_col_skips(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame({"horse_id": [1], "course_len": [16], "race_type": ["芝"]},
                           index=pd.Index(["r"], name="race_id"))
        hr = _horse_results_df()  # 開催 列なし
        out = m._add_course_condition_stats(res, hr)
        assert "win_rate_at_place" not in out.columns


class TestDamsireStats:
    def test_damsire_win_rate(self):
        m = _make_merger(_results_df_with_jockey())
        m._peds = pd.DataFrame(
            {"peds_0": pd.Categorical(["S1"]), "peds_2": pd.Categorical(["D1"])},
            index=pd.Index([10], name="horse_id"),
        )
        phs = pd.DataFrame(
            {"着順": [1, 3, 1], "頭数": [10, 10, 10],
             "peds_2": pd.Categorical(["D1", "D1", "D2"]),
             "date": pd.to_datetime(["2023-01-01"] * 3)},
            index=pd.Index([1, 2, 3], name="horse_id"),
        )
        m._separated_hr_with_sire_dict = {pd.Timestamp("2024-01-01"): phs}
        res = pd.DataFrame({"horse_id": [10]}, index=pd.Index(["r"], name="race_id"))
        out = m._add_damsire_stats(res, pd.Timestamp("2024-01-01"))
        for c in ["damsire_win_rate", "damsire_avg_rank", "damsire_recent_win_rate"]:
            assert c in out.columns
        # D1 産駒 着順[1,3] → win 0.5
        assert out["damsire_win_rate"].iloc[0] == pytest.approx(0.5)
        assert "_ped_key" not in out.columns


class TestChakusaPrizeAggregation:
    def test_in_target_cols_and_aggregated(self):
        from src.constants._feature_cols import AGG_TARGET_COLS

        assert "着差" in AGG_TARGET_COLS and "賞金" in AGG_TARGET_COLS
        m = _make_merger(_results_df_with_jockey())
        hr = pd.DataFrame(
            {"着差": [0.0, 0.3, 1.2], "賞金": [500.0, 0.0, 100.0]},
            index=pd.Index([1, 1, 1], name="horse_id"),
        )
        s = m._summarize(hr, ["着差", "賞金"])
        assert s.loc[1, "着差_mean"] == pytest.approx(0.5)
        assert s.loc[1, "賞金_mean"] == pytest.approx(200.0)


# ──────────────────────────────────────────
# §2c: _add_jockey_trainer_stats
# ──────────────────────────────────────────

class TestAddJockeyTrainerStats:
    def test_jockey_win_rate_added(self):
        from src.preprocessing._data_merger import DataMerger

        past = _results_df_with_jockey().copy()
        past = past[past["date"] < pd.Timestamp("2023-05-01")]
        m = _make_merger(_results_df_with_jockey())
        m._results = _results_df_with_jockey()

        results = _results_df_with_jockey()[_results_df_with_jockey()["date"] == pd.Timestamp("2023-05-01")].copy()
        out = m._add_jockey_trainer_stats(results, pd.Timestamp("2023-05-01"))
        assert "jockey_win_rate" in out.columns
        assert "jockey_avg_rank" in out.columns

    def test_trainer_stats_added(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        m._results = _results_df_with_jockey()
        results = _results_df_with_jockey()[_results_df_with_jockey()["date"] == pd.Timestamp("2023-05-01")].copy()
        out = m._add_jockey_trainer_stats(results, pd.Timestamp("2023-05-01"))
        assert "trainer_win_rate" in out.columns
        assert "trainer_avg_rank" in out.columns

    def test_no_future_data_used(self):
        """target_date より未来のデータを使わないこと（リーク防止）。"""
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        m._results = _results_df_with_jockey()

        # For date 2023-04-01, past results should be empty → NaN
        results = _results_df_with_jockey()[_results_df_with_jockey()["date"] == pd.Timestamp("2023-04-01")].copy()
        out = m._add_jockey_trainer_stats(results, pd.Timestamp("2023-04-01"))
        # No past data → jockey_win_rate should be NaN
        assert out["jockey_win_rate"].isna().all()

    def test_win_rate_correct(self):
        """J1 が 2023-04-01 に 着順=1 → 2023-05-01 target では jockey_win_rate=1.0。"""
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        m._results = _results_df_with_jockey()

        results = _results_df_with_jockey()[_results_df_with_jockey()["date"] == pd.Timestamp("2023-05-01")].copy()
        out = m._add_jockey_trainer_stats(results, pd.Timestamp("2023-05-01"))
        j1_row = out[out["jockey_id"] == "J1"]
        # J1 past: 着順=1 → is_win=1 → win_rate=1.0
        assert j1_row["jockey_win_rate"].iloc[0] == pytest.approx(1.0)


# ──────────────────────────────────────────
# Phase 4: _add_race_pace_forecast（展開予測）
# ──────────────────────────────────────────

class TestAddRacePaceForecast:
    def test_front_rate_and_count(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame(
            {"horse_id": [1, 2, 3], "leg_type_binary": [0.0, 1.0, 0.0],
             "pace_median": [1.0, 2.0, 0.0]},
            index=pd.Index(["r1"] * 3, name="race_id"),
        )
        out = m._add_race_pace_forecast(res)
        assert out["race_front_count"].iloc[0] == 2.0
        assert out["race_front_rate"].iloc[0] == pytest.approx(2 / 3)

    def test_nan_leg_type_excluded_from_denominator(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame(
            {"horse_id": [1, 2, 3, 4], "leg_type_binary": [0.0, np.nan, 1.0, 0.0],
             "pace_median": [1.0, 1.5, 2.0, 0.0]},
            index=pd.Index(["r2"] * 4, name="race_id"),
        )
        out = m._add_race_pace_forecast(res)
        # 2 front / 3 known (NaN excluded) = 2/3
        assert out["race_front_rate"].iloc[0] == pytest.approx(2 / 3)

    def test_own_vs_race_pace(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame(
            {"horse_id": [1, 2, 3], "leg_type_binary": [0.0, 1.0, 0.0],
             "pace_median": [1.0, 2.0, 0.0]},
            index=pd.Index(["r1"] * 3, name="race_id"),
        )
        out = m._add_race_pace_forecast(res)
        # mean pace = 1.0 → own_vs = [0, 1, -1]
        assert out["own_vs_race_pace"].tolist() == [0.0, 1.0, -1.0]

    def test_skips_when_no_pace_cols(self):
        m = _make_merger(_results_df_with_jockey())
        res = pd.DataFrame({"horse_id": [1]}, index=pd.Index(["r"], name="race_id"))
        out = m._add_race_pace_forecast(res)
        assert "race_front_rate" not in out.columns


# ──────────────────────────────────────────
# Phase 4: _dist_band / _add_sire_distance_stats
# ──────────────────────────────────────────

class TestSireDistanceStats:
    def test_dist_band_mapping(self):
        from src.preprocessing._data_merger import DataMerger

        bands = DataMerger._dist_band(pd.Series([12, 16, 20, 24, np.nan]))
        assert bands.iloc[:4].tolist() == ["sprint", "mile", "mid", "long"]
        assert pd.isna(bands.iloc[4])  # 欠損 course_len は NaN 帯（統計にマッチせず NaN）

    def _setup(self):
        m = _make_merger(_results_df_with_jockey())
        hr = pd.DataFrame(
            {
                "着順": [1, 2, 1, 3, 2, 1], "頭数": [10] * 6,
                "course_len": [16, 16, 20, 20, 16, 16],
                "peds_0": pd.Categorical(["A", "A", "A", "A", "B", "B"]),
                "date": pd.to_datetime(["2023-01-01"] * 6),
            },
            index=pd.Index([1, 1, 2, 2, 3, 3], name="horse_id"),
        )
        m._separated_hr_with_sire_dict = {pd.Timestamp("2024-01-01"): hr}
        m._peds = pd.DataFrame(
            {"peds_0": pd.Categorical(["A", "B"])},
            index=pd.Index([10, 11], name="horse_id"),
        )
        return m

    def test_distband_cols_added(self):
        from src.constants._feature_cols import SIRE_DISTANCE_FEATURE_COLS

        m = self._setup()
        res = pd.DataFrame({"horse_id": [10, 11], "course_len": [16.0, 16.0]},
                           index=pd.Index(["r3", "r3"], name="race_id"))
        out = m._add_sire_distance_stats(res, pd.Timestamp("2024-01-01"))
        for c in SIRE_DISTANCE_FEATURE_COLS:
            assert c in out.columns

    def test_win_rate_correct_for_band(self):
        m = self._setup()
        res = pd.DataFrame({"horse_id": [10], "course_len": [16.0]},
                           index=pd.Index(["r3"], name="race_id"))
        out = m._add_sire_distance_stats(res, pd.Timestamp("2024-01-01"))
        # sireA at mile(16): 着順=[1,2] → win_rate=0.5, n=2
        assert out["sire_win_rate_distband"].iloc[0] == pytest.approx(0.5)
        assert out["sire_n_distband"].iloc[0] == 2.0

    def test_temp_keys_dropped(self):
        m = self._setup()
        res = pd.DataFrame({"horse_id": [10], "course_len": [16.0]},
                           index=pd.Index(["r3"], name="race_id"))
        out = m._add_sire_distance_stats(res, pd.Timestamp("2024-01-01"))
        assert "_sire_key" not in out.columns and "_dist_band" not in out.columns


# ──────────────────────────────────────────
# Phase 5: 馬主・生産者統計 + ライブパリティ
# ──────────────────────────────────────────

class TestOwnerBreederStats:
    def _hist(self):
        # owner O1 の過去成績（勝ち2/4）
        return pd.DataFrame(
            {
                "horse_id": ["10", "11", "12", "13"],
                "owner_id": ["O1", "O1", "O1", "O1"],
                "jockey_id": ["J1"] * 4, "trainer_id": ["T1"] * 4,
                "着順": [1, 5, 1, 3], "n_horses": [10] * 4,
                "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01", "2023-04-01"]),
            },
            index=pd.Index(["a", "b", "c", "d"], name="race_id"),
        )

    def test_training_owner_win_rate(self):
        m = _make_merger(self._hist())
        m._entity_stats_build = True
        m._horse_info = pd.DataFrame(
            {"breeder_id": ["B1"] * 4},
            index=pd.Index(["10", "11", "12", "13"], name="horse_id"),
        )
        # target_date 後方 → 過去 O1 全4走で win=2/4=0.5
        results = self._hist().iloc[3:4].copy()
        out = m._add_owner_breeder_stats(results, pd.Timestamp("2023-05-01"))
        assert "owner_win_rate" in out.columns
        # O1 past (date<2023-05-01) = 4走, 着順[1,5,1,3] → win 2/4 = 0.5
        assert out["owner_win_rate"].iloc[0] == pytest.approx(0.5)

    def test_live_loads_from_artifact(self, tmp_path):
        """ライブ(build=False): artifact をロードして非 NaN 統計をマージ（バグ是正）。"""
        from src.preprocessing._entity_stats import (
            compute_entity_stats,
            entity_stats_path,
            save_entity_stats,
        )

        # 学習側スナップショットを保存
        snap = compute_entity_stats(self._hist(), "owner_id", "owner_win_rate", "owner_avg_rank", 100)
        save_entity_stats(snap, entity_stats_path(str(tmp_path), "owner_id"))

        m = _make_merger(pd.DataFrame())
        m._entity_stats_build = False
        m._entity_stats_dir = str(tmp_path)
        m._horse_info = pd.DataFrame(
            {"owner_id": ["O1"], "breeder_id": ["B1"]},
            index=pd.Index(["50"], name="horse_id"),
        )
        # ライブ results: owner_id 無し（horse_info から補完される）
        live = pd.DataFrame({"horse_id": ["50"]}, index=pd.Index(["L"], name="race_id"))
        out = m._add_owner_breeder_stats(live, pd.Timestamp("2024-01-01"))
        assert out["owner_win_rate"].iloc[0] == pytest.approx(0.5)  # 全 NaN でない = 是正

    def test_live_missing_artifact_yields_nan_cols(self, tmp_path):
        m = _make_merger(pd.DataFrame())
        m._entity_stats_build = False
        m._entity_stats_dir = str(tmp_path)  # 空ディレクトリ
        m._horse_info = pd.DataFrame(
            {"owner_id": ["O1"], "breeder_id": ["B1"]},
            index=pd.Index(["50"], name="horse_id"),
        )
        live = pd.DataFrame({"horse_id": ["50"]}, index=pd.Index(["L"], name="race_id"))
        out = m._add_owner_breeder_stats(live, pd.Timestamp("2024-01-01"))
        # artifact 無し → 列は存在して NaN（パリティ維持）
        assert "owner_win_rate" in out.columns
        assert out["owner_win_rate"].isna().all()

    def test_jockey_trainer_live_branch(self, tmp_path):
        from src.preprocessing._entity_stats import (
            compute_entity_stats,
            entity_stats_path,
            save_entity_stats,
        )

        snap = compute_entity_stats(self._hist(), "jockey_id", "jockey_win_rate", "jockey_avg_rank", 30)
        save_entity_stats(snap, entity_stats_path(str(tmp_path), "jockey_id"))
        m = _make_merger(pd.DataFrame())
        m._entity_stats_build = False
        m._entity_stats_dir = str(tmp_path)
        m._horse_info = pd.DataFrame(columns=["owner_id", "breeder_id"])
        live = pd.DataFrame({"horse_id": ["50"], "jockey_id": ["J1"]},
                            index=pd.Index(["L"], name="race_id"))
        out = m._add_jockey_trainer_stats(live, pd.Timestamp("2024-01-01"))
        # J1 全4走 着順[1,5,1,3] → win 2/4 = 0.5
        assert out["jockey_win_rate"].iloc[0] == pytest.approx(0.5)


# ──────────────────────────────────────────
# §2j: _add_sire_stats
# ──────────────────────────────────────────

class TestAddSireStats:
    def _make_peds(self):
        return pd.DataFrame(
            {"peds_0": pd.Categorical(["sireA", "sireB", "sireA"])},
            index=pd.Index([1, 2, 3], name="horse_id"),
        )

    def _make_hr_with_sire(self, peds_df):
        hr = _horse_results_df()
        hr["peds_0"] = hr.index.map(peds_df["peds_0"])
        return hr

    def test_sire_win_rate_added(self):
        from src.preprocessing._data_merger import DataMerger

        peds = self._make_peds()
        m = _make_merger(_results_df_with_jockey(), peds)
        hr_sire = self._make_hr_with_sire(peds)
        m._separated_hr_with_sire_dict[pd.Timestamp("2023-05-01")] = hr_sire

        results = pd.DataFrame(
            {"horse_id": [1, 2]},
            index=pd.Index(["r99", "r99"], name="race_id"),
        )
        out = m._add_sire_stats(results, pd.Timestamp("2023-05-01"))
        assert "sire_win_rate" in out.columns
        assert "sire_avg_rank" in out.columns
        assert "sire_recent_win_rate" in out.columns

    def test_sire_win_rate_correct(self):
        """sireA 産駒: horse1(着順=[1,2,1]) + horse3(着順=[2]) → is_win=[1,0,1,0] → 0.5。"""
        from src.preprocessing._data_merger import DataMerger

        peds = self._make_peds()
        m = _make_merger(_results_df_with_jockey(), peds)
        hr_sire = self._make_hr_with_sire(peds)
        m._separated_hr_with_sire_dict[pd.Timestamp("2023-05-01")] = hr_sire

        results = pd.DataFrame(
            {"horse_id": [1]},
            index=pd.Index(["r99"], name="race_id"),
        )
        out = m._add_sire_stats(results, pd.Timestamp("2023-05-01"))
        # sireA: horse1 [1,2,1] + horse3 [2] → is_win=[1,0,1,0] → mean=0.5
        assert out["sire_win_rate"].iloc[0] == pytest.approx(0.5)

    def test_peds_0_not_leaked_into_results(self):
        """_add_sire_stats は peds_0 を一時列として使い、最終列には残さないこと。"""
        from src.preprocessing._data_merger import DataMerger

        peds = self._make_peds()
        m = _make_merger(_results_df_with_jockey(), peds)
        hr_sire = self._make_hr_with_sire(peds)
        m._separated_hr_with_sire_dict[pd.Timestamp("2023-05-01")] = hr_sire

        results = pd.DataFrame(
            {"horse_id": [1]},
            index=pd.Index(["r99"], name="race_id"),
        )
        out = m._add_sire_stats(results, pd.Timestamp("2023-05-01"))
        assert "peds_0" not in out.columns
        assert "_sire_key" not in out.columns
