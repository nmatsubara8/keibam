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


# ──────────────────────────────────────────
# _normalize_join_keys: ソース混在時の dtype 整合
# ──────────────────────────────────────────

class TestNormalizeJoinKeys:
    """DB 復元(object 文字列) と pickle(Int64/float) 混在でも merge できること。"""

    def test_results_horse_id_normalized_to_str(self):
        results = pd.DataFrame(
            {"horse_id": pd.array([1, 2, 3], dtype="Int64")},
            index=pd.Index(["r1", "r1", "r1"], name="race_id"),
        )
        m = _make_merger(results)
        m._normalize_join_keys()
        assert m._results["horse_id"].map(type).eq(str).all()
        assert m._results["horse_id"].tolist() == ["1", "2", "3"]

    def test_float_horse_id_strips_trailing_dot_zero(self):
        results = pd.DataFrame(
            {"horse_id": [1.0, 2.0, 3.0]},
            index=pd.Index(["r1", "r1", "r1"], name="race_id"),
        )
        m = _make_merger(results)
        m._normalize_join_keys()
        assert m._results["horse_id"].tolist() == ["1", "2", "3"]

    def test_mixed_source_horse_id_indexes_align(self):
        """results(object str) と peds(Int64 index) を正規化後に merge できる。"""
        results = pd.DataFrame(
            {"horse_id": ["1", "2", "3"]},  # DB 復元由来: object str
            index=pd.Index(["r1", "r1", "r1"], name="race_id"),
        )
        peds = pd.DataFrame(
            {"peds_0": ["sireA", "sireB", "sireA"]},
            index=pd.Index(pd.array([1, 2, 3], dtype="Int64"), name="horse_id"),  # pickle 由来
        )
        m = _make_merger(results, peds)
        m._normalize_join_keys()
        # peds index も str に揃い、results.horse_id と同じ dtype で merge 可能
        merged = m._results.merge(
            m._peds, left_on="horse_id", right_index=True, how="left"
        )
        assert merged["peds_0"].tolist() == ["sireA", "sireB", "sireA"]
