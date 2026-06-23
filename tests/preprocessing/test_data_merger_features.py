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
            # 脚質は通過順(第1コーナー位置)から導く。horse1 は前目(1,2,2)=逃げ・先行。
            "first_corner": [1, 2, 2, 8, 9, 6],
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

    def test_pace_from_corner_position(self):
        """脚質は first_corner/頭数 から算出。horse1 は前目 → pace_median 小・leg_type=0(前)。"""
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        # horse 1: first_corner=[1,2,2] / 頭数=[12,12,16] → _pace_num=[.083,.167,.125]
        #          → median≈0.125 < 0.5 → leg_type=0（逃げ・先行）
        h1_row = out[out["horse_id"] == 1]
        assert h1_row["pace_median"].iloc[0] == pytest.approx(0.125, abs=1e-3)
        assert h1_row["leg_type_binary"].iloc[0] == pytest.approx(0.0)

    def test_pace_not_all_nan(self):
        """回帰: 旧実装(ペース列をカテゴリmap)で全NaNだったバグの再発防止。"""
        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df()
        out = m._add_pace_stats(results, hr)
        assert out["pace_median"].notna().any()

    def test_skips_when_corner_col_absent(self):
        from src.preprocessing._data_merger import DataMerger

        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_pace()
        hr = _horse_results_df().drop(columns=["first_corner"])
        out = m._add_pace_stats(results, hr)
        assert "pace_median" not in out.columns


# ──────────────────────────────────────────
# §2k: _add_growth_stats
# ──────────────────────────────────────────

class TestAddGrowthStats:
    def _make_results(self):
        return pd.DataFrame(
            {"horse_id": [1, 2]},
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def _hr_with_trend(self):
        """horse1=5走で上昇基調(古い=着順悪・新しい=着順良)、horse2=2走(履歴薄)。"""
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1, 1, 1, 2, 2],
                "date": pd.to_datetime(
                    ["2023-01-01", "2023-02-01", "2023-03-01",
                     "2023-04-01", "2023-05-01", "2023-01-15", "2023-02-15"]
                ),
                # 古い2走=着順悪(8/10,9/10)、直近3走=着順良(2/10,1/10,1/10)
                "着順": [8, 9, 2, 1, 1, 3, 5],
                "頭数": [10, 10, 10, 10, 10, 10, 10],
            }
        ).set_index("horse_id")

    def test_growth_trend_and_n_starts_added(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_growth_stats(self._make_results(), self._hr_with_trend())
        assert "growth_trend" in out.columns
        assert "n_starts" in out.columns

    def test_growth_trend_negative_for_improving_horse(self):
        """上昇基調の馬は growth_trend < 0（直近の相対着順が小さい=良い）。"""
        m = _make_merger(_results_df_with_jockey())
        out = m._add_growth_stats(self._make_results(), self._hr_with_trend())
        h1 = out[out["horse_id"] == 1]
        # 直近3走 rel=(0.2+0.1+0.1)/3=0.1333、それ以前 rel=(0.8+0.9)/2=0.85
        assert h1["growth_trend"].iloc[0] == pytest.approx(0.1333 - 0.85, abs=1e-3)
        assert h1["n_starts"].iloc[0] == 5

    def test_growth_trend_nan_when_too_few_starts(self):
        """3走以下は『それ以前』が無く growth_trend=NaN（履歴不足は欠損扱い）。"""
        m = _make_merger(_results_df_with_jockey())
        out = m._add_growth_stats(self._make_results(), self._hr_with_trend())
        h2 = out[out["horse_id"] == 2]
        assert pd.isna(h2["growth_trend"].iloc[0])
        assert h2["n_starts"].iloc[0] == 2

    def test_skips_when_rank_col_absent(self):
        m = _make_merger(_results_df_with_jockey())
        hr = self._hr_with_trend().drop(columns=["着順"])
        out = m._add_growth_stats(self._make_results(), hr)
        assert "growth_trend" not in out.columns


# ──────────────────────────────────────────
# §2m(Batch A): _add_prev_race_features
# ──────────────────────────────────────────

class TestAddPrevRaceFeatures:
    def _results(self):
        return pd.DataFrame(
            {
                "horse_id": [1, 2],
                "course_len": [20, 16],
                "斤量": [56.0, 55.0],
                "騎手": ["B", "X"],
            },
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def _hr(self):
        """horse1 は2走（前走=最新2023-03: course_len18/斤量55/騎手B）、horse2 は履歴なし。"""
        return pd.DataFrame(
            {
                "horse_id": [1, 1],
                "date": pd.to_datetime(["2023-01-01", "2023-03-01"]),
                "course_len": [16, 18],
                "斤量": [54.0, 55.0],
                "騎手": ["A", "B"],
            }
        ).set_index("horse_id")

    def test_dist_and_kinryo_delta(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_prev_race_features(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        assert h1["dist_change"] == pytest.approx(20 - 18)
        assert h1["kinryo_delta"] == pytest.approx(56.0 - 55.0)

    def test_dist_change_ratio(self):
        # horse1: 前走18 → 今回20。比 = (20-18)/18
        m = _make_merger(_results_df_with_jockey())
        out = m._add_prev_race_features(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        assert h1["dist_change_ratio"] == pytest.approx((20 - 18) / 18)
        # 履歴なしの horse2 は NaN
        h2 = out[out["horse_id"] == 2].iloc[0]
        assert pd.isna(h2["dist_change_ratio"])

    def test_jockey_change_continued_is_zero(self):
        # horse1: 今回騎手B == 前走騎手B → 0（継続）
        m = _make_merger(_results_df_with_jockey())
        out = m._add_prev_race_features(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        assert h1["jockey_change"] == pytest.approx(0.0)

    def test_no_history_is_nan(self):
        # horse2 は前走なし → 全特徴 NaN（リーク無しで安全な欠損扱い）
        m = _make_merger(_results_df_with_jockey())
        out = m._add_prev_race_features(self._results(), self._hr())
        h2 = out[out["horse_id"] == 2].iloc[0]
        assert pd.isna(h2["dist_change"])
        assert pd.isna(h2["jockey_change"])

    def test_temp_cols_dropped(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_prev_race_features(self._results(), self._hr())
        for c in ("_prev_course_len", "_prev_kinryo", "_prev_jockey"):
            assert c not in out.columns


# ──────────────────────────────────────────
# 相手強度（軽量代理）: _add_opponent_strength_stats
# ──────────────────────────────────────────

class TestAddOpponentStrengthStats:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": [1, 2]},
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def _hr(self):
        # horse1: G1/G3/未勝利 を経験。horse2: 履歴なし。
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1],
                "レース名": ["有馬記念(G1)", "ラジオNIKKEI賞(G3)", "３歳未勝利"],
            }
        ).set_index("horse_id")

    def test_grade_aggregates(self):
        m = _make_merger(self._results())
        out = m._add_opponent_strength_stats(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        assert h1["faced_grade_max"] == 5            # G1
        assert h1["faced_grade_mean"] == pytest.approx((5 + 3 + 0) / 3)
        assert h1["faced_graded_count"] == 2         # G1 + G3（>=3）

    def test_no_history_is_nan(self):
        m = _make_merger(self._results())
        out = m._add_opponent_strength_stats(self._results(), self._hr())
        h2 = out[out["horse_id"] == 2].iloc[0]
        assert pd.isna(h2["faced_grade_max"])
        assert pd.isna(h2["faced_grade_mean"])

    def test_empty_hr_noop(self):
        m = _make_merger(self._results())
        out = m._add_opponent_strength_stats(self._results(), pd.DataFrame())
        assert "faced_grade_max" not in out.columns


# ──────────────────────────────────────────
# §2n(Batch B): _add_aptitude_stats
# ──────────────────────────────────────────

class TestAddAptitudeStats:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": [1], "開催": [5]},  # race_info は整数の開催コード
            index=pd.Index(["r01"], name="race_id"),
        )

    def _hr(self):
        """horse1: 重(勝)/良(5着)@東京"05"、不良(2着)@中山"06"。"""
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1],
                "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01"]),
                "着順": [1, 5, 2],
                "頭数": [10, 10, 8],
                "馬場": ["重", "良", "不良"],
                "開催": ["05", "05", "06"],  # horse_results はゼロ詰め文字列
            }
        ).set_index("horse_id")

    def test_wet_win_rate(self):
        # 道悪=重(勝)+不良(負) → 勝率 0.5
        m = _make_merger(_results_df_with_jockey())
        out = m._add_aptitude_stats(self._results(), self._hr())
        assert out["wet_win_rate"].iloc[0] == pytest.approx(0.5)

    def test_wet_rel_rank(self):
        # 道悪の相対着順 = mean(1/10, 2/8) = 0.175
        m = _make_merger(_results_df_with_jockey())
        out = m._add_aptitude_stats(self._results(), self._hr())
        assert out["wet_rel_rank"].iloc[0] == pytest.approx((0.1 + 0.25) / 2)

    def test_place_win_rate_numeric_match(self):
        # 開催"05"(=5) の2走: 勝+負 → 0.5（文字列"05" vs 整数5 を数値化して一致）
        m = _make_merger(_results_df_with_jockey())
        out = m._add_aptitude_stats(self._results(), self._hr())
        assert out["place_win_rate"].iloc[0] == pytest.approx(0.5)

    def test_empty_horse_results_safe(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_aptitude_stats(self._results(), self._hr().iloc[:0])
        assert "wet_win_rate" not in out.columns


# ──────────────────────────────────────────
# §2l(Batch C): _add_speed_figure_stats
# ──────────────────────────────────────────

class TestAddSpeedFigureStats:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": [1]},
            index=pd.Index(["r01"], name="race_id"),
        )

    def _hr(self):
        """horse1 の過去走 speed_figure（faster=正）。best=2.0、直近5走平均は全6走中5走。"""
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1, 1, 1, 1],
                "date": pd.to_datetime(
                    ["2023-01-01", "2023-02-01", "2023-03-01",
                     "2023-04-01", "2023-05-01", "2023-06-01"]
                ),
                "speed_figure": [-1.0, 0.0, 2.0, 1.0, 0.5, 1.5],
            }
        ).set_index("horse_id")

    def test_best_is_max(self):
        m = _make_merger(_results_df_with_jockey())
        out = m._add_speed_figure_stats(self._results(), self._hr())
        assert out["speed_fig_best"].iloc[0] == pytest.approx(2.0)

    def test_mean5_recent_only(self):
        # 直近5走 = [0.0,2.0,1.0,0.5,1.5] → mean=1.0（最古の-1.0は除外）
        m = _make_merger(_results_df_with_jockey())
        out = m._add_speed_figure_stats(self._results(), self._hr())
        assert out["speed_fig_mean5"].iloc[0] == pytest.approx(1.0)

    def test_skips_when_no_speed_figure(self):
        m = _make_merger(_results_df_with_jockey())
        hr = self._hr().drop(columns=["speed_figure"])
        out = m._add_speed_figure_stats(self._results(), hr)
        assert "speed_fig_best" not in out.columns


# ──────────────────────────────────────────
# §2i: _summarize with extended target_cols (Batch B)
# ──────────────────────────────────────────

class TestSummarizeExtendedTargets:
    def test_multiple_targets_aggregated(self):
        from src.preprocessing._data_merger import DataMerger  # noqa: F401

        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df().copy()
        hr["着差"] = [0.0, 0.3, 0.0, 1.2, 2.0, 0.5]
        out = m._summarize(hr, ["着順", "着差"])
        assert "着順_mean" in out.columns
        assert "着差_mean" in out.columns

    def test_string_dtype_target_does_not_crash(self):
        # DB復元で賞金が string dtype（カンマ区切り）でも集計が落ちないこと（回帰）。
        m = _make_merger(_results_df_with_jockey())
        hr = _horse_results_df().copy()
        hr["賞金"] = pd.array(
            ["1,600.0", "0", "500", "2,000.0", "0", "300"], dtype="string"
        )
        out = m._summarize(hr, ["着順", "賞金"])
        assert "賞金_mean" in out.columns
        # "1,600.0" → 1600.0 と数値化されている
        assert out["賞金_mean"].notna().any()


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

    def test_new_distance_and_type_cols(self):
        m = _make_merger(_results_df_with_jockey())
        results = self._make_results_for_course()
        hr = _horse_results_df()
        out = m._add_course_condition_stats(results, hr)
        for c in ("avg_rank_at_distance", "n_runs_at_distance", "win_rate_at_course_type"):
            assert c in out.columns
        # horse 1: 距離16の過去走2本（着順1,2）→ 出走数2・平均相対着順=(1/12+2/12)/2
        h1 = out[out["horse_id"] == 1]
        assert h1["n_runs_at_distance"].iloc[0] == 2
        assert h1["avg_rank_at_distance"].iloc[0] == pytest.approx((1 / 12 + 2 / 12) / 2)


# ──────────────────────────────────────────
# レース種別 × 馬場別成績: _add_type_ground_stats
# ──────────────────────────────────────────

class TestAddTypeGroundStats:
    def _results(self):
        return pd.DataFrame(
            {
                "horse_id": [1],
                "race_type": ["芝"],
                "ground_state1": ["良"],  # 芝 → ground_state1 を採用
                "ground_state2": ["良"],
            },
            index=pd.Index(["r01"], name="race_id"),
        )

    def _hr(self):
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1, 1],
                "着順": [1, 4, 1, 1],
                "頭数": [10, 10, 10, 10],
                "race_type": ["芝", "芝", "芝", "ダート"],
                "馬場": ["良", "良", "重", "良"],
            }
        ).set_index("horse_id")

    def test_type_ground_match(self):
        m = _make_merger(self._results())
        out = m._add_type_ground_stats(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        # (芝,良) の過去走は 着順[1,4] のみ（重・ダートは除外）
        assert h1["n_runs_type_ground"] == 2
        assert h1["win_rate_type_ground"] == pytest.approx(0.5)
        assert h1["avg_rank_type_ground"] == pytest.approx((1 / 10 + 4 / 10) / 2)

    def test_dirt_uses_ground_state2(self):
        results = self._results()
        results["race_type"] = ["ダート"]
        results["ground_state1"] = ["不良"]
        results["ground_state2"] = ["良"]  # ダート → gs2=良
        m = _make_merger(results)
        out = m._add_type_ground_stats(results, self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        # (ダート,良) は 1本（着順1）
        assert h1["n_runs_type_ground"] == 1
        assert h1["win_rate_type_ground"] == pytest.approx(1.0)

    def test_empty_hr_noop(self):
        m = _make_merger(self._results())
        out = m._add_type_ground_stats(self._results(), pd.DataFrame())
        assert "win_rate_type_ground" not in out.columns


# ──────────────────────────────────────────
# レースクラス別成績: _add_race_class_stats
# ──────────────────────────────────────────

class TestAddRaceClassStats:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": [1], "race_class": ["2勝クラス"]},  # level 3
            index=pd.Index(["r01"], name="race_id"),
        )

    def _hr(self):
        return pd.DataFrame(
            {
                "horse_id": [1, 1, 1, 1],
                "着順": [1, 5, 3, 1],
                "頭数": [10, 10, 18, 12],
                "レース名": ["2勝クラス", "2勝クラス", "天皇賞(GⅠ)", "3歳未勝利"],
            }
        ).set_index("horse_id")

    def test_same_class_stats(self):
        m = _make_merger(self._results())
        out = m._add_race_class_stats(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        # 同格(2勝クラス) は 着順[1,5] → 勝率0.5・出走2・平均(1/10+5/10)/2
        assert h1["n_runs_same_class"] == 2
        assert h1["win_rate_same_class"] == pytest.approx(0.5)
        assert h1["avg_rank_same_class"] == pytest.approx((1 / 10 + 5 / 10) / 2)

    def test_higher_class_winrate(self):
        m = _make_merger(self._results())
        out = m._add_race_class_stats(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        # 今回以上(level>=3): 2勝(3),2勝(3),GI(9) の3本 着順[1,5,3]→is_win[1,0,0]→1/3
        assert h1["win_rate_higher_class"] == pytest.approx(1 / 3)

    def test_best_class_won(self):
        m = _make_merger(self._results())
        out = m._add_race_class_stats(self._results(), self._hr())
        h1 = out[out["horse_id"] == 1].iloc[0]
        # 勝利した過去走: 2勝クラス(level3)・3歳未勝利(level1) → 最高=3
        assert h1["best_class_won"] == 3

    def test_skips_when_no_race_class(self):
        results = self._results().drop(columns=["race_class"])
        m = _make_merger(results)
        out = m._add_race_class_stats(results, self._hr())
        assert "win_rate_same_class" not in out.columns


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
# §2j: _add_damsire_stats（母父=peds_2）
# ──────────────────────────────────────────

class TestAddDamsireStats:
    def _make_peds(self):
        # peds_0=父, peds_32=母父。母父 bmsX を horse1,3 が共有。
        return pd.DataFrame(
            {
                "peds_0": pd.Categorical(["sireA", "sireB", "sireA"]),
                "peds_32": pd.Categorical(["bmsX", "bmsY", "bmsX"]),
            },
            index=pd.Index([1, 2, 3], name="horse_id"),
        )

    def _make_hr(self, peds_df):
        hr = _horse_results_df()
        hr["peds_0"] = hr.index.map(peds_df["peds_0"])
        hr["peds_32"] = hr.index.map(peds_df["peds_32"])
        return hr

    def test_damsire_features_added_and_correct(self):
        peds = self._make_peds()
        m = _make_merger(_results_df_with_jockey(), peds)
        m._separated_hr_with_sire_dict[pd.Timestamp("2023-05-01")] = self._make_hr(peds)

        results = pd.DataFrame(
            {"horse_id": [1]}, index=pd.Index(["r99"], name="race_id")
        )
        out = m._add_damsire_stats(results, pd.Timestamp("2023-05-01"))
        for c in ("damsire_win_rate", "damsire_avg_rank", "damsire_recent_win_rate"):
            assert c in out.columns
        # bmsX: horse1 着順[1,2,1] + horse3 着順[2] → is_win=[1,0,1,0] → 0.5
        assert out["damsire_win_rate"].iloc[0] == pytest.approx(0.5)

    def test_temp_cols_not_leaked(self):
        peds = self._make_peds()
        m = _make_merger(_results_df_with_jockey(), peds)
        m._separated_hr_with_sire_dict[pd.Timestamp("2023-05-01")] = self._make_hr(peds)
        results = pd.DataFrame(
            {"horse_id": [1]}, index=pd.Index(["r99"], name="race_id")
        )
        out = m._add_damsire_stats(results, pd.Timestamp("2023-05-01"))
        assert "_ped_key" not in out.columns
        assert "peds_32" not in out.columns


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


# ──────────────────────────────────────────
# §2c 拡張: 馬主の集計特徴量（owner_win_rate / owner_avg_rank）
# ──────────────────────────────────────────

class TestAttachOwnerStats:
    """_attach_jockey_trainer_stats が owner 統計を shift(1) でリーク無く付与する。"""

    def _results(self):
        # 馬主 O1 が 3 レース連続出走（着順 1,1,5）。日付昇順。
        return pd.DataFrame(
            {
                "着順": [1, 1, 5],
                "n_horses": [10, 10, 10],
                "jockey_id": ["J1", "J1", "J1"],
                "trainer_id": ["T1", "T1", "T1"],
                "owner_id": ["O1", "O1", "O1"],
                "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01"]),
            },
            index=pd.Index(["r1", "r2", "r3"], name="race_id"),
        )

    def test_owner_stats_attached(self):
        m = _make_merger(self._results())
        m._attach_jockey_trainer_stats()
        assert "owner_win_rate" in m._results.columns
        assert "owner_avg_rank" in m._results.columns

    def test_owner_win_rate_is_leak_free(self):
        m = _make_merger(self._results())
        m._attach_jockey_trainer_stats()
        wr = m._results["owner_win_rate"]
        # 1走目は過去なし → NaN（shift(1)）
        assert pd.isna(wr.loc["r1"])
        # 2走目は過去=r1(勝ち) のみ → 1.0
        assert wr.loc["r2"] == pytest.approx(1.0)
        # 3走目は過去=r1,r2(共に勝ち) → 1.0（自レース r3 の着順5は含まない＝リーク無し）
        assert wr.loc["r3"] == pytest.approx(1.0)

    def test_no_owner_column_is_safe(self):
        df = self._results().drop(columns=["owner_id"])
        m = _make_merger(df)
        m._attach_jockey_trainer_stats()  # owner 列が無くても落ちない
        assert "owner_win_rate" not in m._results.columns
        assert "jockey_win_rate" in m._results.columns


# ──────────────────────────────────────────
# 人物年度別: _merge_person_yearly（前年 as-of 結合 + _pkey 堅牢性）
# ──────────────────────────────────────────

class TestMergePersonYearly:
    def _results(self):
        # 2023 年のレース → 前年=2022 の成績を引く
        return pd.DataFrame(
            {
                "horse_id": ["1", "2"],
                "jockey_id": [1009, 1010],  # int64（先頭ゼロ落ち）
                "trainer_id": ["01100", "01101"],
                "date": pd.to_datetime(["2023-04-01", "2023-04-01"]),
            },
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def _person_yearly_long(self):
        # jockey 01009 の 2022 年成績（前年）を1行
        return pd.DataFrame(
            {
                "entity_type": ["jockey"],
                "entity_id": ["01009"],
                "year": [2022],
                "勝率": [0.18],
                "複勝率": [0.45],
                "芝勝率": [0.2],
                "ダート勝率": [0.16],
                "重賞勝利": [3.0],
                "出走回数": [800.0],
            }
        )

    def _make_py_merger(self, py: pd.DataFrame):
        m = _make_merger(self._results())
        m._horse_info = pd.DataFrame()
        m._person_yearly = py
        return m

    def test_pkey_indexed_pickle(self):
        """persist_person_yearly 形式（_pkey index + entity_id 列）で結合できる。"""
        py = self._person_yearly_long().copy()
        py.index = pd.Index(
            py["entity_type"] + "/" + py["entity_id"], name="_pkey"
        )
        m = self._make_py_merger(py)
        m._merge_person_yearly()
        row = m._results[m._results["jockey_id"].astype(str).str.contains("1009")].iloc[0]
        assert row["jockey_py_勝率"] == pytest.approx(0.18)

    def test_double_migrated_pickle_does_not_crash(self):
        """二重移行（_pkey index AND _pkey 列が両方在る）でも落ちず結合できること（回帰）。"""
        py = self._person_yearly_long().copy()
        # 1回目の移行: _pkey index 化
        py.index = pd.Index(
            py["entity_type"] + "/" + py["entity_id"], name="_pkey"
        )
        # 2回目の移行をうっかり実行 → _pkey が列としても残る状態を再現
        py = py.reset_index()  # _pkey が列になる
        py.index = pd.Index(
            py["entity_type"] + "/" + py["entity_id"], name="_pkey"
        )
        # この時点で py は _pkey index かつ _pkey 列を持つ
        assert "_pkey" in py.columns and py.index.name == "_pkey"
        m = self._make_py_merger(py)
        m._merge_person_yearly()  # ValueError: cannot insert _pkey を出さない
        row = m._results[m._results["jockey_id"].astype(str).str.contains("1009")].iloc[0]
        assert row["jockey_py_勝率"] == pytest.approx(0.18)
        # 結合の副産物列が残っていないこと
        assert "_pkey" not in m._results.columns
        assert "_pry" not in m._results.columns

    def test_plain_entity_id_column_pickle(self):
        """旧形式（RangeIndex + entity_id 列、_pkey 無し）でも結合できる。"""
        py = self._person_yearly_long()  # RangeIndex, entity_id は列
        m = self._make_py_merger(py)
        m._merge_person_yearly()
        row = m._results[m._results["jockey_id"].astype(str).str.contains("1009")].iloc[0]
        assert row["jockey_py_勝率"] == pytest.approx(0.18)


# ──────────────────────────────────────────
# 市場歪み: _merge_odds_signals（(race_id,馬番) 左結合）
# ──────────────────────────────────────────

class TestMergeOddsSignals:
    def _results(self):
        return pd.DataFrame(
            {"horse_id": ["1", "2"], "馬番": [1, 2]},
            index=pd.Index(["r01", "r01"], name="race_id"),
        )

    def _signals(self):
        from src.preprocessing._market_signals import MARKET_SIGNAL_COLS

        df = pd.DataFrame(
            {
                "race_id": ["r01", "r01"],
                "馬番": [1, 2],
                "fukusho_implied_p": [1.5, 1.0],
                "place_overlay": [0.1, -0.1],
                "trio_top3_overlay": [0.05, -0.05],
                "trifecta_win_overlay": [0.2, -0.2],
                "trifecta_top3_overlay": [0.03, -0.03],
            }
        )
        assert set(MARKET_SIGNAL_COLS).issubset(df.columns)
        return df

    def test_merge_attaches_overlay_columns(self):
        m = _make_merger(self._results())
        m._odds_signals = self._signals()
        m._merge_odds_signals()
        assert m._results.loc["r01"].iloc[0]["place_overlay"] == pytest.approx(0.1)
        h2 = m._results[m._results["馬番"] == 2].iloc[0]
        assert h2["trifecta_win_overlay"] == pytest.approx(-0.2)
        assert m._results.index.name == "race_id"

    def test_empty_signals_noop(self):
        m = _make_merger(self._results())
        m._odds_signals = pd.DataFrame()
        m._merge_odds_signals()
        assert "place_overlay" not in m._results.columns

    def test_unmatched_umaban_is_nan(self):
        m = _make_merger(self._results())
        sig = self._signals()
        sig.loc[sig["馬番"] == 2, "馬番"] = 9  # 馬番2 を欠落させる
        m._odds_signals = sig
        m._merge_odds_signals()
        h2 = m._results[m._results["馬番"] == 2].iloc[0]
        assert pd.isna(h2["place_overlay"])
