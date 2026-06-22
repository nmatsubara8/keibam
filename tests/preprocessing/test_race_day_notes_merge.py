"""レース当日ノートのマージ＋特徴量化（P0-2）の単体テスト。

DataMerger._merge_race_day_notes と FeatureEngineering の符号化を、合成データで検証する。
DataMerger は重い processor 群を要するため __new__ でバイパスしてメソッドを直接叩く。
"""

import numpy as np
import pandas as pd
import pytest

from src.constants._master import Master
from src.preprocessing._data_merger import DataMerger


def _merger_with_notes(results, training=None, paddock=None, comment=None):
    m = DataMerger.__new__(DataMerger)
    m._results = results
    m._training = training if training is not None else pd.DataFrame()
    m._paddock = paddock if paddock is not None else pd.DataFrame()
    m._comment = comment if comment is not None else pd.DataFrame()
    return m


def _results(umaban):
    df = pd.DataFrame({"馬番": umaban, "date": "2026-06-14"}, index=["R1"] * len(umaban))
    df.index.name = "race_id"
    return df


def _notes(cols):
    df = pd.DataFrame(cols, index=["R1"] * len(next(iter(cols.values()))))
    df.index.name = "race_id"
    return df


class TestMergeRaceDayNotes:
    def test_left_join_on_race_id_umaban(self):
        results = _results([1, 2, 3])
        training = _notes({"馬番": [1, 3], "調教評価": ["叩き良化", "好気配"],
                           "映像グレード": ["B", "A"], "horse_id": ["x", "y"]})
        m = _merger_with_notes(results, training=training)
        m._merge_race_day_notes()
        out = m._results
        assert out.index.name == "race_id"
        assert len(out) == 3  # 行数は results 基準で不変
        by = out.set_index("馬番")
        assert by.loc[1, "調教評価"] == "叩き良化"
        assert by.loc[3, "映像グレード"] == "A"
        # 注記が無い馬番2は NaN
        assert pd.isna(by.loc[2, "調教評価"])
        # horse_id 等の重複列は持ち込まない
        assert "horse_id" not in out.columns or out["horse_id"].isna().all()

    def test_empty_notes_is_noop(self):
        results = _results([1, 2])
        m = _merger_with_notes(results)
        m._merge_race_day_notes()
        assert list(m._results.columns) == ["馬番", "date"]

    def test_all_three_sources_merge(self):
        results = _results([1, 2])
        m = _merger_with_notes(
            results,
            training=_notes({"馬番": [1], "調教評価": ["叩き良化"], "映像グレード": ["B"]}),
            paddock=_notes({"馬番": [2], "パドック評価": ["穴"], "パドックコメント": ["上向く"]}),
            comment=_notes({"馬番": [1], "厩舎コメント": ["good"], "コメント評価": [""]}),
        )
        m._merge_race_day_notes()
        cols = set(m._results.columns)
        assert {"調教評価", "映像グレード", "パドック評価", "パドックコメント", "厩舎コメント"}.issubset(cols)


class _MergerStub:
    def __init__(self, df):
        self.merged_data = df


def _fe(df):
    from src.preprocessing._feature_engineering import FeatureEngineering

    return FeatureEngineering(_MergerStub(df))


class TestEncodings:
    def test_video_grade_ordinal(self):
        fe = _fe(pd.DataFrame({"映像グレード": ["A", "B", "C", None]}))
        out = fe.encode_video_grade().featured_data
        assert out["映像グレード"].tolist()[:3] == [2, 1, 0]

    def test_training_eval_best_effort_ordinal(self):
        out = _fe(pd.DataFrame({"調教評価": ["叩き良化", "好気配", "謎の語"]})).encode_training_eval().featured_data
        assert "調教評価" not in out.columns  # テキスト列は drop
        scores = out["調教評価_score"]
        assert scores.iloc[0] == Master.TRAINING_EVAL_ORDINAL["叩き良化"]
        assert scores.iloc[1] == Master.TRAINING_EVAL_ORDINAL["好気配"]
        assert pd.isna(scores.iloc[2])  # 未知語は NaN

    def test_paddock_eval_onehot(self):
        out = _fe(pd.DataFrame({"パドック評価": ["A", "穴"]})).dumminize_paddock_eval().featured_data
        assert any("パドック評価_" in c and "穴" in c for c in out.columns)
        assert "パドック評価" not in out.columns

    def test_drop_text_notes_keeps_others(self):
        df = pd.DataFrame({"厩舎コメント": ["x"], "パドックコメント": ["y"],
                           "コメント評価": [""], "馬番": [1]})
        out = _fe(df).drop_text_note_columns().featured_data
        assert list(out.columns) == ["馬番"]

    def test_encodings_noop_when_columns_absent(self):
        # 列が無くても例外を出さず素通り（再取得前の状態）
        fe = _fe(pd.DataFrame({"馬番": [1, 2]}))
        out = (
            fe.encode_video_grade()
            .encode_training_eval()
            .dumminize_paddock_eval()
            .drop_text_note_columns()
            .featured_data
        )
        assert list(out.columns) == ["馬番"]


def test_master_ordinals_defined():
    assert Master.VIDEO_GRADE_ORDINAL["A"] > Master.VIDEO_GRADE_ORDINAL["C"]
    assert "穴" in Master.PADDOCK_EVAL_LIST
    assert Master.TRAINING_EVAL_ORDINAL["好気配"] > Master.TRAINING_EVAL_ORDINAL["不安"]
    assert np.isnan(pd.Series(["未知"]).map(Master.TRAINING_EVAL_ORDINAL).iloc[0])


class TestMergeYosoMarks:
    """予想印ロング → (race_id,馬番) コンセンサス左結合（_merge_yoso_marks）。"""

    def _results(self, umaban):
        df = pd.DataFrame({"馬番": umaban, "date": "2026-06-14"}, index=["R1"] * len(umaban))
        df.index.name = "race_id"
        return df

    def _yoso(self):
        return pd.DataFrame(
            {
                "馬番": [1, 1, 2],
                "predictor_yid": ["a", "b", "a"],
                "predictor_name": ["A", "B", "A"],
                "goods_kbn": ["1", "1", "1"],
                "mark": ["◎", "◎", "○"],
                "mark_score": [5, 5, 4],
            },
            index=pd.Index(["R1"] * 3, name="race_id"),
        )

    def test_consensus_merge(self):
        m = DataMerger.__new__(DataMerger)
        m._results = self._results([1, 2, 3])
        m._yoso_marks = self._yoso()
        m._merge_yoso_marks()
        by = m._results.set_index("馬番")
        assert by.loc[1, "yoso_n_marks"] == 2 and by.loc[1, "yoso_n_honmei"] == 2
        assert by.loc[1, "yoso_score_mean"] == 5.0
        assert by.loc[2, "yoso_n_marks"] == 1 and by.loc[2, "yoso_n_honmei"] == 0
        assert pd.isna(by.loc[3, "yoso_n_marks"])  # 印なし馬は NaN

    def test_empty_is_noop(self):
        m = DataMerger.__new__(DataMerger)
        m._results = self._results([1, 2])
        m._yoso_marks = pd.DataFrame()
        m._merge_yoso_marks()
        assert not any(c.startswith("yoso_") for c in m._results.columns)


class TestYosoPredictorSkill:
    """予想家 as-of ◎的中率による加重（_add_yoso_predictor_skill・自前計算・リーク無し）。"""

    def test_as_of_skill_weighting(self):
        results = pd.DataFrame(
            {"馬番": [1, 2, 3], "着順": [1, 5, 2],
             "date": pd.to_datetime(["2023-01-01", "2023-02-01", "2023-03-01"])},
            index=pd.Index(["R1", "R2", "R3"], name="race_id"),
        )
        yoso = pd.DataFrame(
            {"馬番": [1, 2, 3, 3], "predictor_yid": ["A", "A", "A", "B"],
             "mark": ["◎", "◎", "◎", "◎"], "mark_score": [5, 5, 5, 5]},
            index=pd.Index(["R1", "R2", "R3", "R3"], name="race_id"),
        )
        m = DataMerger.__new__(DataMerger)
        m._results, m._yoso_marks = results, yoso
        m._add_yoso_predictor_skill()
        out = m._results
        # R2: A の as-of = R1的中/1 = 1.0
        assert out.loc["R2", "yoso_best_skill"] == pytest.approx(1.0)
        # R3 馬番3: A=(1+0)/2=0.5, B=NaN → sum=0.5, best=0.5
        assert out.loc["R3", "yoso_honmei_skill_sum"] == pytest.approx(0.5)
        assert out.loc["R3", "yoso_best_skill"] == pytest.approx(0.5)
        # R1: A は履歴ゼロ → best=NaN（リーク無し）
        assert pd.isna(out.loc["R1", "yoso_best_skill"])

    def test_empty_is_noop(self):
        results = pd.DataFrame(
            {"馬番": [1], "着順": [1], "date": pd.to_datetime(["2023-01-01"])},
            index=pd.Index(["R1"], name="race_id"),
        )
        m = DataMerger.__new__(DataMerger)
        m._results, m._yoso_marks = results, pd.DataFrame()
        m._add_yoso_predictor_skill()
        assert "yoso_best_skill" not in m._results.columns


class TestMergePersonYearly:
    """騎手/調教師の前年成績 as-of 結合（_merge_person_yearly）。"""

    def test_prev_year_join(self):
        results = pd.DataFrame(
            {"馬番": [1, 2], "jockey_id": ["J1", "J2"], "trainer_id": ["T1", "T1"],
             "date": pd.to_datetime(["2026-03-01", "2026-03-01"])},
            index=pd.Index(["R1", "R1"], name="race_id"),
        )
        py = pd.DataFrame(
            {"entity_type": ["jockey", "jockey", "trainer"], "year": [2025, 2024, 2025],
             "勝率": [0.20, 0.99, 0.10], "複勝率": [0.5, 0.9, 0.3], "芝勝率": [0.22, 0.9, 0.11],
             "ダート勝率": [0.18, 0.9, 0.09], "重賞勝利": [5, 99, 2], "出走回数": [800, 1, 400]},
            index=pd.Index(["J1", "J1", "T1"], name="entity_id"),
        )
        m = DataMerger.__new__(DataMerger)
        m._results, m._person_yearly = results, py
        m._merge_person_yearly()
        by = m._results.set_index("馬番")
        # 前年(2025)を結合・2024 は無視
        assert by.loc[1, "jockey_py_勝率"] == pytest.approx(0.20)
        assert by.loc[1, "trainer_py_勝率"] == pytest.approx(0.10)
        # 未登録騎手 J2 は NaN
        assert pd.isna(by.loc[2, "jockey_py_勝率"])

    def test_empty_is_noop(self):
        results = pd.DataFrame(
            {"馬番": [1], "jockey_id": ["J1"], "date": pd.to_datetime(["2026-03-01"])},
            index=pd.Index(["R1"], name="race_id"),
        )
        m = DataMerger.__new__(DataMerger)
        m._results, m._person_yearly = results, pd.DataFrame()
        m._merge_person_yearly()
        assert not any(c.startswith("jockey_py_") for c in m._results.columns)


class TestYosoProfileSkill:
    """予想家プロフィール prior による◎加重（_add_yoso_profile_skill）。"""

    def test_profile_weighting(self):
        results = pd.DataFrame({"馬番": [1, 2, 3]}, index=pd.Index(["R1"] * 3, name="race_id"))
        yoso = pd.DataFrame(
            {"馬番": [1, 1, 2], "predictor_yid": ["A", "B", "A"], "mark": ["◎", "◎", "○"]},
            index=pd.Index(["R1"] * 3, name="race_id"),
        )
        prior = pd.DataFrame(
            {"profile_honmei_winrate": [0.30, 0.10]},
            index=pd.Index(["A", "B"], name="predictor_yid"),
        )
        m = DataMerger.__new__(DataMerger)
        m._results, m._yoso_marks, m._yoso_predictor = results, yoso, prior
        m._add_yoso_profile_skill()
        by = m._results.set_index("馬番")
        assert by.loc[1, "yoso_profile_skill_sum"] == pytest.approx(0.40)  # A+B
        assert by.loc[1, "yoso_profile_best"] == pytest.approx(0.30)
        assert pd.isna(by.loc[2, "yoso_profile_skill_sum"])  # ○のみ→対象外

    def test_empty_is_noop(self):
        results = pd.DataFrame({"馬番": [1]}, index=pd.Index(["R1"], name="race_id"))
        m = DataMerger.__new__(DataMerger)
        m._results, m._yoso_marks, m._yoso_predictor = results, pd.DataFrame(), pd.DataFrame()
        m._add_yoso_profile_skill()
        assert not any(c.startswith("yoso_profile") for c in m._results.columns)
