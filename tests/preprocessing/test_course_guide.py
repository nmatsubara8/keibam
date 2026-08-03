"""距離別コースガイドマスタ（_course_guide.py）のユニットテスト。"""

from __future__ import annotations

import pandas as pd

from src.preprocessing._course_guide import add_course_guide_features, load_course_guide_master


def _seed(tmp_path):
    path = str(tmp_path / "course_guide_master.csv")
    pd.DataFrame([
        {"place_code": "05", "race_type": "芝", "course_len_m": 1400,
         "run_style_bias": 1, "time_bias": 0, "corner_radius_large": float("nan"),
         "drainage_good": float("nan"), "upset_prone": 1},
        {"place_code": "05", "race_type": "芝", "course_len_m": 2400,
         "run_style_bias": 1, "time_bias": 0, "corner_radius_large": float("nan"),
         "drainage_good": float("nan"), "upset_prone": float("nan")},
    ]).to_csv(path, index=False)
    return path


class TestLoad:
    def test_keys_normalized(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        assert set(gm["place_code"]) == {"05"}
        assert set(gm["course_len_m"].tolist()) == {1400, 2400}

    def test_missing_returns_empty(self):
        assert load_course_guide_master("/nonexistent.csv").empty


class TestAddGuide:
    def _results(self, course_len):
        return pd.DataFrame(
            {"開催": pd.array([5, 5], dtype="Int64"), "race_type": ["芝", "芝"],
             "course_len": course_len},
            index=pd.Index(["a", "b"], name="race_id"),
        )

    def test_attaches_by_distance_bucket(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        # course_len が 100m バケット（14=1400m, 24=2400m）でも結合できる
        out = add_course_guide_features(self._results([14, 24]), gm)
        assert out["guide_upset_prone"].iloc[0] == 1.0     # 東京芝1400 は荒れやすい
        assert pd.isna(out["guide_upset_prone"].iloc[1])   # 東京芝2400 は記載なし
        assert out["guide_run_style_bias"].iloc[0] == 1.0

    def test_attaches_by_meter(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        # 実距離表現（1400/2400 m）でも同じ結果
        out = add_course_guide_features(self._results([1400, 2400]), gm)
        assert out["guide_upset_prone"].iloc[0] == 1.0

    def test_distance_specific(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        # 同一コースでも距離が違えば別プロファイル（1400 のみ upset=1）
        out = add_course_guide_features(self._results([14, 14]), gm)
        assert (out["guide_upset_prone"] == 1.0).all()

    def test_all_feature_cols_present(self, tmp_path):
        from src.constants._course_guide import COURSE_GUIDE_FEATURE_COLS

        gm = load_course_guide_master(_seed(tmp_path))
        out = add_course_guide_features(self._results([14, 24]), gm)
        for c in COURSE_GUIDE_FEATURE_COLS:
            assert c in out.columns

    def test_empty_master_yields_nan_cols(self):
        out = add_course_guide_features(self._results([14, 24]), pd.DataFrame())
        assert out["guide_run_style_bias"].isna().all()

    def test_unknown_distance_is_nan(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        out = add_course_guide_features(self._results([18, 20]), gm)  # 1800/2000 は未登録
        assert out["guide_run_style_bias"].isna().all()


class TestGuideInteraction:
    def test_style_guide_fit(self):
        from src.preprocessing._interaction_features import add_interaction_features

        df = pd.DataFrame({
            "開催": pd.array([5, 5], dtype="Int64"), "race_type": ["芝", "芝"],
            "leg_type_binary": [0.0, 1.0],
            "guide_run_style_bias": [1.0, 1.0], "date": pd.to_datetime(["2024-01-01"] * 2),
        })
        out = add_interaction_features(df)
        # 前有利コース(+1): 前馬(leg=0)→+1（好相性）, 差し馬(leg=1)→−1（不利）
        assert out["style_guide_fit"].tolist() == [1.0, -1.0]


class TestCoverageGuard:
    def _results(self, course_len):
        return pd.DataFrame(
            {"開催": pd.array([5, 5], dtype="Int64"), "race_type": ["芝", "芝"],
             "course_len": course_len},
            index=pd.Index(["a", "b"], name="race_id"),
        )

    def test_require_coverage_raises_when_low(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        # 距離が master に無い(9999)→一致率0→require_coverage 指定で fail-closed
        r = self._results([9999, 9999])
        try:
            add_course_guide_features(r, gm, require_coverage=0.9)
            assert False, "should have raised"
        except RuntimeError as e:
            assert "coverage too low" in str(e)

    def test_require_coverage_passes_when_high(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        out = add_course_guide_features(self._results([14, 24]), gm, require_coverage=0.9)
        assert out["guide_run_style_bias"].iloc[0] == 1.0   # 一致率>=0.9→通過(両行 master 有)

    def test_default_no_guard_backward_compatible(self, tmp_path):
        gm = load_course_guide_master(_seed(tmp_path))
        # require_coverage 未指定は従来どおり（低一致でも raise しない）
        out = add_course_guide_features(self._results([9999, 9999]), gm)
        assert out["guide_run_style_bias"].isna().all()
