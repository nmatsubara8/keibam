"""dumminize_* / encode_* のキャラクタリゼーションテスト（リファクタ前の挙動固定）。

リファクタリング #1（dumminize 集約）・#4（encode 集約）の回帰ガード。
DataMerger を介さず、_make_fe で最小 DataFrame を注入して各変換の出力を固定する。
"""

from __future__ import annotations

import os
import sys
import types

import pandas as pd
import pytest

# ── tqdm スタブ（_data_merger 経由の import を通すため）──
_tqdm_stub = types.ModuleType("tqdm")
_tqdm_auto_stub = types.ModuleType("tqdm.auto")
_tqdm_auto_stub.tqdm = lambda x, **kw: x
_tqdm_stub.auto = _tqdm_auto_stub
sys.modules.setdefault("tqdm", _tqdm_stub)
sys.modules.setdefault("tqdm.auto", _tqdm_auto_stub)

from src.constants._master import Master  # noqa: E402
from src.preprocessing._feature_engineering import FeatureEngineering  # noqa: E402


def _make_fe(df: pd.DataFrame) -> FeatureEngineering:
    fe = object.__new__(FeatureEngineering)
    fe._FeatureEngineering__data = df.copy()
    return fe


# ──────────────────────────────────────────
# dumminize_* （単純系 7 メソッド）
# 各ケース: (メソッド名, 列名, カテゴリ tuple, prefix, 追加 drop 列)
# ──────────────────────────────────────────

_SIMPLE_DUMMIFY_CASES = [
    ("dumminize_race_type", "race_type", ("芝", "ダート", "障害"), "race_type_", []),
    ("dumminize_weather", "weather", Master.WEATHER_LIST, "weather_", []),
    ("dumminize_ground_state1", "ground_state1", Master.GROUND_STATE_LIST, "ground_state1_", []),
    ("dumminize_ground_state2", "ground_state2", Master.GROUND_STATE_LIST, "ground_state2_", ["race_condition"]),
    ("dumminize_sex", "性", Master.SEX_LIST, "性_", []),
    ("dumminize_around", "around", Master.AROUND_LIST, "around_", []),
    ("dumminize_race_class", "race_class", Master.RACE_CLASS_LIST, "race_class_", []),
]


@pytest.mark.parametrize("method,col,categories,prefix,extra_drops", _SIMPLE_DUMMIFY_CASES)
class TestSimpleDummify:
    def _build_df(self, col, categories, extra_drops):
        data = {col: list(categories[:2]) + [categories[0]]}
        # 追加 drop 対象の列も用意
        for ed in extra_drops:
            data[ed] = ["x", "y", "z"]
        # 無関係な特徴量列（保持されること確認用）
        data["keep_me"] = [1.0, 2.0, 3.0]
        return pd.DataFrame(data)

    def test_source_column_dropped(self, method, col, categories, prefix, extra_drops):
        fe = _make_fe(self._build_df(col, categories, extra_drops))
        getattr(fe, method)()
        assert col not in fe.featured_data.columns

    def test_dummy_columns_created(self, method, col, categories, prefix, extra_drops):
        fe = _make_fe(self._build_df(col, categories, extra_drops))
        getattr(fe, method)()
        cols = fe.featured_data.columns
        # pd.get_dummies(prefix="weather_") → "weather__晴"（prefix + "_" + value）
        for cat in categories:
            assert f"{prefix}_{cat}" in cols, f"missing {prefix}_{cat}"

    def test_extra_drops_removed(self, method, col, categories, prefix, extra_drops):
        fe = _make_fe(self._build_df(col, categories, extra_drops))
        getattr(fe, method)()
        for ed in extra_drops:
            assert ed not in fe.featured_data.columns

    def test_unrelated_column_kept(self, method, col, categories, prefix, extra_drops):
        fe = _make_fe(self._build_df(col, categories, extra_drops))
        getattr(fe, method)()
        assert "keep_me" in fe.featured_data.columns

    def test_returns_self(self, method, col, categories, prefix, extra_drops):
        fe = _make_fe(self._build_df(col, categories, extra_drops))
        assert getattr(fe, method)() is fe


# ──────────────────────────────────────────
# add_race_class_level （現レースの格 → 順序値）
# ──────────────────────────────────────────

class TestAddRaceClassLevel:
    def test_maps_class_to_level(self):
        df = pd.DataFrame({"race_class": [Master.RACE_CLASS_G1, Master.RACE_CLASS_1SHO]})
        fe = _make_fe(df)
        out = fe.add_race_class_level().featured_data
        assert out["race_class_level"].tolist() == [9, 2]

    def test_keeps_race_class_for_dummify(self):
        # add_race_class_level は race_class 列を残す（後段 dumminize_race_class が drop）
        df = pd.DataFrame({"race_class": [Master.RACE_CLASS_OPEN]})
        fe = _make_fe(df)
        fe.add_race_class_level()
        assert "race_class" in fe.featured_data.columns
        assert "race_class_level" in fe.featured_data.columns

    def test_unknown_class_is_nan(self):
        df = pd.DataFrame({"race_class": ["謎クラス"]})
        fe = _make_fe(df)
        out = fe.add_race_class_level().featured_data
        assert out["race_class_level"].isna().all()

    def test_missing_column_is_noop(self):
        df = pd.DataFrame({"keep_me": [1.0]})
        fe = _make_fe(df)
        assert fe.add_race_class_level() is fe
        assert "race_class_level" not in fe.featured_data.columns

    def test_chains_before_dummify(self):
        # add_race_class_level → dumminize_race_class の順で両方の列が揃う
        df = pd.DataFrame({"race_class": [Master.RACE_CLASS_G2]})
        fe = _make_fe(df)
        out = fe.add_race_class_level().dumminize_race_class().featured_data
        assert "race_class_level" in out.columns
        assert out["race_class_level"].iloc[0] == 8
        assert any(c.startswith("race_class_") for c in out.columns if c != "race_class_level")
        assert "race_class" not in out.columns


# ──────────────────────────────────────────
# dumminize_kaisai （特殊系: 別途固定。リファクタ対象外だが回帰ガード）
# ──────────────────────────────────────────

class TestDumminizeKaisai:
    def _build_df(self):
        from src.constants._horse_results_cols import HorseResultsCols

        place_vals = list(Master.PLACE_DICT.values())[:2]
        return pd.DataFrame(
            {
                HorseResultsCols.PLACE: place_vals + [place_vals[0]],
                "place": ["a", "b", "c"],
                "time": ["t1", "t2", "t3"],
                "keep_me": [1.0, 2.0, 3.0],
            }
        )

    def test_drops_place_and_time(self):
        fe = _make_fe(self._build_df())
        fe.dumminize_kaisai()
        assert "place" not in fe.featured_data.columns
        assert "time" not in fe.featured_data.columns

    def test_creates_place_dummies(self):
        from src.constants._horse_results_cols import HorseResultsCols

        fe = _make_fe(self._build_df())
        fe.dumminize_kaisai()
        prefix = f"{HorseResultsCols.PLACE}_"
        place_vals = list(Master.PLACE_DICT.values())
        assert any(c.startswith(prefix) for c in fe.featured_data.columns)
        assert f"{prefix}_{place_vals[0]}" in fe.featured_data.columns

    def test_returns_self(self):
        fe = _make_fe(self._build_df())
        assert fe.dumminize_kaisai() is fe


# ──────────────────────────────────────────
# encode_* （ラベルエンコード, master CSV 副作用あり）
# ──────────────────────────────────────────

_ENCODE_CASES = [
    ("encode_horse_id", "horse_id"),
    ("encode_jockey_id", "jockey_id"),
    ("encode_trainer_id", "trainer_id"),
    ("encode_owner_id", "owner_id"),
    ("encode_breeder_id", "breeder_id"),
]


@pytest.mark.parametrize("method,col", _ENCODE_CASES)
class TestEncode:
    def test_encodes_to_categorical_codes(self, tmp_path, monkeypatch, method, col):
        # MASTER_DIR を tmp に向ける（CSV 副作用を隔離）
        import src.preprocessing._feature_engineering as fe_mod

        monkeypatch.setattr(fe_mod.LocalPaths, "MASTER_DIR", str(tmp_path))
        df = pd.DataFrame({col: ["A", "B", "A", "C"], "keep_me": [1, 2, 3, 4]})
        fe = _make_fe(df)
        result = getattr(fe, method)()
        # 返り値は self
        assert result is fe
        # 対象列が Categorical 型になる
        assert str(fe.featured_data[col].dtype) == "category"
        # マスタ CSV が生成される
        assert os.path.isfile(os.path.join(str(tmp_path), col + ".csv"))

    def test_same_value_same_code(self, tmp_path, monkeypatch, method, col):
        import src.preprocessing._feature_engineering as fe_mod

        monkeypatch.setattr(fe_mod.LocalPaths, "MASTER_DIR", str(tmp_path))
        df = pd.DataFrame({col: ["A", "B", "A"], "keep_me": [1, 2, 3]})
        fe = _make_fe(df)
        getattr(fe, method)()
        codes = fe.featured_data[col]
        # 同じ "A" は同じコードに
        assert codes.iloc[0] == codes.iloc[2]
        assert codes.iloc[0] != codes.iloc[1]
