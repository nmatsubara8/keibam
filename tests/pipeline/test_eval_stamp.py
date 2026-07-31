"""検証メタデータ EvalStamp の単体テスト。"""
from __future__ import annotations

from src.pipeline._eval_stamp import (
    date_range,
    feature_schema_hash,
    format_stamp,
    make_stamp,
)


def test_feature_schema_hash_deterministic_and_order_sensitive():
    h1 = feature_schema_hash(["a", "b", "c"])
    assert h1 == feature_schema_hash(["a", "b", "c"])   # 決定的
    assert h1 != feature_schema_hash(["a", "c", "b"])   # 列順違い→別hash
    assert len(h1) == 12


def test_date_range_strings_and_none():
    assert date_range(["2020-03-01", "2019-12-31", "2021-07-05"]) == ("2019-12-31", "2021-07-05")
    assert date_range([]) is None
    assert date_range([None, "", "NaT", "nan"]) is None
    # ISO datetime 文字列も先頭10桁で日付化
    assert date_range(["2020-03-01 12:00:00"]) == ("2020-03-01", "2020-03-01")


def test_make_stamp_assembles_all_fields():
    s = make_stamp(
        model_version="20260731_jra",
        feature_names=["x1", "x2", "jrdb_gaikyu_has"],
        training_dates=["2015-01-01", "2023-12-31"],
        eval_dates=["2024-01-01", "2026-07-01"],
        drop_columns=["rank", "単勝"],
        odds_included=False,
        seed=0,
        split_method="year<2024",
    )
    assert s["model_version"] == "20260731_jra"
    assert s["n_features"] == 3
    assert s["feature_schema_hash"] == feature_schema_hash(["x1", "x2", "jrdb_gaikyu_has"])
    assert s["training_period"] == ("2015-01-01", "2023-12-31")
    assert s["eval_period"] == ("2024-01-01", "2026-07-01")
    assert s["drop_columns"] == ("rank", "単勝")
    assert s["odds_included"] is False
    assert s["seed"] == 0 and s["split_method"] == "year<2024"


def test_make_stamp_handles_missing_inputs():
    s = make_stamp()
    assert s["feature_schema_hash"] is None and s["n_features"] is None
    assert s["training_period"] is None and s["eval_period"] is None
    assert s["drop_columns"] == ()


def test_format_stamp_survives_none_ranges():
    line = format_stamp(make_stamp(seed=1))
    assert line.startswith("[検証メタ]")
    assert "train=None" in line and "eval=None" in line


def test_make_stamp_accepts_generator_feature_names():
    # feature_names がジェネレータでも hash と n が両方取れる（二重消費しない）
    s = make_stamp(feature_names=(c for c in ["a", "b"]))
    assert s["n_features"] == 2 and s["feature_schema_hash"] is not None
