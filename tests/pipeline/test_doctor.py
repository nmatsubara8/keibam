"""src/pipeline/_doctor.py: ヘルスチェック純粋ロジックのテスト。

now / パス / db_path を注入して決定的に検証する。DB は tmp に隔離。
"""

from __future__ import annotations

import datetime as dt
import os

import pytest

from src.pipeline import _doctor as doc
from src.storage._db import _reset_engine_for_testing


@pytest.fixture(autouse=True)
def _reset_engine():
    _reset_engine_for_testing()
    yield
    _reset_engine_for_testing()


_NOW = dt.datetime(2026, 6, 12, 12, 0, 0)


def _touch(path, *, hours_ago=0.0):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        f.write("x")
    if hours_ago:
        t = _NOW.timestamp() - hours_ago * 3600
        os.utime(path, (t, t))


# ---------------------------------------------------------------------------
# check_file
# ---------------------------------------------------------------------------


class TestCheckFile:
    def test_missing_required_is_error(self, tmp_path):
        r = doc.check_file("x", str(tmp_path / "no.pkl"), now=_NOW)
        assert r.level == doc.ERROR

    def test_missing_optional_is_warn(self, tmp_path):
        r = doc.check_file("x", str(tmp_path / "no.pkl"), now=_NOW, required=False)
        assert r.level == doc.WARN

    def test_fresh_is_ok(self, tmp_path):
        p = str(tmp_path / "a.pkl")
        _touch(p, hours_ago=1)
        r = doc.check_file("a", p, now=_NOW, warn_age_h=48)
        assert r.level == doc.OK

    def test_stale_is_warn(self, tmp_path):
        p = str(tmp_path / "a.pkl")
        _touch(p, hours_ago=72)
        r = doc.check_file("a", p, now=_NOW, warn_age_h=48)
        assert r.level == doc.WARN

    def test_very_stale_is_error(self, tmp_path):
        p = str(tmp_path / "a.pkl")
        _touch(p, hours_ago=200)
        r = doc.check_file("a", p, now=_NOW, warn_age_h=48, err_age_h=168)
        assert r.level == doc.ERROR


# ---------------------------------------------------------------------------
# check_models / model_pickle_paths
# ---------------------------------------------------------------------------


class TestCheckModels:
    def test_no_models_is_error(self, tmp_path):
        r = doc.check_models(str(tmp_path / "models"), now=_NOW)
        assert r.level == doc.ERROR

    def test_newest_first_and_ok(self, tmp_path):
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240101", "a.pickle"), hours_ago=100)
        _touch(os.path.join(md, "20240201", "b.pickle"), hours_ago=1)
        paths = doc.model_pickle_paths(md)
        assert "20240201" in paths[0]
        r = doc.check_models(md, now=_NOW, warn_age_h=24 * 14)
        assert r.level == doc.OK

    def test_old_model_warns(self, tmp_path):
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240101", "a.pickle"), hours_ago=24 * 30)
        r = doc.check_models(md, now=_NOW, warn_age_h=24 * 14)
        assert r.level == doc.WARN


class TestCheckCalibration:
    def _cal_path(self, md, name):
        from src.simulation._calibrate import (
            blend_weights_path,
            place_exponents_path,
            win_calibrator_path,
        )

        return {
            "place": place_exponents_path(md),
            "win": win_calibrator_path(md),
            "blend": blend_weights_path(md),
        }[name]

    def test_missing_artifacts_warn(self, tmp_path):
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240201", "20240201_keibam.pickle"), hours_ago=1)
        r = doc.check_calibration(md, now=_NOW)
        assert r.level == doc.WARN
        assert "未fit" in r.detail

    def test_stale_calibration_warn(self, tmp_path):
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240201", "20240201_keibam.pickle"), hours_ago=1)
        _touch(self._cal_path(md, "place"), hours_ago=100)  # モデルより古い
        r = doc.check_calibration(md, now=_NOW)
        assert r.level == doc.WARN
        assert "古い" in r.detail

    def test_fresh_calibration_ok(self, tmp_path):
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240201", "20240201_keibam.pickle"), hours_ago=100)
        for n in ("place", "win", "blend"):
            _touch(self._cal_path(md, n), hours_ago=1)  # モデルより新しい
        r = doc.check_calibration(md, now=_NOW)
        assert r.level == doc.OK

    def test_experiment_model_not_counted_as_production(self, tmp_path):
        # 使い捨て実験モデル（日付接頭辞なし）は本番モデル扱いしない → 較正物ありなら OK
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20240301", "noodds_keibam.pickle"), hours_ago=1)
        _touch(self._cal_path(md, "place"), hours_ago=50)
        r = doc.check_calibration(md, now=_NOW)
        assert r.level == doc.OK  # 本番モデル未検出扱い（古い判定しない）


# ---------------------------------------------------------------------------
# check_db_connection / check_featured_meta / check_disk_space
# ---------------------------------------------------------------------------


class TestOtherChecks:
    def test_db_connection_ok(self, tmp_path):
        r = doc.check_db_connection(str(tmp_path / "test.db"))
        assert r.level == doc.OK

    def test_featured_meta_warn_when_empty(self, tmp_path):
        r = doc.check_featured_meta(str(tmp_path / "test.db"))
        assert r.level == doc.WARN

    def test_disk_space_ok(self, tmp_path):
        r = doc.check_disk_space(str(tmp_path), warn_free_gb=0.0)
        assert r.level == doc.OK


# ---------------------------------------------------------------------------
# overall / run_doctor
# ---------------------------------------------------------------------------


class TestOverallAndRun:
    def test_overall_level_precedence(self):
        rs = [doc.CheckResult("a", doc.OK, ""), doc.CheckResult("b", doc.WARN, "")]
        assert doc.overall_level(rs) == doc.WARN
        rs.append(doc.CheckResult("c", doc.ERROR, ""))
        assert doc.overall_level(rs) == doc.ERROR

    def test_run_doctor_all_present_ok_or_warn(self, tmp_path):
        data = {"results.pkl": str(tmp_path / "r.pkl")}
        _touch(data["results.pkl"], hours_ago=1)
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20260612", "m.pickle"), hours_ago=1)
        results, level = doc.run_doctor(
            now=_NOW, data_paths=data, models_dir=md,
            db_path=str(tmp_path / "test.db"),
            check_duplicates=False,
        )
        names = {r.name for r in results}
        assert {"results.pkl", "models", "db", "featured_meta", "disk"}.issubset(names)
        # featured_meta が空 → 全体は WARN 以上 OK 以下（ERROR ではない）
        assert level in (doc.OK, doc.WARN)

    def test_run_doctor_missing_data_is_error(self, tmp_path):
        data = {"results.pkl": str(tmp_path / "missing.pkl")}
        results, level = doc.run_doctor(
            now=_NOW, data_paths=data, models_dir=str(tmp_path / "models"),
            db_path=str(tmp_path / "test.db"),
            check_duplicates=False,
        )
        assert level == doc.ERROR

    def test_run_doctor_duplicate_raw_is_error(self, tmp_path):
        import pandas as pd

        data = {"results.pkl": str(tmp_path / "r.pkl")}
        _touch(data["results.pkl"], hours_ago=1)
        md = str(tmp_path / "models")
        _touch(os.path.join(md, "20260612", "m.pickle"), hours_ago=1)
        race_info = tmp_path / "race_info.pkl"
        pd.DataFrame({"race_id": ["r1", "r1"], "x": [1, 2]}).to_pickle(race_info)
        results, level = doc.run_doctor(
            now=_NOW, data_paths=data, models_dir=md,
            db_path=str(tmp_path / "test.db"),
            check_duplicates=True,
            duplicate_path_overrides={"raw_race_info": str(race_info)},
        )
        assert level == doc.ERROR
        assert any(r.name == "dup.raw_race_info" and r.level == doc.ERROR for r in results)