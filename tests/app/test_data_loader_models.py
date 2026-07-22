"""app/_data_loader.py のモデルバージョン管理ヘルパのテスト。

予測ページ（2_prediction.py）の「適用モデル選択」が依存する
find_model_paths / list_model_versions / load_model_from_path /
load_model_by_version を、tmp の models ディレクトリで検証する。
"""

from __future__ import annotations

import json
import os

import dill
import pytest

from app._data_loader import (
    find_model_paths,
    list_model_versions,
    load_latest_model,
    load_model_by_version,
    load_model_from_path,
)


def _write_model(models_dir: str, date_dir: str, version: str, payload) -> str:
    d = os.path.join(models_dir, date_dir)
    os.makedirs(d, exist_ok=True)
    path = os.path.join(d, f"{version}.pickle")
    with open(path, "wb") as f:
        dill.dump(payload, f)
    return path


def _write_history(models_dir: str, records: list[dict]) -> None:
    os.makedirs(models_dir, exist_ok=True)
    with open(os.path.join(models_dir, "version_history.json"), "w", encoding="utf-8") as f:
        json.dump(records, f)


# ---------------------------------------------------------------------------
# find_model_paths
# ---------------------------------------------------------------------------


class TestFindModelPaths:
    def test_empty_when_missing(self, tmp_path):
        assert find_model_paths(str(tmp_path / "nope")) == []

    def test_newest_first_by_date_dir(self, tmp_path):
        md = str(tmp_path / "models")
        _write_model(md, "20240101", "20240101_keibam", {"v": 1})
        _write_model(md, "20240201", "20240201_keibam", {"v": 2})
        paths = find_model_paths(md)
        assert len(paths) == 2
        # 新しい日付ディレクトリが先頭
        assert "20240201" in paths[0]
        assert "20240101" in paths[1]

    def test_excludes_non_date_prefixed_experiment_models(self, tmp_path):
        """--version-name の使い捨て実験（日付接頭辞なし）は本番既定を乗っ取らないよう除外。"""
        md = str(tmp_path / "models")
        _write_model(md, "20240101", "20240101_keibam", {"v": "prod"})
        # 実験モデルを新しい日付ディレクトリに置いても拾わない
        _write_model(md, "20240301", "noodds_keibam", {"v": "exp"})
        _write_model(md, "20240301", "base2016", {"v": "exp2"})
        paths = find_model_paths(md)
        assert len(paths) == 1
        assert "20240101_keibam" in paths[0]  # 本番のみ・実験は先頭に来ない


# ---------------------------------------------------------------------------
# load_model_from_path / load_latest_model
# ---------------------------------------------------------------------------


class TestLoadModel:
    def test_load_from_path_roundtrip(self, tmp_path):
        md = str(tmp_path / "models")
        path = _write_model(md, "20240101", "20240101_keibam", {"hello": "world"})
        loaded = load_model_from_path(path)
        assert loaded == {"hello": "world"}

    def test_latest_model_is_newest(self, tmp_path):
        md = str(tmp_path / "models")
        _write_model(md, "20240101", "20240101_keibam", {"v": "old"})
        _write_model(md, "20240301", "20240301_keibam", {"v": "new"})
        assert load_latest_model(md) == {"v": "new"}

    def test_latest_model_none_when_empty(self, tmp_path):
        assert load_latest_model(str(tmp_path / "models")) is None


# ---------------------------------------------------------------------------
# load_model_by_version
# ---------------------------------------------------------------------------


class TestLoadByVersion:
    def test_loads_matching_version(self, tmp_path):
        md = str(tmp_path / "models")
        _write_model(md, "20240101", "20240101_keibam", {"v": "a"})
        _write_model(md, "20240201", "20240201_keibam", {"v": "b"})
        assert load_model_by_version("20240101_keibam", md) == {"v": "a"}

    def test_missing_version_raises(self, tmp_path):
        md = str(tmp_path / "models")
        _write_model(md, "20240101", "20240101_keibam", {"v": "a"})
        with pytest.raises(FileNotFoundError):
            load_model_by_version("does_not_exist", md)


# ---------------------------------------------------------------------------
# list_model_versions（版選択 UI のラベル元）
# ---------------------------------------------------------------------------


class TestListModelVersions:
    def test_empty_when_no_history(self, tmp_path):
        assert list_model_versions(str(tmp_path / "models")) == []

    def test_returns_newest_first(self, tmp_path):
        md = str(tmp_path / "models")
        # version_history.json は古い順で保存され、新しい順で返る
        _write_history(md, [
            {"version": "20240101_keibam", "auc_test": 0.70},
            {"version": "20240201_keibam", "auc_test": 0.75},
        ])
        versions = list_model_versions(md)
        assert [v["version"] for v in versions] == ["20240201_keibam", "20240101_keibam"]
        assert versions[0]["auc_test"] == 0.75
