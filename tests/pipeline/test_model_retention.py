"""src/pipeline/_model_retention.py: モデル世代管理のテスト（tmp_path で隔離）。"""

from __future__ import annotations

import os

from src.pipeline import _model_retention as mr


def _make_date_dir(models_dir, date, fname="m.pickle"):
    d = os.path.join(models_dir, date)
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, fname), "w") as f:
        f.write("x")
    return d


class TestListDateDirs:
    def test_newest_first_and_filters_nondigit(self, tmp_path):
        md = str(tmp_path / "models")
        _make_date_dir(md, "20240101")
        _make_date_dir(md, "20240301")
        os.makedirs(os.path.join(md, "scratch"), exist_ok=True)  # 非日付 → 除外
        with open(os.path.join(md, "version_history.json"), "w") as f:
            f.write("[]")  # ファイル → 除外
        assert mr.list_model_date_dirs(md) == ["20240301", "20240101"]

    def test_empty_when_missing(self, tmp_path):
        assert mr.list_model_date_dirs(str(tmp_path / "nope")) == []


class TestSelectAndPrune:
    def test_select_keeps_newest_n(self, tmp_path):
        md = str(tmp_path / "models")
        for d in ("20240101", "20240201", "20240301"):
            _make_date_dir(md, d)
        targets = mr.select_models_to_delete(md, keep=2)
        assert targets == [os.path.join(md, "20240101")]

    def test_keep_zero_or_negative_deletes_nothing(self, tmp_path):
        md = str(tmp_path / "models")
        _make_date_dir(md, "20240101")
        assert mr.select_models_to_delete(md, keep=0) == []
        assert mr.select_models_to_delete(md, keep=-1) == []

    def test_dry_run_does_not_delete(self, tmp_path):
        md = str(tmp_path / "models")
        for d in ("20240101", "20240201"):
            _make_date_dir(md, d)
        targets = mr.prune_models(md, keep=1, dry_run=True)
        assert targets == [os.path.join(md, "20240101")]
        assert os.path.isdir(os.path.join(md, "20240101"))  # 残っている

    def test_prune_deletes_old(self, tmp_path):
        md = str(tmp_path / "models")
        for d in ("20240101", "20240201", "20240301"):
            _make_date_dir(md, d)
        deleted = mr.prune_models(md, keep=1, dry_run=False)
        assert set(deleted) == {
            os.path.join(md, "20240101"), os.path.join(md, "20240201"),
        }
        assert not os.path.isdir(os.path.join(md, "20240101"))
        assert os.path.isdir(os.path.join(md, "20240301"))  # 最新は残る
