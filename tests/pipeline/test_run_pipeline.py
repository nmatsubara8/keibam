"""src/pipeline/run_pipeline.py::_parse_args の CLI 引数解析テスト。

argparse の構造（サブコマンド・相互排他グループ・各フラグの dest/型/既定値）のみを
検証する。重い依存（selenium/optuna 等）には触れない純粋テスト。
"""

from __future__ import annotations

import pytest

from src.pipeline.run_pipeline import _filter_final_odds_race_ids
from src.pipeline.run_pipeline import _parse_args


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------


class TestIngestArgs:
    def test_race_id_list_parsed_as_ints(self):
        args = _parse_args(["ingest", "--race-id", "1", "2", "3"])
        assert args.job == "ingest"
        assert args.race_ids == [1, 2, 3]
        assert args.force is False

    def test_post_date(self):
        args = _parse_args(["ingest", "--post-date", "20240101"])
        assert args.job == "ingest"
        assert args.post_date == "20240101"

    def test_force_flag(self):
        args = _parse_args(["ingest", "--race-id", "1", "--force"])
        assert args.force is True

    def test_race_id_and_post_date_are_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            _parse_args(["ingest", "--race-id", "1", "--post-date", "20240101"])

    def test_one_of_race_id_or_post_date_required(self):
        with pytest.raises(SystemExit):
            _parse_args(["ingest"])


# ---------------------------------------------------------------------------
# retrain
# ---------------------------------------------------------------------------


class TestRetrainArgs:
    def test_defaults(self):
        args = _parse_args(["retrain"])
        assert args.job == "retrain"
        assert args.version_name is None
        assert args.no_stacking is False
        assert args.with_tuning is False
        assert args.params_rank is None
        assert args.use_selected_params is False

    def test_flags_and_values(self):
        args = _parse_args(
            [
                "retrain",
                "--version-name", "v1",
                "--no-stacking",
                "--with-tuning",
                "--params-rank", "3",
                "--use-selected-params",
            ]
        )
        assert args.version_name == "v1"
        assert args.no_stacking is True
        assert args.with_tuning is True
        assert args.params_rank == 3
        assert args.use_selected_params is True


# ---------------------------------------------------------------------------
# evaluate-odds-dynamics
# ---------------------------------------------------------------------------


class TestEvaluateOddsDynamicsArgs:
    def test_default_holdout_frac(self):
        args = _parse_args(["evaluate-odds-dynamics"])
        assert args.job == "evaluate-odds-dynamics"
        assert args.holdout_frac == 0.2

    def test_custom_holdout_frac(self):
        args = _parse_args(["evaluate-odds-dynamics", "--holdout-frac", "0.3"])
        assert args.holdout_frac == 0.3


# ---------------------------------------------------------------------------
# サブコマンド必須
# ---------------------------------------------------------------------------


def test_subcommand_required():
    with pytest.raises(SystemExit):
        _parse_args([])


def test_unknown_subcommand_errors():
    with pytest.raises(SystemExit):
        _parse_args(["frobnicate"])


# ---------------------------------------------------------------------------
# doctor
# ---------------------------------------------------------------------------


class TestDoctorArgs:
    def test_defaults(self):
        args = _parse_args(["doctor"])
        assert args.job == "doctor"
        assert args.json is False
        assert args.strict is False
        assert args.prune_models is None

    def test_flags(self):
        args = _parse_args(["doctor", "--json", "--strict", "--prune-models", "5"])
        assert args.json is True
        assert args.strict is True
        assert args.prune_models == 5


# ---------------------------------------------------------------------------
# fetch-final-odds
# ---------------------------------------------------------------------------


class TestFetchFinalOddsArgs:
    def test_from_results_flag(self):
        args = _parse_args(["fetch-final-odds", "--from-results"])
        assert args.from_results is True
        assert args.force is False

    def test_years_limit_force(self):
        args = _parse_args([
            "fetch-final-odds", "--from-results",
            "--years", "2010", "2011", "--limit", "500", "--force",
        ])
        assert args.years == [2010, 2011]
        assert args.limit == 500
        assert args.force is True

    def test_race_id_and_from_results_mutually_exclusive(self):
        with pytest.raises(SystemExit):
            _parse_args(["fetch-final-odds", "--race-id", "1", "--from-results"])


class TestFilterFinalOddsRaceIds:
    _IDS = ["202006010101", "202006010102", "202406010101", "202406010102"]

    def test_year_filter(self):
        out = _filter_final_odds_race_ids(self._IDS, years=[2024])
        assert out == ["202406010101", "202406010102"]

    def test_resume_skips_done(self):
        out = _filter_final_odds_race_ids(self._IDS, done={"202006010101", "202406010101"})
        assert out == ["202006010102", "202406010102"]

    def test_force_ignores_done(self):
        out = _filter_final_odds_race_ids(self._IDS, done={"202006010101"}, force=True)
        assert out == self._IDS

    def test_limit(self):
        assert _filter_final_odds_race_ids(self._IDS, limit=2) == self._IDS[:2]

    def test_combined_year_resume_limit(self):
        out = _filter_final_odds_race_ids(
            self._IDS, years=[2024], done={"202406010101"}, limit=5
        )
        assert out == ["202406010102"]


class TestIngestSourceArg:
    def test_source_default_none(self):
        args = _parse_args(["ingest", "--race-id", "1"])
        assert args.source is None

    def test_source_value(self):
        args = _parse_args(["ingest", "--race-id", "1", "--source", "jravan"])
        assert args.source == "jravan"


class TestResolveDataSource:
    def test_cli_wins(self, monkeypatch):
        from src.pipeline.run_pipeline import _resolve_data_source

        class _A:
            source = "jravan"
        assert _resolve_data_source(_A()) == "jravan"

    def test_falls_back_to_saved(self, monkeypatch):
        import src.preparing._data_source as ds
        from src.pipeline.run_pipeline import _resolve_data_source

        monkeypatch.setattr(ds, "load_selected_source", lambda *a, **k: "jravan")

        class _A:
            source = None
        assert _resolve_data_source(_A()) == "jravan"


class TestCalibrateTakeoutArgs:
    def test_defaults(self):
        args = _parse_args(["calibrate-takeout"])
        assert args.job == "calibrate-takeout"
        assert args.min_samples == 20
        assert args.dry_run is False

    def test_flags(self):
        args = _parse_args(["calibrate-takeout", "--min-samples", "50", "--dry-run"])
        assert args.min_samples == 50
        assert args.dry_run is True


class TestRebuildFeatured:
    def test_parses(self):
        args = _parse_args(["rebuild-featured"])
        assert args.job == "rebuild-featured"

    def test_rebuild_calls_builder_and_saves(self, monkeypatch, tmp_path):
        """_rebuild_featured は _build_featured_data の結果を save_raw で保存する。"""
        import pandas as pd

        import src.pipeline._ingestion as ing
        import src.pipeline.run_pipeline as rp

        featured = pd.DataFrame({"x": [1, 2]}, index=["r1", "r1"])
        monkeypatch.setattr(rp, "_build_featured_data", lambda cfg: featured)
        saved = {}
        monkeypatch.setattr(ing, "save_raw", lambda df, path: saved.setdefault("df", df))
        monkeypatch.setattr(ing, "_save_featured_phase2", lambda df, cfg: saved.setdefault("phase2", True))

        rp._rebuild_featured(_parse_args(["rebuild-featured"]))
        assert saved["df"] is featured
        assert saved["phase2"] is True


class TestCalibrateTakeoutHandler:
    def _patch_common(self, monkeypatch):
        import pandas as pd

        import src.pipeline.run_pipeline as rp
        import src.policies._takeout_calibration as tc

        monkeypatch.setattr(rp, "_auto_migrate_db", lambda: None)
        results = pd.DataFrame({"馬番": [1, 2], "単勝": [2.0, 4.0]}, index=["r1", "r1"])
        monkeypatch.setattr(rp, "_load_raw_db_first", lambda alias, path: (results, "db"))
        monkeypatch.setattr(rp, "_return_processor_db_first", lambda: (object(), "db"))
        monkeypatch.setattr(tc, "tansho_odds_by_race_from_table", lambda *a, **k: {"r1": {1: 2.0, 2: 4.0}})
        monkeypatch.setattr(tc, "payout_lookup_from_return_processor", lambda rpobj: {("r1", "umaren", "1-2"): 5.0})
        monkeypatch.setattr(
            tc, "calibrate_takeout_from_payouts",
            lambda *a, **k: {"umaren": {"takeout": 0.22, "n": 30, "source": "calibrated"}},
        )
        return tc

    def test_saves_calibration(self, monkeypatch):
        import src.policies._takeout_calibration as tc

        self._patch_common(monkeypatch)
        saved = {}
        monkeypatch.setattr(tc, "save_takeout_calibration", lambda calib, path: saved.update(calib=calib, path=path))

        import src.pipeline.run_pipeline as rp
        rp._calibrate_takeout(_parse_args(["calibrate-takeout"]))
        assert "umaren" in saved["calib"]

    def test_dry_run_skips_save(self, monkeypatch):
        import src.policies._takeout_calibration as tc

        self._patch_common(monkeypatch)
        called = {"saved": False}
        monkeypatch.setattr(tc, "save_takeout_calibration", lambda *a, **k: called.update(saved=True))

        import src.pipeline.run_pipeline as rp
        rp._calibrate_takeout(_parse_args(["calibrate-takeout", "--dry-run"]))
        assert called["saved"] is False


class TestLoadRawDbFirst:
    def test_prefers_db_when_rows_present(self, monkeypatch):
        import pandas as pd

        import src.pipeline.run_pipeline as rp
        import src.storage as storage

        db_df = pd.DataFrame({"x": [1, 2, 3]})

        class _Repo:
            def has_rows(self, alias):
                return True

            def read(self, alias):
                return db_df

        monkeypatch.setattr(storage, "RawDataRepo", _Repo)
        df, src = rp._load_raw_db_first("raw_results", "/nonexistent.pkl")
        assert src == "db"
        assert len(df) == 3

    def test_falls_back_to_pickle_when_db_empty(self, monkeypatch):
        import pandas as pd

        import src.pipeline._ingestion as ing
        import src.pipeline.run_pipeline as rp
        import src.storage as storage

        class _Repo:
            def has_rows(self, alias):
                return False

            def read(self, alias):
                return pd.DataFrame()

        monkeypatch.setattr(storage, "RawDataRepo", _Repo)
        monkeypatch.setattr(ing, "load_raw", lambda path: pd.DataFrame({"y": [9]}))
        df, src = rp._load_raw_db_first("raw_results", "/whatever.pkl")
        assert src == "pickle"
        assert df["y"].tolist() == [9]


class TestBackfillNotes:
    """backfill-notes ジョブ: race_id 列の正規化・年代ゲート・取得済みスキップ。"""

    def _run(self, monkeypatch, results, training, argv):
        import src.pipeline._ingestion as ing
        import src.preparing._data_source as ds
        import src.pipeline.run_pipeline as rp
        from src.constants._local_paths import LocalPaths

        monkeypatch.setattr(
            ing, "load_raw",
            lambda p: training if p == LocalPaths.RAW_TRAINING_PATH else results,
        )
        called = {}

        class _Src:
            name = "netkeiba"

            def acquire_race_day_notes(self, ids):
                called["ids"] = list(ids)

        monkeypatch.setattr(ds, "create_data_source", lambda kind="netkeiba": _Src())
        monkeypatch.setattr(rp, "_resolve_data_source", lambda a: "netkeiba")
        rp._backfill_notes(rp._parse_args(argv))
        return called.get("ids")

    def test_race_id_as_column_is_normalized(self, monkeypatch):
        import pandas as pd

        # race_id を列に持つ RangeIndex（実 raw_results 形状。着順など他列を伴う）
        results = pd.DataFrame(
            {"race_id": [202606140511, 202606140511, 201005030611], "着順": [1, 2, 1]}
        )
        training = pd.DataFrame()  # 取得済みなし
        ids = self._run(monkeypatch, results, training, ["backfill-notes", "--min-year", "2010"])
        assert ids == ["201005030611", "202606140511"]

    def test_skip_existing_and_era_gate(self, monkeypatch):
        import pandas as pd

        results = pd.DataFrame(
            {"race_id": [202606140511, 201005030611, 199801010101], "着順": [1, 1, 1]}
        )
        training = pd.DataFrame({"y": [1]}, index=pd.Index(["202606140511"], name="race_id"))
        ids = self._run(monkeypatch, results, training, ["backfill-notes", "--min-year", "2010"])
        # 1998 は年代ゲート除外、2026 は取得済みスキップ → 2010 のみ
        assert ids == ["201005030611"]


class TestBackfillPeds:
    """backfill-peds: horse_id を raw_results 列から dedup 列挙・取得済みスキップ。"""

    def _run(self, monkeypatch, results, peds, argv):
        import src.pipeline._ingestion as ing
        import src.preparing._data_source as ds
        import src.pipeline.run_pipeline as rp
        from src.constants._local_paths import LocalPaths

        def fake_load(p):
            if p == LocalPaths.RAW_PEDS_PATH:
                return peds
            if p == LocalPaths.RAW_RESULTS_PATH:
                return results
            return __import__("pandas").DataFrame()

        monkeypatch.setattr(ing, "load_raw", fake_load)
        called = {}

        class _Src:
            name = "netkeiba"

            def acquire_peds(self, ids):
                called["ids"] = list(ids)

        monkeypatch.setattr(ds, "create_data_source", lambda kind="netkeiba": _Src())
        monkeypatch.setattr(rp, "_resolve_data_source", lambda a: "netkeiba")
        rp._backfill_peds(rp._parse_args(argv))
        return called.get("ids")

    def test_dedup_and_skip_existing(self, monkeypatch):
        import pandas as pd

        results = pd.DataFrame({"horse_id": ["A", "A", "B", "C"], "着順": [1, 2, 3, 1]})
        peds = pd.DataFrame({"p": [1]}, index=pd.Index(["C"], name="horse_id"))  # C 取得済み
        ids = self._run(monkeypatch, results, peds, ["backfill-peds"])
        assert ids == ["A", "B"]

    def test_no_skip_existing(self, monkeypatch):
        import pandas as pd

        results = pd.DataFrame({"horse_id": ["A", "C"], "着順": [1, 1]})
        peds = pd.DataFrame({"p": [1]}, index=pd.Index(["C"], name="horse_id"))
        ids = self._run(monkeypatch, results, peds, ["backfill-peds", "--no-skip-existing"])
        assert ids == ["A", "C"]
