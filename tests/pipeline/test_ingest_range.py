"""scripts/ingest_range.py の純粋ロジック検証。

ネットワーク（開催カレンダー取得）と subprocess（1日分の取込）は monkeypatch で
差し替え、日付正規化・resume スキップ・両端含む範囲フィルタのみを検証する。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# scripts/ はパッケージではないため importlib で直接ロードする
_MODULE_PATH = Path(__file__).parents[2] / "scripts" / "ingest_range.py"
_spec = importlib.util.spec_from_file_location("ingest_range", _MODULE_PATH)
ingest_range = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ingest_range)


# ---------------------------------------------------------------------------
# 日付ユーティリティ
# ---------------------------------------------------------------------------

class TestDateUtils:
    def test_to_ymd8_accepts_hyphenated(self):
        assert ingest_range._to_ymd8("2026-07-21") == "20260721"

    def test_to_ymd8_accepts_compact(self):
        assert ingest_range._to_ymd8("20260721") == "20260721"

    def test_to_ymd8_rejects_garbage(self):
        with pytest.raises(ValueError):
            ingest_range._to_ymd8("2026-13-99")

    def test_plus_one_day_crosses_month(self):
        assert ingest_range._plus_one_day("20260731") == "20260801"

    def test_calendar_days_inclusive_both_ends(self):
        # ② from と to の両端を含む
        assert ingest_range._calendar_days("20260721", "20260724") == [
            "20260721", "20260722", "20260723", "20260724",
        ]

    def test_calendar_days_single_day(self):
        assert ingest_range._calendar_days("20260721", "20260721") == ["20260721"]


# ---------------------------------------------------------------------------
# resume ファイル
# ---------------------------------------------------------------------------

class TestResumeFile:
    def test_load_missing_returns_empty(self, tmp_path):
        assert ingest_range._load_done(tmp_path / "nope.txt") == set()

    def test_mark_and_load_roundtrip(self, tmp_path):
        rf = tmp_path / "resume.txt"
        ingest_range._mark_done(rf, "20260721")
        ingest_range._mark_done(rf, "20260722")
        assert ingest_range._load_done(rf) == {"20260721", "20260722"}

    def test_load_ignores_non_date_lines(self, tmp_path):
        rf = tmp_path / "resume.txt"
        rf.write_text("20260721\n# comment\n\nbad\n20260722\n", encoding="utf-8")
        assert ingest_range._load_done(rf) == {"20260721", "20260722"}


# ---------------------------------------------------------------------------
# main(): 両端含む範囲 + resume スキップ + 成功時のみ done 記録
# ---------------------------------------------------------------------------

class TestMain:
    def _patch(self, monkeypatch, race_day_set, ingested, ok=True):
        """開催プローブと ingest を差し替える。

        race_day_set に含まれる日は「開催あり」（race_id を返す）、それ以外は
        「開催なし」として扱う。ingest 呼び出しごとに渡された race_id 群を
        ingested に記録する（バッチ時は1回、per-day 時は日数分）。
        """
        race_day_set = set(race_day_set)
        monkeypatch.setattr(
            ingest_range, "_probe_race_ids",
            lambda ymd8: [f"{ymd8}01", f"{ymd8}02"] if ymd8 in race_day_set else [],
        )

        def fake_ingest(race_ids, label):
            ingested.append(list(race_ids))
            return ok

        monkeypatch.setattr(ingest_range, "_ingest_race_ids", fake_ingest)

    def _base_args(self, rf, extra=None):
        # --sleep-between 0 で実スリープを避ける
        args = ["--resume-file", str(rf), "--sleep-between", "0"]
        return args + (extra or [])

    def test_inclusive_bounds_ingests_both_ends(self, tmp_path, monkeypatch):
        # ② from(=20260721) と to(=20260726) の両端を含む。開催日は両端のみ。
        # 既定（バッチ）: 両日の race_id をまとめて1回 ingest。
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260721", "20260726"], ingested)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(self._base_args(rf, ["--from", "20260721", "--to", "20260726"]))
        assert rc == 0
        assert ingested == [["2026072101", "2026072102", "2026072601", "2026072602"]]
        assert ingest_range._load_done(rf) == {"20260721", "20260726"}

    def test_per_day_calls_ingest_per_race_day(self, tmp_path, monkeypatch):
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260721", "20260726"], ingested)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            self._base_args(rf, ["--from", "20260721", "--to", "20260726", "--per-day"])
        )
        assert rc == 0
        # 日ごとに1回ずつ、その日の race_id のみ
        assert ingested == [["2026072101", "2026072102"], ["2026072601", "2026072602"]]
        assert ingest_range._load_done(rf) == {"20260721", "20260726"}

    def test_non_race_days_are_skipped_and_not_recorded(self, tmp_path, monkeypatch):
        # 開催なしの日は取込まず resume にも記録しない（次回再確認される）
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260726"], ingested)  # 開催は 26 のみ
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(self._base_args(rf, ["--from", "20260721", "--to", "20260726"]))
        assert rc == 0
        assert ingested == [["2026072601", "2026072602"]]
        assert ingest_range._load_done(rf) == {"20260726"}

    def test_already_done_days_are_skipped(self, tmp_path, monkeypatch):
        # ③ resume に記録済みの日は再取込しない（プローブもしない）
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260721", "20260726"], ingested)
        rf = tmp_path / "resume.txt"
        rf.write_text("20260721\n", encoding="utf-8")
        rc = ingest_range.main(self._base_args(rf, ["--from", "20260721", "--to", "20260726"]))
        assert rc == 0
        assert ingested == [["2026072601", "2026072602"]]  # 21 はスキップ
        assert ingest_range._load_done(rf) == {"20260721", "20260726"}

    def test_failed_ingest_not_marked_done_and_returns_1(self, tmp_path, monkeypatch):
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260721"], ingested, ok=False)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(self._base_args(rf, ["--from", "20260721", "--to", "20260721"]))
        assert rc == 1
        assert ingest_range._load_done(rf) == set()  # 失敗日は記録されない

    def test_list_only_does_not_ingest(self, tmp_path, monkeypatch):
        ingested: list[list[str]] = []
        self._patch(monkeypatch, ["20260721"], ingested)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            self._base_args(rf, ["--from", "20260721", "--to", "20260721", "--list-only"])
        )
        assert rc == 0
        assert ingested == []
