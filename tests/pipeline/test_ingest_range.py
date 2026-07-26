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
    def _patch(self, monkeypatch, race_day_set, ingested):
        """開催プローブと1日取込を差し替える。

        race_day_set に含まれる日は「開催あり」、それ以外は「開催なし」として
        扱い、実際に取込まれた日を ingested に記録する。
        """
        race_day_set = set(race_day_set)
        monkeypatch.setattr(
            ingest_range, "_probe_race_ids",
            lambda ymd8: ["r1", "r2"] if ymd8 in race_day_set else [],
        )

        def fake_ingest(ymd8):
            ingested.append(ymd8)
            return True

        monkeypatch.setattr(ingest_range, "_ingest_one_day", fake_ingest)

    def test_inclusive_bounds_ingests_both_ends(self, tmp_path, monkeypatch):
        # ② from(=20260721) と to(=20260726) の両端を含む。開催日は両端のみ。
        ingested: list[str] = []
        self._patch(monkeypatch, ["20260721", "20260726"], ingested)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            ["--from", "20260721", "--to", "20260726", "--resume-file", str(rf)]
        )
        assert rc == 0
        assert ingested == ["20260721", "20260726"]
        assert ingest_range._load_done(rf) == {"20260721", "20260726"}

    def test_non_race_days_are_skipped_and_not_recorded(self, tmp_path, monkeypatch):
        # 開催なしの日は取込まず resume にも記録しない（次回再確認される）
        ingested: list[str] = []
        self._patch(monkeypatch, ["20260726"], ingested)  # 開催は 26 のみ
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            ["--from", "20260721", "--to", "20260726", "--resume-file", str(rf)]
        )
        assert rc == 0
        assert ingested == ["20260726"]
        assert ingest_range._load_done(rf) == {"20260726"}

    def test_already_done_days_are_skipped(self, tmp_path, monkeypatch):
        # ③ resume に記録済みの日は再取込しない（プローブもしない）
        ingested: list[str] = []
        self._patch(monkeypatch, ["20260721", "20260726"], ingested)
        rf = tmp_path / "resume.txt"
        rf.write_text("20260721\n", encoding="utf-8")
        rc = ingest_range.main(
            ["--from", "20260721", "--to", "20260726", "--resume-file", str(rf)]
        )
        assert rc == 0
        assert ingested == ["20260726"]  # 21 はスキップされ 26 のみ

    def test_failed_day_not_marked_done_and_returns_1(self, tmp_path, monkeypatch):
        monkeypatch.setattr(ingest_range, "_probe_race_ids", lambda ymd8: ["r1"])
        monkeypatch.setattr(ingest_range, "_ingest_one_day", lambda ymd8: False)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            ["--from", "20260721", "--to", "20260721", "--resume-file", str(rf)]
        )
        assert rc == 1
        assert ingest_range._load_done(rf) == set()  # 失敗日は記録されない

    def test_list_only_does_not_ingest(self, tmp_path, monkeypatch):
        ingested: list[str] = []
        self._patch(monkeypatch, ["20260721"], ingested)
        rf = tmp_path / "resume.txt"
        rc = ingest_range.main(
            ["--from", "20260721", "--to", "20260721", "--list-only", "--resume-file", str(rf)]
        )
        assert rc == 0
        assert ingested == []
