"""レース当日ページパーサ（_race_day_notes）のテスト。

実 DOM から切り出したフィクスチャ（tests/fixtures/race_day/）で、調教評価・パドック・
厩舎コメントの抽出を検証する。フィクスチャは府中牝馬S 202605030611 の実データ由来。
"""

import os

from src.preparing._race_day_notes import (
    RaceDayNotesScraper,
    parse_comments,
    parse_paddock,
    parse_training,
    persist_notes,
)

_FIX = os.path.join(os.path.dirname(__file__), "..", "fixtures", "race_day")


def _read(name: str) -> str:
    with open(os.path.join(_FIX, name), encoding="utf-8") as f:
        return f.read()


class TestTraining:
    def test_extracts_eval_and_grade(self):
        df = parse_training(_read("oikiri.html"), "202605030611").set_index("馬番")
        assert len(df) == 16  # 全頭
        assert df.loc[1, "horse_id"] == "2022100019"
        assert df.loc[1, "調教評価"] == "叩き良化"
        assert df.loc[1, "映像グレード"] == "B"  # 映像グレードが全行取れる（旧バグの回帰）
        assert df["映像グレード"].notna().all()

    def test_race_id_and_umaban_types(self):
        df = parse_training(_read("oikiri.html"), "202605030611")
        assert (df["race_id"] == "202605030611").all()
        assert df["馬番"].map(type).eq(int).all()

    def test_empty_html_returns_typed_empty(self):
        df = parse_training("<html></html>", "R")
        assert df.empty
        assert list(df.columns) == ["race_id", "馬番", "horse_id", "調教評価", "映像グレード"]


class TestPaddock:
    def test_sparse_attention_horses_only(self):
        df = parse_paddock(_read("paddock.html"), "202605030611").set_index("馬番")
        # 注目馬のみ（全16頭ではなく7頭）
        assert len(df) == 7
        assert set(df.index) == {5, 6, 7, 8, 9, 12, 15}

    def test_grade_and_comment(self):
        df = parse_paddock(_read("paddock.html"), "202605030611").set_index("馬番")
        assert df.loc[6, "パドック評価"] == "A"
        assert df.loc[9, "パドック評価"] == "穴"
        assert "馬体" in df.loc[6, "パドックコメント"] or df.loc[6, "パドックコメント"]
        assert df.loc[5, "horse_id"].isdigit()

    def test_empty_returns_typed_empty(self):
        df = parse_paddock("<html></html>", "R")
        assert df.empty
        assert "パドック評価" in df.columns


class TestComments:
    def test_extracts_stable_comments(self):
        df = parse_comments(_read("comment.html"), "202605030611").set_index("馬番")
        assert set(df.index) == {1, 2, 3}
        assert df.loc[1, "horse_id"] == "2022100019"
        assert "前走" in df.loc[1, "厩舎コメント"]

    def test_empty_returns_typed_empty(self):
        df = parse_comments("<html></html>", "R")
        assert df.empty
        assert "厩舎コメント" in df.columns


class _FakeScraper:
    """fetch_sync をフィクスチャ HTML / 例外に差し替えるダミー。"""

    def __init__(self, html_by_selector=None, raise_exc=False):
        self._map = html_by_selector or {}
        self._raise = raise_exc
        self.calls = []

    def fetch_sync(self, url, wait_selector=None):
        self.calls.append((url, wait_selector))
        if self._raise:
            raise RuntimeError("network down")
        return self._map.get(wait_selector, "")


class TestScraper:
    def test_capture_parses_each_type(self):
        scraper = RaceDayNotesScraper(
            scraper=_FakeScraper(
                {
                    "table.OikiriTable": _read("oikiri.html"),
                    "table.Paddock_Table": _read("paddock.html"),
                    "#All_Comment_Table": _read("comment.html"),
                }
            )
        )
        assert len(scraper.capture("R", "training")) == 16
        assert len(scraper.capture("R", "paddock")) == 7
        assert len(scraper.capture("R", "comment")) == 3

    def test_capture_builds_correct_url(self):
        fake = _FakeScraper({"table.OikiriTable": _read("oikiri.html")})
        RaceDayNotesScraper(scraper=fake).capture("202605030611", "training")
        url, sel = fake.calls[0]
        assert "oikiri.html?race_id=202605030611" in url
        assert sel == "table.OikiriTable"

    def test_capture_returns_empty_on_fetch_failure(self):
        scraper = RaceDayNotesScraper(scraper=_FakeScraper(raise_exc=True))
        df = scraper.capture("R", "training")
        assert df.empty
        assert "調教評価" in df.columns  # 型付き空（例外を投げない）

    def test_unknown_note_type_raises(self):
        import pytest

        with pytest.raises(ValueError):
            RaceDayNotesScraper(scraper=_FakeScraper()).capture("R", "bogus")


class TestPersist:
    def test_persist_sets_race_id_index(self, tmp_path, monkeypatch):
        captured = {}

        def fake_update(path, df):
            captured["path"] = path
            captured["df"] = df

        monkeypatch.setattr(
            "src.preparing._get_rawdata.update_rawdata", fake_update, raising=True
        )
        df = parse_training(_read("oikiri.html"), "202605030611")
        n = persist_notes(df, str(tmp_path / "training.pkl"))
        assert n == 16
        assert captured["df"].index.name == "race_id"
        assert "馬番" in captured["df"].columns

    def test_persist_empty_is_noop(self):
        import pandas as pd

        assert persist_notes(pd.DataFrame(), "x.pkl") == 0


class TestRegistration:
    def test_aliases_registered(self):
        from src.constants._local_paths import LocalPaths
        from src.storage._db import PICKLE_PATH_TO_ALIAS, TABLE_SPECS

        for alias, path in (
            ("raw_training", LocalPaths.RAW_TRAINING_PATH),
            ("raw_paddock", LocalPaths.RAW_PADDOCK_PATH),
            ("raw_comment", LocalPaths.RAW_COMMENT_PATH),
        ):
            assert PICKLE_PATH_TO_ALIAS.get(path) == alias
            assert TABLE_SPECS[alias].primary_key == ("race_id", "馬番")
            assert TABLE_SPECS[alias].index_col == "race_id"

    def test_catalog_has_comment_and_umaban(self):
        from src.constants import _field_catalog as fc

        for t in ("raw_training", "raw_paddock", "raw_comment"):
            assert "馬番" in fc.columns(t)


class TestJoinKeyConsistency:
    def test_all_keyed_by_race_id_umaban(self):
        # 3 ソースとも (race_id, 馬番) を持ち results に結合できる
        for df in (
            parse_training(_read("oikiri.html"), "R"),
            parse_paddock(_read("paddock.html"), "R"),
            parse_comments(_read("comment.html"), "R"),
        ):
            assert {"race_id", "馬番"}.issubset(df.columns)
            assert not df.empty
            # (race_id, 馬番) は一意
            assert not df.duplicated(["race_id", "馬番"]).any()
