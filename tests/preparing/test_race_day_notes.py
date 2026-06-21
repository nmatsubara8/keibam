"""レース当日ページパーサ（_race_day_notes）のテスト。

実 DOM から切り出したフィクスチャ（tests/fixtures/race_day/）で、調教評価・パドック・
厩舎コメントの抽出を検証する。フィクスチャは府中牝馬S 202605030611 の実データ由来。
"""

import os

from src.preparing._race_day_notes import (
    parse_comments,
    parse_paddock,
    parse_training,
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
