"""HTML 構造要約（_html_structure）の単体テスト。

合成 HTML で table 抽出・id 抽出・プレミアム検出・レポート整形を検証する。
"""

from src.preparing._html_structure import (
    find_element_ids,
    find_premium_markers,
    structure_report,
    summarize_tables,
)

_SAMPLE = """
<html><body>
<table summary="調教タイム" class="OikiriTable">
  <tr><th>馬名</th><th>評価</th><th>映像</th></tr>
  <tr><td>マカナ</td><td>叩き良化</td><td>B</td></tr>
  <tr><td>ウイントワイライト</td><td>好気配</td><td>B</td></tr>
</table>
<table summary="ラップタイム">
  <caption>ラップタイム</caption>
  <tr><td>ラップ</td><td>12.2 - 10.8</td></tr>
</table>
<table class="layout"><tr><td>広告</td></tr></table>
<div id="odds-1_07">10.2</div>
<div id="odds-2_07">2.3</div>
<p>続きはプレミアムコース登録でご覧になれます。<a>ログイン</a></p>
</body></html>
"""


class TestSummarizeTables:
    def test_extracts_summary_and_headers(self):
        tables = summarize_tables(_SAMPLE)
        oikiri = next(t for t in tables if t["summary"] == "調教タイム")
        assert oikiri["class"] == "OikiriTable"
        assert oikiri["headers"] == ["馬名", "評価", "映像"]
        assert oikiri["sample_row"][:2] == ["マカナ", "叩き良化"]
        assert oikiri["n_cols"] == 3

    def test_caption_captured(self):
        tables = summarize_tables(_SAMPLE)
        lap = next(t for t in tables if t["summary"] == "ラップタイム")
        assert lap["caption"] == "ラップタイム"

    def test_counts_all_tables(self):
        assert len(summarize_tables(_SAMPLE)) == 3


class TestFindElementIds:
    def test_odds_ids_dedup_ordered(self):
        ids = find_element_ids(_SAMPLE, r"^odds-")
        assert ids == ["odds-1_07", "odds-2_07"]

    def test_no_match(self):
        assert find_element_ids(_SAMPLE, r"^nope-") == []


class TestPremiumMarkers:
    def test_detects_premium_and_login(self):
        prem = find_premium_markers(_SAMPLE)
        assert prem.get("プレミアム", 0) >= 1
        assert prem.get("ログイン", 0) >= 1
        assert prem.get("続きは", 0) >= 1

    def test_clean_html_no_markers(self):
        assert find_premium_markers("<table><tr><td>x</td></tr></table>") == {}


class TestStructureReport:
    def test_report_contains_key_sections(self):
        rep = structure_report(_SAMPLE, url="http://example/oikiri", min_rows=2)
        assert "http://example/oikiri" in rep
        assert "調教タイム" in rep
        assert "odds-1_07" in rep
        assert "プレミアム" in rep

    def test_min_rows_filters_small_tables(self):
        # layout テーブル(1行)は min_rows=2 で除外され、調教/ラップのみ残る
        rep = structure_report(_SAMPLE, min_rows=2)
        assert "広告" not in rep

    def test_handles_empty_html(self):
        rep = structure_report("", url="x")
        assert "html長 : 0" in rep
