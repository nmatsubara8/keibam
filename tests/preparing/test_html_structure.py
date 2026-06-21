"""HTML 構造要約（_html_structure）の単体テスト。

合成 HTML で table 抽出・id 抽出・プレミアム検出・レポート整形を検証する。
"""

from src.preparing._html_structure import (
    find_element_ids,
    find_premium_markers,
    structure_report,
    summarize_images,
    summarize_repeated_containers,
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


# 予想印のような div/カード型ページ（table を使わない）の合成サンプル
_CARDS = """
<html><body>
<div class="YosoList">
  <div class="YosoItem"><span class="name">予想家A</span> ◎7 ○3 ▲5</div>
  <div class="YosoItem"><span class="name">予想家B</span> ◎3 ○7 ▲1</div>
  <div class="YosoItem"><span class="name">本紙</span> ◎7 ○5 ▲3</div>
  <div class="YosoItem"><span class="name">AI予想</span> ◎7 ○3 ▲11</div>
</div>
<ul class="nav"><li class="tab">A</li><li class="tab">B</li></ul>
</body></html>
"""


class TestRepeatedContainers:
    def test_detects_card_list(self):
        cs = summarize_repeated_containers(_CARDS, min_repeat=3)
        yoso = next(c for c in cs if c["child_class"] == "YosoItem")
        assert yoso["count"] == 4
        assert yoso["parent_class"] == "YosoList"
        assert yoso["child_tag"] == "div"
        assert "予想家A" in yoso["sample"]

    def test_min_repeat_filters_small_groups(self):
        # nav の li は 2 個 → min_repeat=3 で除外
        cs = summarize_repeated_containers(_CARDS, min_repeat=3)
        assert all(c["child_class"] != "tab" for c in cs)

    def test_report_includes_containers_for_tableless_page(self):
        rep = structure_report(_CARDS, url="http://example/yoso", min_rows=2)
        assert "繰り返しコンテナ" in rep
        assert "YosoItem" in rep


# 予想印がアイコン画像で表現されるケース
_MARK_IMGS = """
<html><body>
<div class="YosoTableWrap">
  <span class="mark"><img src="/m1.png" alt="◎"></span>
  <span class="mark"><img src="/m2.png" alt="○"></span>
  <span class="mark"><img src="/m1.png" alt="◎"></span>
  <span class="mark"><img src="/m3.png" alt="▲"></span>
  <img src="/logo.png" alt="netkeiba">
</div>
</body></html>
"""


class TestSummarizeImages:
    def test_counts_alt_values(self):
        imgs = dict(summarize_images(_MARK_IMGS))
        assert imgs["◎"] == 2
        assert imgs["○"] == 1
        assert imgs["▲"] == 1

    def test_report_surfaces_mark_icons(self):
        rep = structure_report(_MARK_IMGS, url="http://example/yoso")
        assert "印アイコン alt" in rep
        assert "◎" in rep


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
