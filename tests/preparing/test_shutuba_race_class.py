"""出馬表ヘッダのレースクラス判定（_scrape_shutuba._parse_race_header）のテスト。

現レース（ライブ予測）側の格取得が、学習側と同じく classify_race_class で頑健化され、
GⅢ/GIII(全角・半角ローマ数字)・(L)・全角数字の条件戦を取りこぼさないことを検証する。
"""

from bs4 import BeautifulSoup

from src.constants._master import Master
from src.preparing._scrape_shutuba import (
    _parse_race_header,
    _race_list_sub_url,
    _shutuba_url,
)


def test_shutuba_url_routes_by_organizer():
    # 中央（場コード 01）は race.netkeiba.com、地方（門別30）は nar.netkeiba.com
    assert _shutuba_url("202405010101").startswith("https://race.netkeiba.com/race/shutuba.html")
    assert _shutuba_url("202630072201").startswith("https://nar.netkeiba.com/race/shutuba.html")
    assert "race_id=202630072201" in _shutuba_url("202630072201")


def test_race_list_sub_url_routes_by_organizer():
    # 既定（中央）は race.netkeiba.com、organizer=local で nar.netkeiba.com
    assert _race_list_sub_url("20240501").startswith("https://race.netkeiba.com/top/race_list_sub.html")
    assert _race_list_sub_url("20260722", "local").startswith(
        "https://nar.netkeiba.com/top/race_list_sub.html"
    )
    assert "kaisai_date=20260722" in _race_list_sub_url("20260722", "local")


def _soup(race_name: str, data02: str = "") -> BeautifulSoup:
    html = (
        '<div class="RaceList_Item02">'
        f'<div class="RaceName">{race_name}</div>'
        '<div class="RaceData01">芝1600m / 天候:晴 / 馬場:良</div>'
        f'<div class="RaceData02">{data02}</div>'
        "</div>"
    )
    return BeautifulSoup(html, "lxml")


def _race_class(race_name: str, data02: str = ""):
    return _parse_race_header(_soup(race_name, data02), "202501010101")["race_class"]


class TestShutubaRaceClass:
    def test_fullwidth_roman_grade(self):
        assert _race_class("日経賞(GⅡ)") == Master.RACE_CLASS_G2
        assert _race_class("京都金杯(GⅢ)") == Master.RACE_CLASS_G3

    def test_halfwidth_latin_grade(self):
        assert _race_class("レース(GIII)") == Master.RACE_CLASS_G3
        assert _race_class("レース(G1)") == Master.RACE_CLASS_G1

    def test_listed(self):
        # 以前は (L) を取りこぼしていた（リステッドを認識すべき）
        assert _race_class("アイビスサマーダッシュ(L)") == Master.RACE_CLASS_LISTED

    def test_fullwidth_digit_condition(self):
        # 全角数字「１勝クラス」も NFKC で吸収
        assert _race_class("３歳以上１勝クラス") == Master.RACE_CLASS_1SHO

    def test_condition_in_data02(self):
        assert _race_class("特別戦", data02="ダ1800m 3歳未勝利") == Master.RACE_CLASS_MISHORI

    def test_legacy_class(self):
        assert _race_class("条件戦", data02="500万下") == Master.RACE_CLASS_1SHO

    def test_unknown_stays_nan(self):
        # 判定不能なら既定の NaN を維持（上書きしない）
        val = _race_class("ただのレース")
        assert val != val  # NaN
