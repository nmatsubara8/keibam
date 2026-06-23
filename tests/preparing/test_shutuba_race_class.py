"""出馬表ヘッダのレースクラス判定（_scrape_shutuba._parse_race_header）のテスト。

現レース（ライブ予測）側の格取得が、学習側と同じく classify_race_class で頑健化され、
GⅢ/GIII(全角・半角ローマ数字)・(L)・全角数字の条件戦を取りこぼさないことを検証する。
"""

from bs4 import BeautifulSoup

from src.constants._master import Master
from src.preparing._scrape_shutuba import _parse_race_header


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
