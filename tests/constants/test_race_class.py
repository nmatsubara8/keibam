"""レースクラス正規化（src.constants._race_class）のテスト。

取りこぼし対策の中核: 全角ローマ数字グレード(GⅠ/GⅡ/GⅢ)・(L)・全角数字の条件戦・
旧称(500万下)を正しく正準クラスへ写像できることを検証する。
"""

from src.constants._master import Master
from src.constants._master import classify_race_class
from src.constants._master import race_class_level


class TestClassifyGrade:
    def test_fullwidth_roman_grades(self):
        # ユーザー指摘: GⅠ/GⅡ/GⅢ（全角ローマ数字）
        assert classify_race_class("天皇賞(秋)(GⅠ)") == Master.RACE_CLASS_G1
        assert classify_race_class("宝塚記念(GⅡ)") == Master.RACE_CLASS_G2
        assert classify_race_class("京都金杯(GⅢ)") == Master.RACE_CLASS_G3

    def test_halfwidth_arabic_grades(self):
        assert classify_race_class("レース(G1)") == Master.RACE_CLASS_G1
        assert classify_race_class("レース(G2)") == Master.RACE_CLASS_G2
        assert classify_race_class("レース(G3)") == Master.RACE_CLASS_G3

    def test_halfwidth_latin_roman_grades(self):
        # 半角ラテン I のローマ数字表記（GI/GII/GIII）
        assert classify_race_class("レース(GI)") == Master.RACE_CLASS_G1
        assert classify_race_class("レース(GII)") == Master.RACE_CLASS_G2
        assert classify_race_class("レース(GIII)") == Master.RACE_CLASS_G3

    def test_fullwidth_alnum_grade(self):
        # Ｇ３（全角英数）→ NFKC → G3
        assert classify_race_class("レースＧ３") == Master.RACE_CLASS_G3

    def test_jpn_grade(self):
        assert classify_race_class("帝王賞(Jpn1)") == Master.RACE_CLASS_G1
        assert classify_race_class("レース(JpnⅢ)") == Master.RACE_CLASS_G3

    def test_grade_without_parens(self):
        assert classify_race_class("重賞 G1 レース") == Master.RACE_CLASS_G1

    def test_no_false_positive_in_words(self):
        # "GI" を含む単語（例: 英字略称）で誤検出しない
        assert classify_race_class("GIRL記念") is None
        assert classify_race_class("BIGINNING") is None


class TestClassifyListed:
    def test_listed_in_name(self):
        # ユーザー指摘: レース名に (L) が含まれるもの
        assert classify_race_class("アイビスサマーダッシュ(L)") == Master.RACE_CLASS_LISTED

    def test_listed_fullwidth_paren(self):
        assert classify_race_class("レース（Ｌ）") == Master.RACE_CLASS_LISTED

    def test_bare_L_not_matched(self):
        # 括弧なしの裸の "L" は誤検出しない（単語内 L を避ける）
        assert classify_race_class("LONDON記念") is None


class TestClassifyCondition:
    def test_shinba_mishori(self):
        assert classify_race_class("サラ系3歳新馬") == Master.RACE_CLASS_SHINBA
        assert classify_race_class("3歳未勝利") == Master.RACE_CLASS_MISHORI

    def test_win_classes_halfwidth(self):
        assert classify_race_class("3歳以上1勝クラス") == Master.RACE_CLASS_1SHO
        assert classify_race_class("2勝クラス") == Master.RACE_CLASS_2SHO
        assert classify_race_class("3勝クラス") == Master.RACE_CLASS_3SHO

    def test_win_classes_fullwidth_digit(self):
        # 全角数字「１勝クラス」も NFKC で吸収
        assert classify_race_class("３歳以上１勝クラス") == Master.RACE_CLASS_1SHO
        assert classify_race_class("２勝クラス") == Master.RACE_CLASS_2SHO

    def test_legacy_classes(self):
        assert classify_race_class("500万下") == Master.RACE_CLASS_1SHO
        assert classify_race_class("1000万下") == Master.RACE_CLASS_2SHO
        assert classify_race_class("900万下") == Master.RACE_CLASS_2SHO
        assert classify_race_class("1600万下") == Master.RACE_CLASS_3SHO

    def test_open(self):
        assert classify_race_class("オープン") == Master.RACE_CLASS_OPEN
        assert classify_race_class("オープン特別") == Master.RACE_CLASS_OPEN_SPECIAL

    def test_grade_priority_over_open(self):
        # オープン特別だが (GⅢ) ならグレード優先
        assert classify_race_class("オープン特別(GⅢ)") == Master.RACE_CLASS_G3


class TestUnknownAndMissing:
    def test_unknown_returns_none(self):
        assert classify_race_class("ただのレース名") is None

    def test_missing(self):
        assert classify_race_class(None) is None
        assert classify_race_class("") is None
        assert classify_race_class(float("nan")) is None


class TestRaceClassLevel:
    def test_canonical_levels_ordered(self):
        lv = race_class_level
        assert lv(Master.RACE_CLASS_SHINBA) == 1
        assert lv(Master.RACE_CLASS_MISHORI) == 1
        assert lv(Master.RACE_CLASS_1SHO) == 2
        assert lv(Master.RACE_CLASS_2SHO) == 3
        assert lv(Master.RACE_CLASS_3SHO) == 4
        assert lv(Master.RACE_CLASS_OPEN) == 5
        assert lv(Master.RACE_CLASS_LISTED) == 6
        assert lv(Master.RACE_CLASS_G3) == 7
        assert lv(Master.RACE_CLASS_G2) == 8
        assert lv(Master.RACE_CLASS_G1) == 9

    def test_listed_between_open_and_g3(self):
        lv = race_class_level
        assert lv(Master.RACE_CLASS_OPEN) < lv(Master.RACE_CLASS_LISTED) < lv(Master.RACE_CLASS_G3)

    def test_from_raw_name(self):
        # 生のレース名/旧称からも順序値を引ける
        assert race_class_level("京都金杯(GⅢ)") == 7
        assert race_class_level("500万下") == 2
        assert race_class_level("アイビスSD(L)") == 6

    def test_unknown_none(self):
        assert race_class_level(None) is None
        assert race_class_level(float("nan")) is None
        assert race_class_level("謎のクラス") is None
