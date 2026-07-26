"""レースクラス分類網羅性の監査（src.preprocessing._race_class_audit）のテスト。"""

from src.constants._master import Master
from src.preprocessing._race_class_audit import class_distribution
from src.preprocessing._race_class_audit import classify_with_level
from src.preprocessing._race_class_audit import coverage_summary
from src.preprocessing._race_class_audit import unclassified_counts


class TestCoverageSummary:
    def test_counts_and_coverage(self):
        values = [
            "3歳新馬", "3歳未勝利", "天皇賞(GⅠ)",  # 分類可(3)
            "謎クラスA", "謎クラスA",               # 判定不能(2・重複)
            None, "", "  ",                          # 欠損(3)
        ]
        s = coverage_summary(values)
        assert s["total"] == 8
        assert s["blank"] == 3
        assert s["evaluated"] == 5
        assert s["classified"] == 3
        assert s["unclassified"] == 2
        assert s["coverage"] == 3 / 5

    def test_empty(self):
        s = coverage_summary([])
        assert s["total"] == 0 and s["coverage"] == 0.0


class TestUnclassifiedCounts:
    def test_distinct_with_counts_desc(self):
        values = ["謎A", "謎A", "謎B", "3歳未勝利", None, ""]
        c = unclassified_counts(values)
        assert c["謎A"] == 2
        assert c["謎B"] == 1
        assert "3歳未勝利" not in c  # 分類可は含めない
        assert c.most_common(1)[0] == ("謎A", 2)

    def test_blanks_excluded(self):
        assert unclassified_counts([None, "", "   ", float("nan")]) == {}


class TestClassDistribution:
    def test_distribution_includes_none(self):
        values = ["G1レース(G1)", "3歳未勝利", "3歳未勝利", "謎"]
        d = class_distribution(values)
        assert d[Master.RACE_CLASS_G1] == 1
        assert d[Master.RACE_CLASS_MISHORI] == 2
        assert d[None] == 1  # 判定不能


class TestClassifyWithLevel:
    def test_pair(self):
        assert classify_with_level("京都金杯(GⅢ)") == (Master.RACE_CLASS_G3, 7)
        assert classify_with_level("謎") == (None, None)


class TestListedLiteralRule:
    def test_listed_text_now_classified(self):
        # (L) だけでなく文字列「リステッド」も分類できる（網羅性補強）
        assert classify_with_level("リステッド競走")[0] == Master.RACE_CLASS_LISTED
