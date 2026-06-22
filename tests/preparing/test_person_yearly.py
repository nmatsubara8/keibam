"""人物年度別成績パーサ（_person_yearly）の単体テスト。"""

from __future__ import annotations

import pytest

from src.preparing._person_yearly import parse_person_yearly


def _row(cells):
    return "<tr>" + "".join(f"<td>{c}</td>" for c in cells) + "</tr>"


# 実 HTML（/jockey/result/<id>/ の 2026 行）を模した21列レイアウト
_HTML = (
    '<table class="race_table_01"><tr><th>年度</th><th>順位</th></tr>'
    + _row(["累計", "", "2205", "1578", "1153", "4868", "898", "179", "2607", "542",
            "6299", "1484", "5783", "1376", "4021", "829", ".225", ".386", ".503",
            "5,145,314.6", ""])
    + _row(["2026", "1", "91", "58", "45", "129", "35", "9", "91", "22", "197", "60",
            "189", "57", "134", "34", ".282", ".461", ".601", "233,807.7", "エンブロイダリー"])
    + "</table>"
)


class TestParsePersonYearly:
    def test_year_row_parsed(self):
        df = parse_person_yearly(_HTML, "jockey", "05339")
        assert len(df) == 1  # 累計・ヘッダは除外
        r = df.iloc[0]
        assert r["entity_type"] == "jockey" and r["entity_id"] == "05339"
        assert int(r["year"]) == 2026
        assert r["勝利数"] == 91
        assert r["出走回数"] == 91 + 58 + 45 + 129
        assert r["勝率"] == pytest.approx(0.282)
        assert r["複勝率"] == pytest.approx(0.601)
        assert r["芝勝率"] == pytest.approx(57 / 189)
        assert r["ダート勝率"] == pytest.approx(34 / 134)
        assert r["重賞勝利"] == 9
        assert r["収得賞金"] == pytest.approx(233807.7)

    def test_empty_and_no_table(self):
        assert parse_person_yearly("", "jockey", "1").empty
        assert parse_person_yearly("<html><body>no table</body></html>", "jockey", "1").empty
