"""予想家プロフィール由来スキル prior（_yoso_predictor）の単体テスト。"""

from __future__ import annotations

import pytest

from src.preparing._yoso_predictor import parse_yoso_predictor


def _row(c):
    return "<tr>" + "".join(f"<td>{x}</td>" for x in c) + "</tr>"


_HTML = (
    '<table class="nk_tb_common"><tr><th>日付</th><th>場名</th><th>R</th>'
    "<th>レース名</th><th>結果</th><th>的中配当</th><th>◎の成績</th></tr>"
    + _row(["6/21", "函館", "12R", "x", "", "", "マイネルモメンタム 8着(6番人気)"])
    + _row(["6/21", "函館", "10R", "y", "的中", "馬単 480円", "チャーリー 1着(1番人気)"])
    + _row(["6/21", "函館", "8R", "z", "的中", "馬単 3,030円", "シーズ 2着(1番人気)"])
    + "</table>"
)


class TestParseYosoPredictor:
    def test_aggregate(self):
        df = parse_yoso_predictor(_HTML, "266994")
        assert len(df) == 1
        r = df.iloc[0]
        assert r["predictor_yid"] == "266994"
        assert r["profile_n"] == 3
        assert r["profile_honmei_winrate"] == pytest.approx(1 / 3)  # ◎着順 8,1,2 → 1着は1つ
        assert r["profile_honmei_pkrate"] == pytest.approx(2 / 3)   # 3着内は1,2
        assert r["profile_hit_rate"] == pytest.approx(2 / 3)        # 的中2/3
        assert r["profile_avg_return"] == pytest.approx((480 + 3030) / 2)

    def test_empty_and_no_table(self):
        assert parse_yoso_predictor("", "1").empty
        assert parse_yoso_predictor("<html>no</html>", "1").empty
