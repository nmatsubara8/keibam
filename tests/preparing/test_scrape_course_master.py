"""Phase 9: コース形状マスタ スクレイパの解析ロジックのテスト（実フェッチなし）。"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.scrape_course_master import build_row, parse_course_page

_HTML = """
<html><body>
<h1>東京 芝1400m</h1>
<p>ゴール前の直線距離は 525.9m と長い。最大高低差は約 2.5m。</p>
<p>スタートから第1コーナーまでの距離は約 500m。ゴール前には上り坂がある。</p>
</body></html>
"""

_HTML_NO_HILL = """
<html><body>直線 310m。高低差 1.8m。1コーナーまで 400m。平坦なコース。</body></html>
"""

_HTML_EMPTY = "<html><body>準備中</body></html>"


class TestParseCoursePage:
    def test_extracts_numbers(self):
        r = parse_course_page(_HTML)
        assert r["straight_length"] == 525.9
        assert r["elevation_diff"] == 2.5
        assert r["first_corner_dist"] == 500.0
        assert r["has_final_hill"] == 1.0

    def test_no_hill_but_has_elev(self):
        r = parse_course_page(_HTML_NO_HILL)
        assert r["straight_length"] == 310.0
        assert r["has_final_hill"] == 0.0  # 坂の言及なし + 高低差あり → 0

    def test_empty_page_all_nan(self):
        r = parse_course_page(_HTML_EMPTY)
        assert math.isnan(r["straight_length"])
        assert math.isnan(r["has_final_hill"])  # 高低差も坂も無し → 判定不能

    def test_empty_html(self):
        r = parse_course_page("")
        assert math.isnan(r["elevation_diff"])


class TestBuildRow:
    def test_bucket_conversion_and_schema(self):
        from src.constants._course_master import COURSE_MASTER_COLS

        row = build_row(_HTML, place="5", race_type="芝", course_len_m="1400")
        assert list(row.keys()) == COURSE_MASTER_COLS
        assert row["place_code"] == "05"       # zfill
        assert row["course_len"] == 14          # 1400m → 100m バケット
        assert row["straight_length"] == 525.9
