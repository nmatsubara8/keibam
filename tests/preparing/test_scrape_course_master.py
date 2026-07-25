"""Phase 9-rev: JRA コース形状スクレイパの解析ロジックのテスト（実フェッチなし）。

合成 HTML は JRA「コースデータ」表 / 「コース紹介」プロセの実構造を模したもの
（値は事実ベース、プロセはテスト用合成文）。実ページでの検証はセッション内で実施済み。
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from scripts.scrape_course_master import (
    TRACK_SLUG_TO_PLACE,
    build_rows,
    parse_course_prose,
    parse_geometry_by_surface,
    parse_turn_direction,
)

# 芝=（高低差表＋A/B/C 区分表）、ダート=（単一結合表）の実レイアウトを模した合成 HTML
_HTML = """
<html><body>
<h2>札幌競馬場</h2><p>コースは右回り。</p>
<h3>コースデータ</h3>
<h3>芝コース</h3>
<table><tr><th>高低差</th><th>発走距離</th></tr>
<tr><td>0.7m</td><td>1,000m、1,200m</td></tr></table>
<table><tr><th>コース</th><th>一周距離</th><th>幅員</th><th>直線距離</th></tr>
<tr><td>A</td><td>1,640.9m</td><td>25～27m</td><td>266.1m</td></tr>
<tr><td>B</td><td>1,650.4m</td><td>22～25.5m</td><td>267.6m</td></tr></table>
<h3>ダートコース</h3>
<table><tr><th>一周距離</th><th>幅員</th><th>直線距離</th><th>高低差</th><th>発走距離</th></tr>
<tr><td>1,487m</td><td>20m</td><td>264.3m</td><td>0.9m</td><td>1,000m</td></tr></table>
<p>この競馬場はオール洋芝で、コーナーの半径が大きい緩やかなカーブが特徴だ。スパイラルカーブは採用されておらず、直線も短い。後方一気の追い込みタイプは苦戦傾向にある。走破時計は遅くなりがちだが、水はけが抜群でよく、重になりにくい馬場である。（テスト用合成文）</p>
</body></html>
"""


class TestParseGeometry:
    def test_shiba_from_variant_tables(self):
        g = parse_geometry_by_surface(_HTML)["芝"]
        assert g["elevation_diff"] == 0.7
        assert g["lap_length"] == 1640.9        # A コース
        assert g["straight_length"] == 266.1
        assert g["width_min"] == 25.0 and g["width_max"] == 27.0

    def test_dirt_from_combined_table(self):
        g = parse_geometry_by_surface(_HTML)["ダート"]
        assert g["lap_length"] == 1487.0
        assert g["straight_length"] == 264.3
        assert g["elevation_diff"] == 0.9
        assert g["width_min"] == 20.0 and g["width_max"] == 20.0


class TestParseProse:
    def test_profile_fields(self):
        p = parse_course_prose(
            "オール洋芝で半径が大きい緩やかなカーブ。スパイラルカーブは採用されておらず"
            "直線も短い。後方一気の追い込みタイプは苦戦。走破時計は遅くなりがち。水はけが抜群でよく重になりにくい。"
        )
        assert p["turf_type_code"] == 1.0
        assert p["corner_radius_large"] == 1.0
        assert p["has_spiral_curve"] == 0.0     # 採用されておらず
        assert p["run_style_bias"] == 1.0       # 追い込み苦戦 → 前有利
        assert p["time_bias"] == -1.0           # 遅くなりがち
        assert p["drainage_good"] == 1.0

    def test_dirt_or_unknown_turf_nan(self):
        p = parse_course_prose("平坦なコース。")
        assert math.isnan(p["turf_type_code"])


class TestTurnDirection:
    def test_right(self):
        assert parse_turn_direction("右回りのコース") == 0.0

    def test_left(self):
        assert parse_turn_direction("左回りのコース") == 1.0

    def test_unknown(self):
        assert math.isnan(parse_turn_direction("不明"))


class TestBuildRows:
    def test_two_surface_rows_with_schema(self):
        from src.constants._course_master import COURSE_MASTER_COLS

        rows = build_rows(_HTML, "01")
        assert len(rows) == 2
        for r in rows:
            assert list(r.keys()) == COURSE_MASTER_COLS
        shiba = next(r for r in rows if r["race_type"] == "芝")
        dirt = next(r for r in rows if r["race_type"] == "ダート")
        assert shiba["straight_length"] == 266.1
        assert shiba["turf_type_code"] == 1.0
        assert math.isnan(dirt["turf_type_code"])   # 芝種はダートに無関係
        assert dirt["turn_direction"] == 0.0


class TestTrackMap:
    def test_ten_jra_tracks(self):
        assert len(TRACK_SLUG_TO_PLACE) == 10
        assert TRACK_SLUG_TO_PLACE["tokyo"] == "05"
        assert TRACK_SLUG_TO_PLACE["sapporo"] == "01"


# ──────────────────────────────────────────
# 2 段階処理（Stage1 取得→素テキスト保存 / Stage2 分析）
# ──────────────────────────────────────────

class TestTwoStage:
    def test_stage1_builds_raw_record(self):
        from scripts.scrape_course_master import build_raw_record

        rec = build_raw_record(_HTML, "sapporo", "01")
        assert rec["place_code"] == "01"
        assert rec["turn_direction"] == 0.0
        # 素テキストは要点抽出せず verbatim（オール洋芝 の記述が残る）
        assert "オール洋芝" in rec["prose_raw"]
        assert "芝" in rec["geometry"] and "ダート" in rec["geometry"]

    def test_stage1_save_load_roundtrip(self, tmp_path):
        from scripts.scrape_course_master import (
            build_raw_record, load_raw_prose, save_raw_prose,
        )

        recs = {"01": build_raw_record(_HTML, "sapporo", "01")}
        path = str(tmp_path / "course_prose.json")
        save_raw_prose(recs, path)
        loaded = load_raw_prose(path)
        assert loaded["01"]["prose_raw"] == recs["01"]["prose_raw"]
        assert loaded["01"]["geometry"]["芝"]["straight_length"] == 266.1

    def test_stage2_analyze_from_raw(self):
        from scripts.scrape_course_master import analyze_record, build_raw_record

        rec = build_raw_record(_HTML, "sapporo", "01")
        rows = analyze_record(rec)
        assert len(rows) == 2
        shiba = next(r for r in rows if r["race_type"] == "芝")
        assert shiba["straight_length"] == 266.1
        assert shiba["turf_type_code"] == 1.0     # 洋芝（Stage2 の要点抽出）
        assert shiba["run_style_bias"] == 1.0      # 追い込み苦戦 → 前有利

    def test_stage2_reanalyze_without_refetch(self, tmp_path):
        """Stage1 の JSON を読み直して Stage2 だけ再実行できる（再フェッチ不要）。"""
        from scripts.scrape_course_master import (
            _stage_analyze, build_raw_record, load_raw_prose, save_raw_prose,
        )

        path = str(tmp_path / "course_prose.json")
        save_raw_prose({"01": build_raw_record(_HTML, "sapporo", "01")}, path)
        rows = _stage_analyze(load_raw_prose(path))
        assert any(r["race_type"] == "芝" and r["straight_length"] == 266.1 for r in rows)


class TestProseMarkerCoverage:
    def test_wide_corner_variants(self):
        from scripts.scrape_course_master import parse_course_prose

        # 「半径がゆったり」（東京系の表現）も緩コーナーとして拾う
        assert parse_course_prose("カーブの半径がゆったりしている").get("corner_radius_large") == 1.0
        # 「タイト」「小回り」は急コーナー
        assert parse_course_prose("2コーナーのカーブがかなりタイトな内回り").get("corner_radius_large") == 0.0

    def test_fast_time_from_agari(self):
        from scripts.scrape_course_master import parse_course_prose

        assert parse_course_prose("レースの上がりタイムは速くなるのが常").get("time_bias") == 1.0
