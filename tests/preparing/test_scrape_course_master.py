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


# ──────────────────────────────────────────
# 地方競馬（NAR）: 1 ページ内アンカー＋「データ名｜詳細」表
# ──────────────────────────────────────────

# keiba.go.jp/guide/course/ の実構造を模した合成 HTML
# （門別=ダートのみ / 盛岡=芝＋ダート併設で detail に種別分割・平坦表記あり）
_NAR_HTML = """
<html><body>
<div id="course_monb">門別競馬場</div>
<p>広いコーナーの半径が大きい緩やかなカーブで、逃げ・先行が有利なコース。</p>
<table>
<tr><th>データ名</th><th>詳細</th></tr>
<tr><td>回り</td><td>右回り</td></tr>
<tr><td>1周距離</td><td>1,600m</td></tr>
<tr><td>直線距離</td><td>400m</td></tr>
<tr><td>高低差</td><td>1.54m</td></tr>
<tr><td>幅員</td><td>25m</td></tr>
</table>
<div id="course_mori">盛岡競馬場</div>
<p>芝コースを併設する。直線が長く、差し・追い込みも決まりやすい。</p>
<table>
<tr><th>データ名</th><th>詳細</th></tr>
<tr><td>回り</td><td>左回り</td></tr>
<tr><td>1周距離</td><td>ダート1,600m 芝1,400m</td></tr>
<tr><td>直線距離</td><td>ダート300m 芝330m</td></tr>
<tr><td>高低差</td><td>平坦</td></tr>
<tr><td>幅員</td><td>25m</td></tr>
</table>
<div id="course_ooi">大井競馬場</div>
<p>後方からの直線一気も決まる。</p>
<table>
<tr><th>データ名</th><th>詳細</th></tr>
<tr><td>回り</td><td>右回り</td></tr>
<tr><td>1周距離</td><td>1,600m</td></tr>
<tr><td>直線距離</td><td>486m</td></tr>
<tr><td>高低差</td><td>平坦</td></tr>
<tr><td>幅員</td><td>25m</td></tr>
</table>
</body></html>
"""


class TestNar:
    def test_anchor_map_excludes_banei(self):
        from scripts.scrape_course_master import NAR_ANCHOR_TO_PLACE

        assert len(NAR_ANCHOR_TO_PLACE) == 14
        assert NAR_ANCHOR_TO_PLACE["monb"] == "30"     # 門別
        assert NAR_ANCHOR_TO_PLACE["ooi"] == "44"      # 大井
        assert "obi" not in NAR_ANCHOR_TO_PLACE        # 帯広（ばんえい）は対象外

    def test_section_isolates_anchor(self):
        from scripts.scrape_course_master import _nar_section

        seg = _nar_section(_NAR_HTML, "monb")
        assert "門別競馬場" in seg
        assert "盛岡競馬場" not in seg                  # 次アンカーで切れている

    def test_geometry_dirt_only(self):
        from scripts.scrape_course_master import parse_nar_geometry, _nar_section

        g = parse_nar_geometry(_nar_section(_NAR_HTML, "monb"), "ダート")
        assert g["straight_length"] == 400.0
        assert g["lap_length"] == 1600.0
        assert g["elevation_diff"] == 1.54
        assert g["width_min"] == 25.0 and g["width_max"] == 25.0

    def test_hiratan_maps_to_zero(self):
        from scripts.scrape_course_master import parse_nar_geometry, _nar_section

        g = parse_nar_geometry(_nar_section(_NAR_HTML, "ooi"), "ダート")
        assert g["elevation_diff"] == 0.0              # 平坦 → 0

    def test_surface_split_for_shiba(self):
        from scripts.scrape_course_master import (
            parse_nar_geometry, parse_nar_surfaces, _nar_section,
        )

        seg = _nar_section(_NAR_HTML, "mori")
        assert parse_nar_surfaces(seg) == ["ダート", "芝"]   # 芝併設を検出
        dirt = parse_nar_geometry(seg, "ダート")
        shiba = parse_nar_geometry(seg, "芝")
        assert dirt["straight_length"] == 300.0
        assert shiba["straight_length"] == 330.0            # detail の種別分割を解決

    def test_turn_direction(self):
        from scripts.scrape_course_master import _nar_turn

        assert _nar_turn("右回り") == 0.0
        assert _nar_turn("左回り") == 1.0
        assert math.isnan(_nar_turn("左右両回り"))

    def test_build_raw_records_shapes(self):
        from scripts.scrape_course_master import build_raw_records_nar

        recs = build_raw_records_nar(_NAR_HTML)
        assert set(recs) == {"30", "35", "44"}
        assert recs["30"]["turn_direction"] == 0.0
        assert "ダート" in recs["30"]["geometry"] and "芝" not in recs["30"]["geometry"]
        assert "芝" in recs["35"]["geometry"]              # 盛岡は芝併設
        assert "門別競馬場" in recs["30"]["prose_raw"]

    def test_end_to_end_analyze(self):
        from scripts.scrape_course_master import analyze_record, build_raw_records_nar

        recs = build_raw_records_nar(_NAR_HTML)
        rows = analyze_record(recs["30"])
        assert len(rows) == 1                              # ダートのみ
        row = rows[0]
        assert row["place_code"] == "30"
        assert row["race_type"] == "ダート"
        assert row["straight_length"] == 400.0
        assert row["run_style_bias"] == 1.0                # 逃げ・先行有利 → 前
        # 大井「直線一気」は差し側として拾う（後方一気 単独は誤検出しない）
        ooi = analyze_record(recs["44"])[0]
        assert ooi["run_style_bias"] == -1.0


# ──────────────────────────────────────────
# 距離別コースガイド（書籍/ガイド由来プロセ）Stage2 抽出
# ──────────────────────────────────────────

class TestGuideProse:
    def test_upset_prone_positive(self):
        from scripts.scrape_course_master import parse_guide_prose

        p = parse_guide_prose("なかなか難しいコースだけに、馬券が荒れやすいことでも知られている。")
        assert p["upset_prone"] == 1.0

    def test_upset_prone_negative(self):
        from scripts.scrape_course_master import parse_guide_prose

        p = parse_guide_prose("府中の千八、展開要らず。紛れもマグレも出にくいコースということだ。")
        assert p["upset_prone"] == 0.0

    def test_upset_prone_tricky(self):
        from scripts.scrape_course_master import parse_guide_prose

        assert parse_guide_prose("かなりトリッキーな形状だ。").get("upset_prone") == 1.0

    def test_front_negation_guard(self):
        from scripts.scrape_course_master import parse_course_prose

        # 「前有利とはいい切れない」は前有利マッチを相殺する（差し側イン差しが残る）
        p = parse_course_prose("イン差しを決めやすい。意外と前有利とはいい切れないコースでもある。")
        assert p["run_style_bias"] <= 0.0

    def test_only_guide_value_cols(self):
        from scripts.scrape_course_master import parse_guide_prose
        from src.constants._course_guide import COURSE_GUIDE_VALUE_COLS

        p = parse_guide_prose("前有利になりがちだ。")
        assert set(p.keys()) == set(COURSE_GUIDE_VALUE_COLS)


class TestBuildGuideMaster:
    def test_from_csv(self, tmp_path):
        from scripts.scrape_course_master import build_guide_master
        from src.constants._course_guide import COURSE_GUIDE_MASTER_COLS

        path = tmp_path / "course_guide.csv"
        path.write_text(
            "place_code,race_type,course_len_m,course_note,prose_guide\n"
            '05,芝,1400,,"馬券が荒れやすい。前有利になりがちだ。"\n'
            '08,芝,1400,内,"逃げ・先行が有利。"\n'
            '08,芝,1400,外,"差し有利。"\n',
            encoding="utf-8",
        )
        rows = build_guide_master(str(path))
        assert len(rows) == 3                                  # 内/外の両方を保持
        for r in rows:
            assert list(r.keys()) == COURSE_GUIDE_MASTER_COLS
        tokyo = next(r for r in rows if r["place_code"] == "05")
        assert tokyo["course_len_m"] == 1400
        assert tokyo["upset_prone"] == 1.0
        assert tokyo["run_style_bias"] == 1.0

    def test_missing_input_is_empty(self):
        from scripts.scrape_course_master import build_guide_master

        assert build_guide_master("/nonexistent.csv") == []


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
