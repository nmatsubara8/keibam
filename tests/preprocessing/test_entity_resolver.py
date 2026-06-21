"""名寄せ（エンティティ解決）レイヤの単体テスト。"""

import numpy as np
import pandas as pd

from src.preprocessing._entity_resolver import (
    EntityMaster,
    canonical_id,
    extract_race_grade,
    normalize_breeder_name,
    normalize_race_name,
    normalize_text,
)


class TestNormalizeText:
    def test_nfkc_fullwidth_to_halfwidth(self):
        # 全角英数字・記号を半角へ統一
        assert normalize_text("ＡＢＣ１２３") == "ABC123"

    def test_collapse_and_strip_whitespace(self):
        assert normalize_text("  ノーザン  ファーム  ") == "ノーザン ファーム"

    def test_missing_values_return_empty(self):
        assert normalize_text(None) == ""
        assert normalize_text(np.nan) == ""
        assert normalize_text(float("nan")) == ""

    def test_non_string_coerced(self):
        assert normalize_text(123) == "123"


class TestBreederName:
    def test_strips_location_suffix(self):
        # 「ノーザンファーム 勇払郡」→「ノーザンファーム」
        assert normalize_breeder_name("ノーザンファーム 勇払郡") == "ノーザンファーム"
        assert normalize_breeder_name("社台ファーム 千歳市") == "社台ファーム"

    def test_keeps_name_without_location(self):
        assert normalize_breeder_name("キャロットファーム") == "キャロットファーム"

    def test_fullwidth_space_location(self):
        assert normalize_breeder_name("三嶋牧場　浦河町") == "三嶋牧場"

    def test_missing(self):
        assert normalize_breeder_name(np.nan) == ""


class TestRaceName:
    def test_removes_grade_paren(self):
        assert normalize_race_name("京王杯スプリングC(G2)") == "京王杯スプリングC"

    def test_fullwidth_grade(self):
        # 全角の「Ｃ（GⅡ）」も NFKC + グレード除去で揃う
        assert normalize_race_name("京王杯スプリングＣ（GⅡ）") == "京王杯スプリングC"

    def test_plain_name(self):
        assert normalize_race_name("3歳未勝利") == "3歳未勝利"


class TestExtractGrade:
    def test_g1_g2_g3(self):
        assert extract_race_grade("有馬記念(G1)") == "G1"
        assert extract_race_grade("京王杯スプリングC(GⅡ)") == "G2"
        assert extract_race_grade("福島牝馬S(GⅢ)") == "G3"

    def test_jpn_and_listed(self):
        assert extract_race_grade("東京ダービー(Jpn1)") == "JPN1"
        assert extract_race_grade("淀短距離S(L)") == "L"

    def test_no_grade(self):
        assert extract_race_grade("3歳未勝利") is None
        assert extract_race_grade(np.nan) is None


class TestCanonicalId:
    def test_int_float_unified(self):
        # int64 由来と float64 由来を同じ文字列へ
        assert canonical_id(1234) == "1234"
        assert canonical_id(1234.0) == "1234"
        assert canonical_id("1234") == "1234"

    def test_hashed_owner_id_preserved(self):
        assert canonical_id("o_4ERnJzTEF0d") == "o_4ERnJzTEF0d"

    def test_missing(self):
        assert canonical_id(None) is None
        assert canonical_id(np.nan) is None
        assert canonical_id("") is None


class TestEntityMaster:
    def test_build_and_resolve_unique(self):
        m = EntityMaster().build(
            ids=[101, 102, 101], names=["ディープ", "オルフェ", "ディープ"]
        )
        assert m.resolve("ディープ") == "101"
        assert m.resolve("オルフェ") == "102"
        assert len(m) == 2

    def test_resolve_ambiguous_returns_none(self):
        # 同名に複数 id がぶら下がる場合は曖昧 → None
        m = EntityMaster().build(ids=[1, 2], names=["サクラ", "サクラ"])
        assert m.resolve("サクラ") is None

    def test_resolve_unknown_returns_none(self):
        m = EntityMaster().build(ids=[1], names=["A"])
        assert m.resolve("存在しない") is None
        assert m.resolve(np.nan) is None

    def test_float_id_unified_in_master(self):
        # float 由来 id でも canonical_id で揃う
        m = EntityMaster().build(ids=[2021103272.0], names=["メイショウタバル"])
        assert m.resolve("メイショウタバル") == "2021103272"
        assert m.name_of(2021103272) == "メイショウタバル"

    def test_normalized_lookup(self):
        # 別表記（全角空白・全角英字）でも正規化して解決
        m = EntityMaster().build(ids=[5], names=["Ｗ ファーム"])
        assert m.resolve("W ファーム") == "5"

    def test_breeder_normalizer_dedups_location(self):
        # 所在地サフィックス違いを同一生産者へ名寄せ
        m = EntityMaster(normalizer=normalize_breeder_name).build(
            ids=["b_1", "b_1"],
            names=["ノーザンファーム 勇払郡", "ノーザンファーム"],
        )
        assert m.resolve("ノーザンファーム 安平町") == "b_1"
        assert len(m) == 1

    def test_build_from_pandas_series(self):
        df = pd.DataFrame({"id": [10, 11], "name": ["甲", "乙"]})
        m = EntityMaster().build(df["id"], df["name"])
        assert m.resolve("甲") == "10"
        assert m.name_of(11) == "乙"
