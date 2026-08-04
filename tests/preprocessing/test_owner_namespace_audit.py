"""馬主 ID 空間監査（_owner_namespace_audit.py）のユニットテスト。読み取り専用純関数のみ。"""

from __future__ import annotations

import pandas as pd

from src.preprocessing._owner_namespace_audit import (
    bridge_via_horse_info, exact_match, id_space_profile, name_id_consistency,
    results_owner_temporal_variability, unmatched_top, year_join_coverage,
)


class TestIdSpaceProfile:
    def test_basic_shape(self):
        s = pd.Series(["000031", "001031", "003060", None, "000031"])
        p = id_space_profile(s)
        assert p["total_rows"] == 5
        assert p["nonnull_rows"] == 4
        assert p["unique"] == 3
        assert p["len_dist"] == {6: 3}
        assert p["has_leading_zero"] is True

    def test_float_dot_zero_stripped(self):
        s = pd.Series([31.0, 1031.0])  # float の ".0" は落とす
        p = id_space_profile(s)
        assert set(p["examples"]) == {"31", "1031"}
        assert p["has_leading_zero"] is False


class TestExactMatch:
    def test_disjoint_spaces(self):
        left = pd.Series(["000031", "001031", "003060"])   # results 空間
        right = pd.Series(["100800", "102800", "108803"])  # person_yearly 空間
        m = exact_match(left, right)
        assert m["row_match_rate"] == 0.0
        assert m["matched_unique"] == 0

    def test_matching_space(self):
        left = pd.Series(["494800", "486800", "999999"])
        right = pd.Series(["494800", "486800", "165033"])
        m = exact_match(left, right)
        assert abs(m["row_match_rate"] - 2 / 3) < 1e-9
        assert m["matched_unique"] == 2


class TestYearJoinCoverage:
    def test_id_and_year_separated(self):
        feat = pd.DataFrame({"owner_id": ["494800", "494800"], "_yr": [2020, 2021]})
        py = pd.DataFrame({"entity_id": ["494800"], "year": [2019]})
        r = year_join_coverage(feat, py, id_col="owner_id", year_col="_yr")
        assert r["id_match_rate"] == 1.0          # ID は両行一致
        assert r["id_and_year_match_rate"] == 0.5  # 2020→2019 のみ前年成績あり


class TestNameIdConsistency:
    def test_collision_and_alias(self):
        # 同名で別ID(衝突) と、同一IDに別名(表記ゆれ)
        names = ["社台レースホース", "社台レースホース", "ウイン", "ウ イン"]
        ids = ["415800", "999999", "494800", "494800"]
        r = name_id_consistency(names, ids)
        assert r["name_collisions"] == 1   # 社台レースホース → 2 IDs
        assert r["id_alias_spread"] == 1   # 内部スペース差は温存され別名として検出（監査で可視化）

    def test_normalization_absorbs_spacing(self):
        r = name_id_consistency(["ノーザンファーム 勇払郡", "ノーザンファーム"], ["1", "1"])
        assert r["id_alias_spread"] == 0   # 所在地サフィックス除去で同名化


class TestBridgeViaHorseInfo:
    def test_bridge_recovers_when_results_space_wrong(self):
        # results.owner_id は別空間だが horse_info.owner_id(db) 経由で py と繋がる
        feat = pd.DataFrame({"horse_id": ["h1", "h2"], "owner_id": ["000031", "000031"],
                             "_yr": [2020, 2021]})
        hinfo = pd.DataFrame({"owner_id": ["494800", "486800"]},
                             index=pd.Index(["h1", "h2"], name="horse_id"))
        py = pd.DataFrame({"entity_id": ["494800", "486800"], "year": [2019, 2020]})
        r = bridge_via_horse_info(feat, hinfo, py, year_col="_yr")
        assert r["bridge_success_rate"] == 1.0
        assert r["bridged_id_match_rate"] == 1.0
        assert r["final_join_rate_incl_prior_year"] == 1.0   # 2020→2019, 2021→2020 とも有
        assert r["horses_with_multiple_owner_in_horse_info"] == 0
        assert r["horse_info_is_static_master"] is True

    def test_bridge_miss_when_horse_absent(self):
        feat = pd.DataFrame({"horse_id": ["h1", "hX"], "owner_id": ["1", "1"],
                             "_yr": [2020, 2020]})
        hinfo = pd.DataFrame({"owner_id": ["494800"]},
                             index=pd.Index(["h1"], name="horse_id"))
        py = pd.DataFrame({"entity_id": ["494800"], "year": [2019]})
        r = bridge_via_horse_info(feat, hinfo, py, year_col="_yr")
        assert r["bridge_success_rate"] == 0.5   # hX は horse_info に無い


class TestResultsOwnerTemporal:
    def test_variability_flags_racetime_owner(self):
        # 同一馬で年により results.owner_id が変わる＝race-time 馬主の可能性
        feat = pd.DataFrame({"horse_id": ["h1", "h1", "h2"],
                             "owner_id": ["000031", "000099", "000031"]})
        r = results_owner_temporal_variability(feat)
        assert r["horses_with_multiple_results_owner"] == 1
        assert r["horses"] == 2


class TestUnmatchedTop:
    def test_counts(self):
        left = pd.Series(["000031", "000031", "001031"])
        right = pd.Series(["100800"])
        top = unmatched_top(left, right, top=5)
        assert top[0] == ("000031", 2)
