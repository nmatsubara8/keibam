"""血統 coverage 監査（_peds_coverage_audit.py）のユニットテスト。読み取り専用純関数のみ。"""

from __future__ import annotations

import pandas as pd

from src.preprocessing._peds_coverage_audit import (
    coverage, id_profile, peds_integrity, unmatched_examples, year_coverage,
)


class TestIdProfile:
    def test_shape(self):
        s = pd.Series(["2019104567", "2018101234", None])
        p = id_profile(s, "x")
        assert p["nonnull_rows"] == 2
        assert p["unique"] == 2
        assert p["len_dist"] == {10: 2}


class TestCoverage:
    def test_exact_low_norm_high_is_key_mismatch(self):
        # leading zero だけの差 → exact 低・数値正規化で一致
        feat = pd.Series(["0123", "0456"])
        peds = pd.Series(["123", "456"])
        c = coverage(feat, peds)
        assert c["exact"]["unique_coverage"] == 0.0
        assert c["numeric_normalized"]["unique_coverage"] == 1.0

    def test_disjoint_stays_low(self):
        feat = pd.Series(["2019104567", "2018101234"])
        peds = pd.Series(["111", "222"])
        c = coverage(feat, peds)
        assert c["exact"]["unique_coverage"] == 0.0
        assert c["numeric_normalized"]["unique_coverage"] == 0.0


class TestYearCoverage:
    def test_by_year(self):
        feat = pd.DataFrame({"horse_id": ["1", "2", "3", "4"],
                             "_yr": [2020, 2020, 2021, 2021]})
        peds = pd.Series(["1", "3"])
        yc = year_coverage(feat, peds, id_col="horse_id", year_col="_yr")
        assert yc[2020] == 0.5
        assert yc[2021] == 0.5


class TestDuplicateIndexRobustness:
    def test_year_coverage_survives_duplicate_index(self):
        idx = pd.Index(["r1", "r1", "r2"], name="race_id")
        feat = pd.DataFrame({"horse_id": ["1", "2", "3"], "_yr": [2020, 2020, 2021]},
                            index=idx)
        peds = pd.Series(["1"])
        yc = year_coverage(feat, peds, id_col="horse_id", year_col="_yr")
        assert yc[2020] == 0.5
        assert yc[2021] == 0.0


class TestPedsIntegrity:
    def test_duplicates_and_conflict(self):
        peds = pd.DataFrame({
            "horse_id": ["1", "1", "2"],
            "peds_0": ["sireA", "sireB", "sireC"],   # 馬1 に父競合
            "peds_32": ["dsX", "dsX", None],
        })
        r = peds_integrity(peds, id_col="horse_id", sire_col="peds_0", damsire_col="peds_32")
        assert r["unique_horse"] == 2
        assert r["duplicate_horse_rows"] == 1
        assert r["one_to_many_horses"] == 1
        assert r["sire_conflicting_horses"] == 1     # 馬1: sireA vs sireB
        assert r["damsire_conflicting_horses"] == 0
        assert abs(r["damsire_nonnull_rate"] - 2 / 3) < 1e-9

    def test_missing_cols_graceful(self):
        peds = pd.DataFrame({"horse_id": ["1", "2"]})
        r = peds_integrity(peds, id_col="horse_id", sire_col=None, damsire_col=None)
        assert r["sire_col"] is None
        assert r["unique_horse"] == 2


class TestUnmatched:
    def test_examples(self):
        feat = pd.Series(["a", "b", "c"])
        peds = pd.Series(["a"])
        assert set(unmatched_examples(feat, peds, top=10)) == {"b", "c"}
