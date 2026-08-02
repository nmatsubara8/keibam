"""学習由来メタ(_provenance)の純関数テスト＝nar_rows は実データ由来・fail-closed。"""
from __future__ import annotations

import pandas as pd
import pytest

from src.training._provenance import assert_jra_only, build_training_provenance


def test_provenance_data_derived_nar():
    # JRA(場05) 2件 + NAR(場44大井,50園田) 2件
    idx = pd.Index(["202305010101", "202305010102", "202544011301", "202550011302"])
    p = build_training_provenance(idx, jra_only=True)
    assert p["train_rows"] == 4 and p["train_races"] == 4
    assert p["nar_rows"] == 2                       # 実データから 2（フラグでなく実測）
    assert p["nar_fraction"] == 0.5
    assert p["jra_only_effective"] is False         # flag=True でも実データは JRA限定でない
    assert p["jra_only_flag"] is True               # 入力フラグは記録のみ
    assert p["place_code_counts"].get("05") == 2


def test_provenance_pure_jra():
    idx = pd.Index(["202305010101", "202306010102"])   # 場05,06 = JRA
    p = build_training_provenance(idx)
    assert p["nar_rows"] == 0 and p["jra_only_effective"] is True


def test_assert_jra_only_fail_closed():
    ok = pd.Index(["202305010101", "202306010102"])
    assert assert_jra_only(ok) == 0                 # JRAのみ→通る
    bad = pd.Index(["202305010101", "202544011301"])  # NAR混入
    with pytest.raises(RuntimeError, match="NAR"):
        assert_jra_only(bad)
