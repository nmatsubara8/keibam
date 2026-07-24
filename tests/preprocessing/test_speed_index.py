"""Phase 3: スピード指数（_speed_index.py）のユニットテスト。

基準タイム表の cutoff によるリーク遮断、標準化、粗キーフォールバック、
save/load 往復を検証する。ラベルシャッフルでは分布リークを検出できないため、
「cutoff 以降のレースが基準統計に寄与しない」構造テストが必須。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.constants._speed_index import BASE_TIME_MIN_COUNT, SPEED_INDEX_BASE
from src.preprocessing._speed_index import (
    attach_speed_index,
    build_base_time_table,
    load_base_time_table,
    save_base_time_table,
)


def _hr(n: int, place: str = "05", gs: str = "良", t0: float = 90.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "開催": [place] * n,
            "race_type": ["芝"] * n,
            "course_len": [16.0] * n,
            "馬場": [gs] * n,
            "time_seconds": list(np.linspace(t0, t0 + 10, n)),
            "date": pd.date_range("2020-01-01", periods=n, freq="7D"),
        }
    )


class TestBuildBaseTimeTable:
    def test_fine_and_coarse_present(self):
        base = build_base_time_table(_hr(40))
        assert not base["fine"].empty
        assert not base["coarse"].empty

    def test_cutoff_excludes_future_races(self):
        hr = _hr(40)  # weekly from 2020-01-01 → ~40 weeks
        cutoff = hr["date"].iloc[20]
        full = build_base_time_table(hr)["fine"]["count"].iloc[0]
        cut = build_base_time_table(hr, cutoff_date=cutoff)["fine"]["count"].iloc[0]
        assert cut < full
        assert cut == 20  # date < cutoff → first 20 rows

    def test_no_time_col_returns_empty(self):
        base = build_base_time_table(pd.DataFrame({"開催": ["05"]}))
        assert base["fine"].empty and base["coarse"].empty


class TestAttachSpeedIndex:
    def test_faster_gets_higher_index(self):
        base = build_base_time_table(_hr(40))  # mean ~95
        fast = pd.DataFrame({"開催": ["05"], "race_type": ["芝"], "course_len": [16.0],
                             "馬場": ["良"], "time_seconds": [90.0]})
        slow = pd.DataFrame({"開催": ["05"], "race_type": ["芝"], "course_len": [16.0],
                             "馬場": ["良"], "time_seconds": [100.0]})
        si_fast = attach_speed_index(fast, base)["speed_index"].iloc[0]
        si_slow = attach_speed_index(slow, base)["speed_index"].iloc[0]
        assert si_fast > SPEED_INDEX_BASE > si_slow

    def test_coarse_fallback_for_unknown_place(self):
        base = build_base_time_table(_hr(40))
        # 開催=99 は fine に無い → coarse(race_type,course_len) にフォールバック
        row = pd.DataFrame({"開催": ["99"], "race_type": ["芝"], "course_len": [16.0],
                            "馬場": ["重"], "time_seconds": [95.0]})
        si = attach_speed_index(row, base)["speed_index"].iloc[0]
        assert not pd.isna(si)

    def test_fine_below_min_count_falls_back(self):
        # fine セルが min_count 未満 → coarse を使う。coarse は十分な件数にする。
        small = _hr(BASE_TIME_MIN_COUNT - 5, place="07")
        big = _hr(40, place="05")
        base = build_base_time_table(pd.concat([small, big], ignore_index=True))
        # 開催07 の fine count < min → coarse fallback（coarse は 07+05 合算で十分）
        row = pd.DataFrame({"開催": ["07"], "race_type": ["芝"], "course_len": [16.0],
                            "馬場": ["良"], "time_seconds": [95.0]})
        si = attach_speed_index(row, base)["speed_index"].iloc[0]
        assert not pd.isna(si)

    def test_no_base_yields_nan(self):
        base = build_base_time_table(_hr(40))
        row = pd.DataFrame({"開催": ["99"], "race_type": ["ダート"], "course_len": [24.0],
                            "馬場": ["重"], "time_seconds": [150.0]})
        assert pd.isna(attach_speed_index(row, base)["speed_index"].iloc[0])

    def test_empty_base_dict_yields_nan_col(self):
        row = pd.DataFrame({"時": [1], "time_seconds": [90.0]})
        out = attach_speed_index(row, {})
        assert "speed_index" in out.columns and out["speed_index"].isna().all()


class TestSaveLoadRoundTrip:
    def test_roundtrip_preserves_speed_index(self, tmp_path):
        base = build_base_time_table(_hr(40))
        path = str(tmp_path / "base_time_table.csv")
        save_base_time_table(base, path)
        loaded = load_base_time_table(path)
        row = pd.DataFrame({"開催": ["05"], "race_type": ["芝"], "course_len": [16.0],
                            "馬場": ["良"], "time_seconds": [90.0]})
        a = attach_speed_index(row, base)["speed_index"].iloc[0]
        b = attach_speed_index(row, loaded)["speed_index"].iloc[0]
        assert a == pytest.approx(b)

    def test_load_missing_path_returns_empty(self):
        loaded = load_base_time_table("/nonexistent/path.csv")
        assert loaded["fine"].empty and loaded["coarse"].empty
