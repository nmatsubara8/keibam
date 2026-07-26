"""as-of キャリア集計特徴（_add_career_stats）の単体テスト。

horse_info の現在累計でなく horse_results の過去走から積算することで、リーク無しの
「レース日時点までのキャリア」を再現することを確認する。
"""

import numpy as np
import pandas as pd

from src.preprocessing._data_merger import DataMerger


def _merger():
    # __init__ は重い processor 群を要するため、メソッド単体検証ではバイパスする
    return DataMerger.__new__(DataMerger)


def test_career_counts_and_winrate():
    m = _merger()
    results = pd.DataFrame({"horse_id": ["A", "B", "C"]}, index=["r1", "r1", "r1"])
    results.index.name = "race_id"
    # horse_results は当該レース日より前のみ（呼び出し側で date カット済みの想定）
    hr = pd.DataFrame(
        {"着順": [1, 2, 4, 1], "賞金": [1000.0, 0.0, 0.0, 500.0]},
        index=["A", "A", "A", "B"],
    )
    hr.index.name = "horse_id"

    out = m._add_career_stats(results, hr)

    by_horse = out.set_index("horse_id")
    # A: 3 走・1 勝・勝率 1/3・賞金 1000
    assert by_horse.loc["A", "career_starts"] == 3
    assert by_horse.loc["A", "career_wins"] == 1
    assert by_horse.loc["A", "career_winrate"] == 1 / 3
    assert by_horse.loc["A", "career_earnings"] == 1000.0
    # B: 1 走・1 勝・勝率 1.0
    assert by_horse.loc["B", "career_starts"] == 1
    assert by_horse.loc["B", "career_winrate"] == 1.0
    # C: 過去走なし → 欠損（初出走＝未知）
    assert np.isnan(by_horse.loc["C", "career_starts"])
    assert np.isnan(by_horse.loc["C", "career_winrate"])


def test_career_earnings_log_scale():
    m = _merger()
    results = pd.DataFrame({"horse_id": ["A"]}, index=["r1"])
    results.index.name = "race_id"
    hr = pd.DataFrame({"着順": [1], "賞金": [9999.0]}, index=["A"])
    hr.index.name = "horse_id"

    out = m._add_career_stats(results, hr)
    # log1p(9999) ≈ log(10000)
    assert np.isclose(out.iloc[0]["career_earnings_log"], np.log1p(9999.0))


def test_empty_horse_results_is_noop():
    m = _merger()
    results = pd.DataFrame({"horse_id": ["A"]}, index=["r1"])
    empty = pd.DataFrame(columns=["着順", "賞金"])
    out = m._add_career_stats(results, empty)
    # 列は追加されず、行も維持される
    assert "career_starts" not in out.columns
    assert len(out) == 1


def test_no_leakage_only_uses_given_past_rows():
    # 渡された horse_results のみで集計（未来走は呼び出し側で除外済み）。
    # ここでは「2 走分しか渡されていない」状態で 2 走としてカウントされることを確認。
    m = _merger()
    results = pd.DataFrame({"horse_id": ["A"]}, index=["r1"])
    results.index.name = "race_id"
    past_only = pd.DataFrame({"着順": [3, 2], "賞金": [0.0, 0.0]}, index=["A", "A"])
    past_only.index.name = "horse_id"
    out = m._add_career_stats(results, past_only)
    assert out.iloc[0]["career_starts"] == 2
    assert out.iloc[0]["career_wins"] == 0
