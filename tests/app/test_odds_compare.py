"""連系オッズ 実績 vs Harville 推定 比較（app._odds_compare）のテスト。"""

import datetime as dt

import pandas as pd

from app._odds_compare import available_combo_targets
from app._odds_compare import compare_combo_odds
from app._odds_compare import final_odds_lookup_from_payouts
from app._odds_compare import tansho_odds_by_race
from src.constants._bet_types import BetType
from src.constants._bet_types import combo_key
from src.preparing._odds_snapshot import make_snapshot

_POST = dt.datetime(2024, 1, 1, 15, 40)
_CAP = dt.datetime(2024, 1, 1, 15, 35)


def _snap(bet_type, combo, odds):
    return make_snapshot("r1", bet_type, combo, odds, _POST, _CAP)


def _race_snaps():
    return [
        # 単勝（Harville 推定の元）
        _snap(BetType.TANSHO, (1,), 2.0),
        _snap(BetType.TANSHO, (2,), 4.0),
        _snap(BetType.TANSHO, (3,), 8.0),
        # 馬連の実績
        _snap(BetType.UMAREN, (1, 2), 6.0),
        _snap(BetType.UMAREN, (1, 3), 11.0),
        _snap(BetType.UMAREN, (2, 3), 20.0),
    ]


def test_tansho_odds_by_race():
    m = tansho_odds_by_race(_race_snaps())
    assert m["r1"] == {1: 2.0, 2: 4.0, 3: 8.0}


def test_available_combo_targets_lists_only_combos():
    targets = available_combo_targets(_race_snaps())
    assert ("r1", BetType.UMAREN) in targets
    # 単勝は連系でないので含まれない
    assert all(bt != BetType.TANSHO for _, bt in targets)


def test_compare_combo_odds_actual_and_harville():
    df = compare_combo_odds(_race_snaps(), "r1", BetType.UMAREN)
    assert list(df.columns) == ["buy", "actual", "harville", "ratio"]
    assert len(df) == 3
    # 実績の降順
    assert df["actual"].is_monotonic_decreasing
    # 各行に実績と推定の両方が入る（単勝があるので Harville 計算可能）
    row = df[df["buy"] == "1-2"].iloc[0]
    assert row["actual"] == 6.0
    assert row["harville"] is not None and row["harville"] > 0
    assert row["ratio"] is not None


def test_compare_without_tansho_harville_is_nan():
    snaps = [_snap(BetType.UMAREN, (1, 2), 6.0)]  # 単勝なし
    df = compare_combo_odds(snaps, "r1", BetType.UMAREN)
    assert len(df) == 1
    assert df.iloc[0]["actual"] == 6.0
    assert df.iloc[0]["harville"] is None  # 推定できない


def test_compare_empty_when_no_actuals():
    snaps = [_snap(BetType.TANSHO, (1,), 2.0)]
    assert compare_combo_odds(snaps, "r1", BetType.UMAREN).empty


# ---------------------------------------------------------------------------
# 払戻由来の確定オッズ lookup（過去レースの確実なソース）
# ---------------------------------------------------------------------------

class _FakeRP:
    def __init__(self, tables):
        self.preprocessed_data = tables


def test_final_odds_lookup_from_payouts():
    tables = {
        # 単勝: win=馬番(int), return=払戻円
        BetType.TANSHO: pd.DataFrame({"win_0": [1], "return_0": [240]}, index=["r1"]),
        # 馬連: win=組合せ list, return=円
        BetType.UMAREN: pd.DataFrame({"win_0": [[1, 2]], "return_0": [1230]}, index=["r1"]),
        # 三連単: win="1→2→3" 文字列
        BetType.SANRENTAN: pd.DataFrame({"win_0": ["1→2→3"], "return_0": [45600]}, index=["r1"]),
    }
    lookup = final_odds_lookup_from_payouts(_FakeRP(tables))
    assert lookup[("r1", BetType.TANSHO, combo_key(BetType.TANSHO, [1]))] == 2.4
    assert lookup[("r1", BetType.UMAREN, "1-2")] == 12.3
    assert lookup[("r1", BetType.SANRENTAN, "1-2-3")] == 456.0


def test_final_odds_lookup_skips_empty_cells():
    tables = {
        BetType.UMAREN: pd.DataFrame(
            {"win_0": [[1, 2]], "return_0": [600], "win_1": [0], "return_1": [0]},
            index=["r1"],
        ),
    }
    lookup = final_odds_lookup_from_payouts(_FakeRP(tables))
    assert lookup == {("r1", BetType.UMAREN, "1-2"): 6.0}


def test_final_odds_lookup_feeds_stored_provider():
    """払戻由来 lookup を StoredFinalOddsProvider に渡して照会できる。"""
    from src.policies._odds_provider import AbstractOddsProvider
    from src.policies._odds_provider import StoredFinalOddsProvider

    class _FB(AbstractOddsProvider):
        def get_odds(self, race_id, bet_type, combo):
            return -1.0

    tables = {BetType.UMAREN: pd.DataFrame({"win_0": [[2, 1]], "return_0": [1230]}, index=["r1"])}
    lookup = final_odds_lookup_from_payouts(_FakeRP(tables))
    provider = StoredFinalOddsProvider(lookup, fallback=_FB())
    # 順不同なので (1,2)/(2,1) どちらでも実績にヒット
    assert provider.get_odds("r1", BetType.UMAREN, (1, 2)) == 12.3
    assert provider.get_odds("r1", BetType.UMAREN, (5, 6)) == -1.0  # 非的中→fallback
