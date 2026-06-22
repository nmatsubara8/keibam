"""市場歪み特徴量（_market_signals）のユニットテスト。

確定オッズ → 馬単位 marginal → Harville 理論との overlay の算出を検証する。
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.policies import _harville as harville
from src.preprocessing._market_signals import (
    MARKET_SIGNAL_COLS,
    build_market_signal_frame,
    race_market_signals,
)


def _win_odds_3():
    # 3頭、明確な人気順（馬番1が一番人気）
    return {1: 2.0, 2: 4.0, 3: 8.0}


class TestRaceMarketSignals:
    def test_empty_when_too_few_horses(self):
        assert race_market_signals({1: 2.0, 2: 3.0}, {}) == {}

    def test_no_exotic_data_returns_empty(self):
        # 単勝のみ・連系データ無し → overlay は計算できず空
        assert race_market_signals(_win_odds_3(), {}) == {}

    def test_fukusho_implied_and_overlay(self):
        win = _win_odds_3()
        # 複勝 implied が単勝順と同じ並びなら overlay は小さく、歪ませると符号が出る
        by_type = {BetType.FUKUSHO: {(1,): 1.3, (2,): 2.0, (3,): 4.0}}
        sig = race_market_signals(win, by_type)
        # fukusho_implied_p は Σ_h = 3 に正規化される
        total = sum(sig[h]["fukusho_implied_p"] for h in (1, 2, 3))
        assert total == pytest.approx(3.0, abs=1e-9)
        # 各馬 place_overlay = implied − Harville複勝
        wp = harville.normalize({1: 1 / 2.0, 2: 1 / 4.0, 3: 1 / 8.0})
        for h in (1, 2, 3):
            expect = sig[h]["fukusho_implied_p"] - harville.prob_place(wp, h, 3)
            assert sig[h]["place_overlay"] == pytest.approx(expect, abs=1e-9)

    def test_trio_top3_marginal_sums_to_three(self):
        win = _win_odds_3()
        # 3頭なら三連複の組合せは1通り {1,2,3}
        by_type = {BetType.SANRENPUKU: {(1, 2, 3): 5.0}}
        sig = race_market_signals(win, by_type)
        # 単一 combo なので各馬の marginal は等しく、Σ_h = 3 → 各 1.0
        # overlay = 1.0*3/3 - place_p。Σ_h overlay = 3 - 3 = 0
        s = sum(sig[h]["trio_top3_overlay"] for h in (1, 2, 3))
        assert s == pytest.approx(0.0, abs=1e-9)

    def test_trifecta_win_marginal_sums_to_one(self):
        win = _win_odds_3()
        # 全6順列に均等オッズ → 1着 marginal は各馬 1/3、Σ=1
        perms = {
            (1, 2, 3): 6.0, (1, 3, 2): 6.0, (2, 1, 3): 6.0,
            (2, 3, 1): 6.0, (3, 1, 2): 6.0, (3, 2, 1): 6.0,
        }
        sig = race_market_signals(win, {BetType.SANRENTAN: perms})
        # 均等なので 1着 marginal = 1/3 each
        for h in (1, 2, 3):
            wm = sig[h]["trifecta_win_overlay"] + harville.normalize(
                {1: 1 / 2.0, 2: 1 / 4.0, 3: 1 / 8.0}
            )[h]
            assert wm == pytest.approx(1 / 3, abs=1e-9)
        # win_overlay の Σ_h = 1 - 1 = 0
        assert sum(sig[h]["trifecta_win_overlay"] for h in (1, 2, 3)) == pytest.approx(
            0.0, abs=1e-9
        )

    def test_smart_money_sign(self):
        """三連単で馬3を不自然に厚く（低オッズ＝買われている）すると win_overlay>0。"""
        win = _win_odds_3()  # 馬3は単勝8.0で人気薄
        # 馬3が1着の順列だけ極端に低オッズ（＝市場が厚く張っている）
        perms = {
            (1, 2, 3): 50.0, (1, 3, 2): 50.0, (2, 1, 3): 50.0, (2, 3, 1): 50.0,
            (3, 1, 2): 2.0, (3, 2, 1): 2.0,  # 馬3が1着の目だけ厚い
        }
        sig = race_market_signals(win, {BetType.SANRENTAN: perms})
        # 単勝では人気薄なのに連系1着で厚い → overlay は正（市場の歪み＝妙味/コネ）
        assert sig[3]["trifecta_win_overlay"] > 0


class TestBuildMarketSignalFrame:
    def test_empty_inputs(self):
        df = build_market_signal_frame({}, {})
        assert list(df.columns) == ["race_id", "馬番", *MARKET_SIGNAL_COLS]
        assert df.empty

    def test_frame_has_rows_and_keys(self):
        from src.constants._bet_types import combo_key

        win_by_race = {"r1": {1: 2.0, 2: 4.0, 3: 8.0}}
        lookup = {
            ("r1", BetType.FUKUSHO, combo_key(BetType.FUKUSHO, (1,))): 1.3,
            ("r1", BetType.FUKUSHO, combo_key(BetType.FUKUSHO, (2,))): 2.0,
            ("r1", BetType.FUKUSHO, combo_key(BetType.FUKUSHO, (3,))): 4.0,
        }
        df = build_market_signal_frame(lookup, win_by_race)
        assert set(df["馬番"]) == {1, 2, 3}
        assert (df["race_id"] == "r1").all()
        assert df["fukusho_implied_p"].notna().all()
        # 馬番は int
        assert df["馬番"].map(type).eq(int).all()
