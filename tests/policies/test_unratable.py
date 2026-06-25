"""unratable（初出走）馬の公衆フォールバック（ベンター §3）のテスト。"""

import pandas as pd
import pytest

from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._odds_provider import AbstractOddsProvider
from src.policies._unratable import build_unratable_by_race
from src.policies._unratable import is_unratable_only
from src.policies._unratable import public_fallback


class _DictOdds(AbstractOddsProvider):
    """(bet_type, combo) → オッズの辞書スタブ。未登録は既定値。"""

    def __init__(self, table, default=1000.0):
        self._t = table
        self._d = default

    def get_odds(self, race_id, bet_type, combo):
        return self._t.get((bet_type, tuple(combo)), self._d)


def _table(race_id, umaban_probs):
    rows = [{ResultsCols.UMABAN: u, "prob": p} for u, p in umaban_probs]
    return pd.DataFrame(rows, index=[race_id] * len(rows))


class TestPublicFallback:
    def test_unratable_takes_public_prob(self):
        model = {1: 0.6, 2: 0.3, 3: 0.1}
        public = {1: 0.5, 2: 0.3, 3: 0.2}
        out = public_fallback(model, public, {3})
        # 3番は公衆値、合計は1
        assert out[3] == pytest.approx(0.2)
        assert sum(out.values()) == pytest.approx(1.0)
        # 1,2 はモデル比(0.6:0.3=2:1)を保ったまま残余質量(0.8)を分配
        assert out[1] / out[2] == pytest.approx(2.0)
        assert out[1] + out[2] == pytest.approx(0.8)

    def test_no_unratable_is_renormalized_model(self):
        model = {1: 0.6, 2: 0.3, 3: 0.1}
        public = {1: 0.5, 2: 0.3, 3: 0.2}
        out = public_fallback(model, public, set())
        assert out == pytest.approx({1: 0.6, 2: 0.3, 3: 0.1})

    def test_unratable_without_public_kept_as_model(self):
        # 4番は unratable だが公衆値なし → 置換せずモデル勝率を残す
        model = {1: 0.6, 2: 0.3, 4: 0.1}
        public = {1: 0.5, 2: 0.5}
        out = public_fallback(model, public, {4})
        assert set(out) == {1, 2, 4}
        assert sum(out.values()) == pytest.approx(1.0)

    def test_is_unratable_only(self):
        assert is_unratable_only([1, 2], {1, 2, 3}) is True
        assert is_unratable_only([1, 2], {1}) is False
        assert is_unratable_only([], {1}) is False


class TestBuildUnratableByRace:
    def test_debut_detected_by_career_starts(self):
        X = pd.DataFrame(
            {
                ResultsCols.UMABAN: [1, 2, 3, 1, 2],
                "career_starts": [5.0, float("nan"), 0.0, 3.0, 10.0],
            },
            index=["r1", "r1", "r1", "r2", "r2"],
        )
        out = build_unratable_by_race(X)
        assert out == {"r1": {2, 3}}  # r1 の馬2(NaN)・馬3(0) が初出走、r2 は無し

    def test_missing_column_returns_empty(self):
        X = pd.DataFrame({ResultsCols.UMABAN: [1, 2]}, index=["r1", "r1"])
        assert build_unratable_by_race(X) == {}


class TestPolicyWiring:
    def test_debut_only_race_excluded(self):
        # 全馬初出走 → 候補なし（モデルが効かないため除外）
        probs = [(1, 0.5), (2, 0.5)]
        policy = ExpectedValueBetPolicy(
            _DictOdds({}), thresholds={BetType.TANSHO: 0.0}, unratable_fallback=True,
        )
        cands = policy.select(
            _table("r1", probs), unratable_by_race={"r1": {1, 2}}
        )
        assert cands == []

    def test_debut_horse_uses_public_prob(self):
        # 馬3を初出走に。単勝オッズから公衆 implied を作り 3 の勝率が公衆値へ動く。
        probs = [(1, 0.2), (2, 0.2), (3, 0.6)]  # モデルは3を過大評価
        odds = {
            (BetType.TANSHO, (1,)): 3.0,
            (BetType.TANSHO, (2,)): 3.0,
            (BetType.TANSHO, (3,)): 30.0,  # 市場は3を低評価
        }
        policy = ExpectedValueBetPolicy(
            _DictOdds(odds), thresholds={BetType.TANSHO: 0.0}, unratable_fallback=True,
        )
        cands = {
            c.combo: c.probability
            for c in policy.select(_table("r1", probs), unratable_by_race={"r1": {3}})
        }
        # 3 の勝率は元の 0.6 から公衆 implied 由来へ低下
        assert cands[(3,)] < 0.6
        assert sum(cands.values()) == pytest.approx(1.0)

    def test_no_fallback_map_is_unchanged(self):
        probs = [(1, 0.5), (2, 0.5)]
        base = ExpectedValueBetPolicy(_DictOdds({}), thresholds={BetType.TANSHO: 1.0})
        wired = ExpectedValueBetPolicy(
            _DictOdds({}), thresholds={BetType.TANSHO: 1.0}, unratable_fallback=True,
        )
        a = {c.combo: c.probability for c in base.select(_table("r1", probs))}
        # unratable_by_race を渡さなければ従来挙動
        b = {c.combo: c.probability for c in wired.select(_table("r1", probs))}
        assert a == b
