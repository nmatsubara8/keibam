"""確定オッズ実績 lookup・StoredFinalOddsProvider・combo 正規化のテスト。"""

import datetime as dt

from src.constants._bet_types import BetType
from src.constants._bet_types import canonical_combo
from src.constants._bet_types import combo_key
from src.policies._odds_provider import AbstractOddsProvider
from src.policies._odds_provider import StoredFinalOddsProvider
from src.preparing._odds_snapshot import build_final_odds_lookup
from src.preparing._odds_snapshot import make_snapshot

_POST = dt.datetime(2024, 1, 1, 15, 40)


# ---------------------------------------------------------------------------
# combo 正規化
# ---------------------------------------------------------------------------

def test_canonical_combo_unordered_sorts():
    assert canonical_combo(BetType.UMAREN, (2, 1)) == (1, 2)
    assert canonical_combo(BetType.SANRENPUKU, (3, 1, 2)) == (1, 2, 3)


def test_canonical_combo_ordered_preserves():
    assert canonical_combo(BetType.UMATAN, (2, 1)) == (2, 1)
    assert canonical_combo(BetType.SANRENTAN, (3, 1, 2)) == (3, 1, 2)


def test_combo_key_strings():
    assert combo_key(BetType.UMAREN, (2, 1)) == "1-2"
    assert combo_key(BetType.SANRENTAN, (3, 1, 2)) == "3-1-2"


# ---------------------------------------------------------------------------
# build_final_odds_lookup
# ---------------------------------------------------------------------------

def _snap(race_id, bet_type, combo, odds, captured):
    return make_snapshot(race_id, bet_type, combo, odds, _POST, captured)


def test_build_lookup_canonicalizes_and_picks_latest():
    c0 = dt.datetime(2024, 1, 1, 15, 0)
    c1 = dt.datetime(2024, 1, 1, 15, 30)
    snaps = [
        _snap("r1", BetType.UMAREN, (2, 1), 10.0, c0),  # 古い
        _snap("r1", BetType.UMAREN, (1, 2), 12.5, c1),  # 新しい（同一組合せ）
        _snap("r1", BetType.SANRENTAN, (3, 1, 2), 250.0, c1),
    ]
    lookup = build_final_odds_lookup(snaps)
    # (2,1) と (1,2) は同一キー "1-2" に正規化され、最新の 12.5 が残る
    assert lookup[("r1", BetType.UMAREN, "1-2")] == 12.5
    assert lookup[("r1", BetType.SANRENTAN, "3-1-2")] == 250.0


def test_build_lookup_filters_bet_types():
    snaps = [
        _snap("r1", BetType.UMAREN, (1, 2), 10.0, _POST),
        _snap("r1", BetType.WIDE, (1, 2), 3.0, _POST),
    ]
    lookup = build_final_odds_lookup(snaps, bet_types=[BetType.UMAREN])
    assert set(k[1] for k in lookup) == {BetType.UMAREN}


def test_build_lookup_skips_nonpositive_odds():
    snaps = [_snap("r1", BetType.UMAREN, (1, 2), 0.0, _POST)]
    assert build_final_odds_lookup(snaps) == {}


# ---------------------------------------------------------------------------
# StoredFinalOddsProvider
# ---------------------------------------------------------------------------

class _FixedFallback(AbstractOddsProvider):
    def __init__(self, odds):
        self._odds = odds

    def get_odds(self, race_id, bet_type, combo):
        return self._odds


def test_provider_returns_actual_when_present():
    lookup = {("r1", BetType.UMAREN, "1-2"): 12.5}
    provider = StoredFinalOddsProvider(lookup, fallback=_FixedFallback(99.0))
    # 順不同なので (2,1) で照会しても実績 (1,2) にヒット
    assert provider.get_odds("r1", BetType.UMAREN, (2, 1)) == 12.5
    assert provider.has_actual("r1", BetType.UMAREN, (2, 1))


def test_provider_falls_back_when_missing():
    provider = StoredFinalOddsProvider({}, fallback=_FixedFallback(99.0))
    assert provider.get_odds("r1", BetType.UMAREN, (1, 2)) == 99.0
    assert not provider.has_actual("r1", BetType.UMAREN, (1, 2))


def test_provider_ordered_combo_distinguishes_order():
    lookup = {("r1", BetType.UMATAN, "1-2"): 30.0}
    provider = StoredFinalOddsProvider(lookup, fallback=_FixedFallback(99.0))
    assert provider.get_odds("r1", BetType.UMATAN, (1, 2)) == 30.0
    # 逆順は別馬券 → fallback
    assert provider.get_odds("r1", BetType.UMATAN, (2, 1)) == 99.0
