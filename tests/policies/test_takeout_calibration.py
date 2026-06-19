"""src/policies/_takeout_calibration.py の単体テスト。

払戻実績（的中組の確定オッズ）+ 単勝勝率から券種別実効控除率を逆算するロジックと、
永続化（JSON）を検証する。Streamlit / I/O 依存なし。
"""

from __future__ import annotations

import math

from src.constants._bet_types import BetType
from src.constants._bet_types import combo_key
from src.policies import _harville as harville
from src.policies._takeout_calibration import calibrate_takeout_from_payouts
from src.policies._takeout_calibration import latest_takeout_map
from src.policies._takeout_calibration import save_takeout_calibration
from src.policies._takeout_calibration import takeout_calibration_path
from src.policies._takeout_calibration import takeout_map
from src.policies._takeout_calibration import winner_return_rate


def _tansho(n_races: int) -> dict:
    """同一オッズ構成の n_races レース分の {race_id: {馬番: 単勝}}。"""
    odds = {1: 2.0, 2: 4.0, 3: 8.0, 4: 16.0}
    return {f"r{i}": dict(odds) for i in range(n_races)}


def _synthetic_payouts(tansho: dict, bet_type: str, combo, takeout: float) -> dict:
    """各レースで combo が的中し、確定オッズ = (1-takeout)/P_harville(combo) だったと仮定。

    この合成データに対しては逆算 takeout が厳密に元の takeout を復元するはず。
    """
    out: dict = {}
    for race_id, odds_map in tansho.items():
        implied = {u: 1.0 / o for u, o in odds_map.items()}
        win_probs = harville.normalize(implied)
        p = harville.combo_probability(bet_type, win_probs, list(combo))
        actual = (1.0 - takeout) / p
        out[(race_id, bet_type, combo_key(bet_type, combo))] = actual
    return out


class TestWinnerReturnRate:
    def test_recovers_one_minus_takeout(self):
        tansho = _tansho(1)
        payouts = _synthetic_payouts(tansho, BetType.UMAREN, (1, 2), takeout=0.225)
        (key, actual), = payouts.items()
        race_id, bet_type, combo_str = key
        combo = [int(x) for x in combo_str.split("-")]
        r = winner_return_rate(tansho, race_id, bet_type, combo, actual)
        assert r is not None
        assert math.isclose(r, 1 - 0.225, rel_tol=1e-9)

    def test_missing_race_returns_none(self):
        assert winner_return_rate({}, "rX", BetType.UMAREN, [1, 2], 10.0) is None

    def test_missing_horse_returns_none(self):
        tansho = {"r0": {1: 2.0, 2: 4.0}}
        # 馬番 9 は単勝に無い → KeyError を None に
        assert winner_return_rate(tansho, "r0", BetType.UMAREN, [1, 9], 10.0) is None


class TestCalibrateTakeout:
    def test_recovers_known_takeout(self):
        tansho = _tansho(40)
        payouts = _synthetic_payouts(tansho, BetType.SANRENTAN, (1, 2, 3), takeout=0.25)
        calib = calibrate_takeout_from_payouts(tansho, payouts, min_samples=20)
        info = calib[BetType.SANRENTAN]
        assert info["source"] == "calibrated"
        assert info["n"] == 40
        assert abs(info["takeout"] - 0.25) < 1e-3

    def test_falls_back_to_nominal_when_few_samples(self):
        tansho = _tansho(3)
        payouts = _synthetic_payouts(tansho, BetType.UMAREN, (1, 2), takeout=0.5)
        calib = calibrate_takeout_from_payouts(tansho, payouts, min_samples=20)
        info = calib[BetType.UMAREN]
        assert info["source"] == "nominal"
        # 公称（馬連 0.225）に張り付き、合成の 0.5 は採用されない
        assert abs(info["takeout"] - 0.225) < 1e-9

    def test_all_calibratable_types_present(self):
        calib = calibrate_takeout_from_payouts({}, {})
        for bt in (
            BetType.FUKUSHO, BetType.UMAREN, BetType.UMATAN,
            BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
        ):
            assert bt in calib
            assert calib[bt]["source"] == "nominal"

    def test_wakuren_excluded(self):
        # 枠連は Harville 非対応のため較正対象に含まれない
        calib = calibrate_takeout_from_payouts({}, {})
        assert BetType.WAKUREN not in calib

    def test_takeout_map_extracts_floats(self):
        calib = calibrate_takeout_from_payouts({}, {})
        m = takeout_map(calib)
        assert all(isinstance(v, float) for v in m.values())
        assert m[BetType.SANRENTAN] == 0.25


class TestPersistence:
    def test_save_then_load_latest(self, tmp_path):
        path = takeout_calibration_path(str(tmp_path))
        calib = {
            BetType.UMAREN: {"takeout": 0.21, "n": 100, "source": "calibrated"},
            BetType.SANRENTAN: {"takeout": 0.26, "n": 80, "source": "calibrated"},
        }
        save_takeout_calibration(calib, path)
        m = latest_takeout_map(path)
        assert m[BetType.UMAREN] == 0.21
        assert m[BetType.SANRENTAN] == 0.26

    def test_load_missing_returns_empty(self, tmp_path):
        assert latest_takeout_map(str(tmp_path / "nope.json")) == {}

    def test_same_day_replaces(self, tmp_path):
        path = takeout_calibration_path(str(tmp_path))
        save_takeout_calibration({BetType.UMAREN: {"takeout": 0.21, "n": 1, "source": "calibrated"}}, path)
        save_takeout_calibration({BetType.UMAREN: {"takeout": 0.23, "n": 2, "source": "calibrated"}}, path)
        assert latest_takeout_map(path)[BetType.UMAREN] == 0.23
