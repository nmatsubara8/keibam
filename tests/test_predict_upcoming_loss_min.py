"""predict_upcoming の損失最小化ゲート配線（_apply_loss_min_gate）の単体テスト。

run_prediction が返した候補に対し、ゲートが (a)無効時は素通し、(b)有効時は券種/EV/OOS/控除で
構造的に見送る、を固定する。スクレイプ・モデルは介さない純粋関数テスト。
"""
from __future__ import annotations

from predict_upcoming import _apply_loss_min_gate
from src.constants._bet_types import BetType
from src.policies._bet_candidate import BetCandidate
from src.policies._loss_minimization import (
    LossMinimizationConfig,
    calibrated_takeout_fn,
)
from src.constants._takeout import takeout


def _cand(bet_type=BetType.TANSHO, ev=1.2, prob=0.3, combo=(5,), race_id="r1"):
    return BetCandidate(
        race_id=race_id, bet_type=bet_type, combo=combo,
        probability=prob, odds=ev / prob, expected_value=ev,
    )


def _gate(cfg=None, takeout_of=takeout, is_oos=True):
    return (cfg or LossMinimizationConfig(), takeout_of, is_oos)


def test_gate_none_is_passthrough():
    cands = [_cand(ev=0.5), _cand(bet_type=BetType.SANRENTAN, ev=9.0)]
    assert _apply_loss_min_gate("r1", cands, None) == cands


def test_gate_keeps_valid_tansho():
    kept = _apply_loss_min_gate("r1", [_cand(ev=1.2)], _gate())
    assert len(kept) == 1 and kept[0].expected_value == 1.2


def test_gate_denies_negative_ev():
    assert _apply_loss_min_gate("r1", [_cand(ev=0.9)], _gate()) == []


def test_gate_in_sample_denies_all():
    # threshold_is_oos=False → require_oos_threshold により全件見送り
    cands = [_cand(ev=5.0), _cand(ev=3.0, combo=(7,))]
    assert _apply_loss_min_gate("r1", cands, _gate(is_oos=False)) == []


def test_gate_respects_ev_margin():
    cfg = LossMinimizationConfig(ev_safety_margin=0.15)  # 必要EV=1.15
    kept = _apply_loss_min_gate("r1", [_cand(ev=1.10), _cand(ev=1.20, combo=(8,))], _gate(cfg))
    assert [c.expected_value for c in kept] == [1.20]


def test_gate_denies_disallowed_bet_type():
    cands = [_cand(bet_type=BetType.SANRENTAN, ev=3.0, combo=(1, 2, 3))]
    assert _apply_loss_min_gate("r1", cands, _gate()) == []


def test_gate_uses_effective_takeout():
    # 実効控除で複勝が上限超え → 見送り（単勝は公称0.20で通過）
    eff = calibrated_takeout_fn({BetType.FUKUSHO: 0.234})
    cfg = LossMinimizationConfig(
        allowed_bet_types=(BetType.TANSHO, BetType.FUKUSHO), max_takeout=0.22
    )
    cands = [_cand(bet_type=BetType.FUKUSHO, ev=1.3, prob=0.5)]
    assert _apply_loss_min_gate("r1", cands, _gate(cfg, takeout_of=eff)) == []
    assert len(_apply_loss_min_gate("r1", cands, _gate(cfg))) == 1  # 公称なら通る
