"""券種戦略 ROI の walk-forward 選択規律テスト（proven edge 無ければ S0＝賭けない）。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "sim_ticket", Path(__file__).resolve().parents[2] / "scripts" / "sim_ticket_strategy_roi.py")
sim = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sim)

_S0 = {"name": "S0_skip", "n_bets": 0, "roi_ex": 0.0, "ci_lo": 0.0}


def test_all_losing_selects_skip():
    # 全戦略が CI下限<1（黒字でない）→ 負けの中で最良の券でなく S0（賭けない）を選ぶ。
    rows = [{"name": "単勝1位", "n_bets": 3738, "roi_ex": 0.79, "ci_lo": 0.65},
            {"name": "S4", "n_bets": 29870, "roi_ex": 0.39, "ci_lo": 0.18}, _S0]
    pick, why = sim.select_bet_or_skip(rows, _S0)
    assert pick["name"] == "S0_skip" and "賭けない" in why


def test_proven_edge_is_selected():
    rows = [{"name": "妙味X", "n_bets": 500, "roi_ex": 1.15, "ci_lo": 1.04}, _S0]
    pick, why = sim.select_bet_or_skip(rows, _S0)
    assert pick["name"] == "妙味X" and "proven edge" in why


def test_high_roi_but_insignificant_ci_skips():
    # 除最大ROI>1 でも CI下限<=1（有意でない・まぐれ）なら賭けない。
    rows = [{"name": "まぐれ", "n_bets": 120, "roi_ex": 1.30, "ci_lo": 0.80}, _S0]
    pick, _ = sim.select_bet_or_skip(rows, _S0)
    assert pick["name"] == "S0_skip"


def test_low_bet_count_excluded_then_skip():
    # 点数不足(min_bets 未満)の戦略は候補外＝S0。
    rows = [{"name": "少数", "n_bets": 50, "roi_ex": 2.0, "ci_lo": 1.5}, _S0]
    pick, _ = sim.select_bet_or_skip(rows, _S0, min_bets=100)
    assert pick["name"] == "S0_skip"
