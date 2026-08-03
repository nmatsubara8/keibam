"""市場/B5/J41 paired ROI 比較ハーネスの純部テスト（本命選択・非証拠診断）。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "paired", Path(__file__).resolve().parents[2] / "scripts" / "run_jrdb42_paired_roi_reference.py")
paired = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(paired)


def test_pick_favorite_and_payout():
    probs = {1: 0.5, 2: 0.3, 3: 0.2}
    odds = {1: 2.0, 2: 4.0, 3: 8.0}
    assert paired._pick(probs, odds, 1) == (1, 1, 2.0)      # 本命=1・的中→payout=odds
    assert paired._pick(probs, odds, 3) == (1, 0, 0.0)      # 本命=1・外れ→payout=0
    assert paired._pick({}, odds, 1) == (None, 0, 0.0)      # probs 空


def test_frozen_family_references_both_models():
    # paired 比較は B(5) と J41(41) の凍結スペックを参照する
    assert len(paired.B5["features"]) == 5
    assert len(paired.J41["features"]) == 41
    assert paired.B5["l2"] == 1.0 and paired.J41["l2"] == 1.0
