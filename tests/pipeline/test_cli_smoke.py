"""run_pipeline の CLI 配線スモークテスト（handler 分割のリグレッション網）。

handler を commands/ パッケージへ分割しても「全サブコマンドが handler に解決し、handler が
import 可能・呼び出し可能」であることを保証する。各 handler の本体ロジックは個別テストに委ね、
ここでは配線（パーサ subcommand ⇔ HANDLERS の網羅、import 健全性）だけを検証する。
"""

from __future__ import annotations

import argparse

from src.pipeline import run_pipeline as rp
from src.pipeline._cli_parser import build_parser


def _subcommand_names() -> set[str]:
    parser = build_parser()
    sub = next(
        a for a in parser._subparsers._group_actions
        if isinstance(a, argparse._SubParsersAction)
    )
    return set(sub.choices)


def test_every_subcommand_has_handler():
    """build_parser の全サブコマンドが HANDLERS に存在し、逆も成り立つ（ドリフト検知）。"""
    assert _subcommand_names() == set(rp.HANDLERS)


def test_all_handlers_callable():
    """登録 handler はすべて callable（import 漏れ・未定義参照を early に検出）。"""
    for name, handler in rp.HANDLERS.items():
        assert callable(handler), f"{name} の handler が callable でない"


def test_parse_args_dispatches_known_jobs():
    """各サブコマンドが job 名として解析でき、HANDLERS に対応する。"""
    minimal = {
        "ingest": ["ingest", "--race-id", "1"],
        "fetch-final-odds": ["fetch-final-odds", "--from-results"],
        "calibrate-ev": ["calibrate-ev"],
        "backtest": ["backtest"],
        "build-combined": ["build-combined", "--gbdt-model", "g.pkl", "--nn-model", "n.pkl"],
    }
    for name in _subcommand_names():
        argv = minimal.get(name, [name])
        args = rp._parse_args(argv)
        assert args.job == name
        assert name in rp.HANDLERS


def test_retrain_holdout_years_parsed():
    """retrain --holdout-years が複数年を受け取り int リストになる（既定 None）。"""
    p = build_parser()
    assert p.parse_args(["retrain", "--holdout-years", "2024", "2025"]).holdout_years == [2024, 2025]
    assert p.parse_args(["retrain"]).holdout_years is None
