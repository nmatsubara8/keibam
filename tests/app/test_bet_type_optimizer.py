"""券種別パラメータ最適化（app._bet_type_optimizer）のテスト。

合成 EV スコア表（較正勝率 + 単勝オッズ）と合成払戻テーブルを DI し、
HistoricalOddsProvider → ExpectedValueBetPolicy → Simulator の最適化経路を検証する。
"""

import pandas as pd

from app._bet_type_optimizer import backtest_bet_type
from app._bet_type_optimizer import default_grid
from app._bet_type_optimizer import optimize_all
from app._bet_type_optimizer import optimize_bet_type
from app._bet_type_optimizer import results_to_frame
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies._bet_type_params import BetTypeParams
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB


# モデル勝率が市場（単勝オッズ）より人気側に確信的 → 連系 EV>1 が成立する設計。
_MODEL_PROBS = {1: 0.45, 2: 0.30, 3: 0.12, 4: 0.08, 5: 0.05}
_TANSHO_ODDS = {1: 5.0, 2: 8.0, 3: 10.0, 4: 15.0, 5: 25.0}


class _FakeAI:
    """calc_score が固定 EV スコア表を返すスタブ KeibaAI。"""

    def __init__(self, table: pd.DataFrame) -> None:
        self._table = table

    def calc_score(self, X, policy):  # noqa: ANN001 — policy は無視（EV 表を直接返す）
        return self._table


class _FakeReturnProcessor:
    def __init__(self, tables: dict) -> None:
        self.preprocessed_data = tables


def _ev_score_table(n_races=25) -> pd.DataFrame:
    rows = []
    index = []
    for r in range(n_races):
        rid = str(202601010000 + r)
        for u in range(1, 6):
            rows.append({ResultsCols.UMABAN: u, PROB: _MODEL_PROBS[u], CURRENT_ODDS: _TANSHO_ODDS[u]})
            index.append(rid)
    df = pd.DataFrame(rows, index=index)
    df.index.name = "race_id"
    return df


def _return_tables(n_races=25, umaren_return=900) -> _FakeReturnProcessor:
    """全 8 券種の払戻テーブル。各レースで馬番 1-2 が 1-2 着（umaren (1,2) 的中）。"""
    def _empty():
        return pd.DataFrame()

    umaren_rows = {}
    umatan_rows = {}
    for r in range(n_races):
        rid = 202601010000 + r
        umaren_rows[rid] = {"win_0": (1, 2), "return_0": umaren_return}
        umatan_rows[rid] = {"win_0": (1, 2), "return_0": umaren_return * 2}
    tables = {bt: _empty() for bt in (
        BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN, BetType.UMAREN,
        BetType.UMATAN, BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
    )}
    tables[BetType.UMAREN] = pd.DataFrame.from_dict(umaren_rows, orient="index")
    tables[BetType.UMATAN] = pd.DataFrame.from_dict(umatan_rows, orient="index")
    return _FakeReturnProcessor(tables)


def test_backtest_bet_type_places_umaren_bets():
    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    params = BetTypeParams(ev_threshold=1.0)
    summary, per_race = backtest_bet_type(ai, pd.DataFrame(), rp, BetType.UMAREN, params)
    assert summary, "umaren で賭けが成立しない"
    assert summary["n_bets"] > 0
    assert summary["return_rate"] > 0  # (1,2) が的中するため回収あり
    assert not per_race.empty


def test_compare_calibration_backtest_structure():
    from app._bet_type_optimizer import compare_calibration_backtest

    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    calibrated = {BetType.UMATAN: 0.28, BetType.SANRENTAN: 0.28}  # 順序系を高め
    df = compare_calibration_backtest(
        ai, pd.DataFrame(), rp, calibrated, bet_types=[BetType.UMAREN, BetType.UMATAN],
    )
    assert set(df["bet_type"]) == {BetType.UMAREN, BetType.UMATAN}
    for col in ("n_nominal", "return_nominal", "n_calibrated", "return_calibrated", "delta_return"):
        assert col in df.columns
    # 馬連は較正 takeout を渡していない → nominal と同条件で n が一致する
    umaren = df[df["bet_type"] == BetType.UMAREN].iloc[0]
    assert umaren["n_nominal"] == umaren["n_calibrated"]


def test_optimize_bet_type_returns_best():
    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    res = optimize_bet_type(
        ai, pd.DataFrame(), rp, BetType.UMAREN,
        grid={"ev_thresholds": [1.0, 1.5, 2.0], "temperatures": [1.0, 1.6], "prob_scales": [1.0]},
        objective="return_rate", min_bets=1,
    )
    assert res["bet_type"] == BetType.UMAREN
    assert res["results"], "グリッド探索結果が空"
    assert isinstance(res["best_params"], BetTypeParams)
    # best は results 中で return_rate 最大
    best_rr = res["best_summary"]["return_rate"]
    assert best_rr == max(r["summary"]["return_rate"] for r in res["results"]
                          if r["summary"].get("n_bets", 0) >= 1)


def test_optimize_bet_type_min_bets_unmet_returns_none():
    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    res = optimize_bet_type(
        ai, pd.DataFrame(), rp, BetType.UMAREN,
        grid={"ev_thresholds": [1.0], "temperatures": [1.0], "prob_scales": [1.0]},
        min_bets=10_000,  # 到底満たせない
    )
    assert res["best_params"] is None
    assert res["best_summary"] == {}


def test_results_to_frame_sorted():
    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    res = optimize_bet_type(
        ai, pd.DataFrame(), rp, BetType.UMAREN,
        grid={"ev_thresholds": [1.0, 1.5], "temperatures": [1.0, 1.6], "prob_scales": [1.0]},
        min_bets=1,
    )
    df = results_to_frame(res)
    assert not df.empty
    assert list(df.columns)[:3] == ["ev_threshold", "temperature", "prob_scale"]
    # return_rate 降順
    assert df["return_rate"].is_monotonic_decreasing


def test_optimize_all_smoke():
    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    params_map, metrics_map, all_results = optimize_all(
        ai, pd.DataFrame(), rp,
        bet_types=[BetType.UMAREN, BetType.WIDE],
        grid={"ev_thresholds": [1.0], "temperatures": [1.0], "prob_scales": [1.0]},
        min_bets=1,
    )
    # umaren は payout あり → best_params が探索由来、wide は payout 無し → 既定値
    assert BetType.UMAREN in params_map and BetType.WIDE in params_map
    assert isinstance(params_map[BetType.UMAREN], BetTypeParams)


def test_default_grid_shape():
    g = default_grid()
    assert g["ev_thresholds"] and g["temperatures"]
