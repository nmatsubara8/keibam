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


def test_compare_calibration_backtest_ev_threshold_override():
    from app._bet_type_optimizer import compare_calibration_backtest

    ai = _FakeAI(_ev_score_table())
    rp = _return_tables()
    calibrated = {BetType.UMAREN: 0.2}
    # 低い閾値なら馬連の買い目が出る、高すぎる閾値なら 0 になる
    low = compare_calibration_backtest(
        ai, pd.DataFrame(), rp, calibrated, bet_types=[BetType.UMAREN], ev_threshold=1.0,
    ).iloc[0]
    high = compare_calibration_backtest(
        ai, pd.DataFrame(), rp, calibrated, bet_types=[BetType.UMAREN], ev_threshold=999.0,
    ).iloc[0]
    assert low["n_nominal"] >= high["n_nominal"]
    assert int(high["n_nominal"]) == 0


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


# --- Phase 2: Optuna(TPE) 規律版（時系列 train/val・頑健目的・prob_scale 連続探索）---

class _SliceAwareAI:
    """calc_score が featured_slice の race_id に対応する行だけ返す（train/val 分離を尊重）。"""

    def __init__(self, table: pd.DataFrame) -> None:
        self._table = table

    def calc_score(self, X, policy):  # noqa: ANN001
        if X is None or len(X) == 0:
            return self._table
        ids = set(X.index.astype(str))
        return self._table[self._table.index.astype(str).isin(ids)]


def _featured_index(n_races=25) -> pd.DataFrame:
    """race_id を index に持つダミー featured（time_split 用・順序は race_id で時系列）。"""
    ids = [str(202601010000 + r) for r in range(n_races)]
    df = pd.DataFrame({"_dummy": range(n_races)}, index=ids)
    df.index.name = "race_id"
    return df


def test_time_split_chronological_disjoint():
    from app._bet_type_optimizer import time_split
    feat = _featured_index(20).sample(frac=1.0, random_state=1)   # 入力順シャッフル
    train, val = time_split(feat, val_frac=0.3)
    tr_ids = sorted(train.index.astype(str))
    va_ids = sorted(val.index.astype(str))
    assert set(tr_ids).isdisjoint(va_ids)                          # 重複なし
    assert len(tr_ids) + len(va_ids) == 20
    assert max(tr_ids) < min(va_ids)                               # train は val より前（時系列）


def test_robust_metric_trimmed_drops_jackpot():
    from app._bet_type_optimizer import robust_metric
    per_race = pd.DataFrame(
        {"n_bets": [1, 1, 1, 1], "bet_amount": [1, 1, 1, 1],
         "return_amount": [0, 0, 0, 100], "hit_or_not": [0, 0, 0, 1]},
        index=["r1", "r2", "r3", "r4"],
    )
    summary = {"return_rate": 25.0, "sharpe_ratio": 0.5, "n_bets": 4}
    assert robust_metric(summary, per_race, "return_rate") == 25.0        # 生は万馬券込み
    assert robust_metric(summary, per_race, "trimmed_return_rate") == 0.0  # 最大払戻1本除外→0
    assert robust_metric(summary, per_race, "sharpe_ratio") == 0.5
    assert robust_metric({}, per_race, "return_rate") == float("-inf")     # 空 summary


def test_optimize_bet_type_tpe_reports_generalization():
    from app._bet_type_optimizer import optimize_bet_type_tpe
    ai = _SliceAwareAI(_ev_score_table(25))
    rp = _return_tables(25)
    res = optimize_bet_type_tpe(
        ai, _featured_index(25), rp, BetType.UMAREN,
        n_trials=6, bounds={"ev_threshold": (1.0, 1.2), "temperature": (0.8, 1.2),
                            "prob_scale": (0.9, 1.1)},
        objective="trimmed_return_rate", min_bets=1, val_frac=0.3, seed=0,
    )
    assert res["bet_type"] == BetType.UMAREN
    assert isinstance(res["best_params"], BetTypeParams)
    # 汎化判定に必要な val 系フィールドが揃う（最適化 vs 既定を out-of-sample で比較できる）
    for k in ("train_metric", "val_metric", "val_metric_default", "n_train_races", "n_val_races"):
        assert k in res
    assert res["n_train_races"] + res["n_val_races"] == 25
    assert res["n_val_races"] > 0


def test_optimize_bet_type_tpe_min_bets_unmet_returns_none():
    from app._bet_type_optimizer import optimize_bet_type_tpe
    ai = _SliceAwareAI(_ev_score_table(25))
    rp = _return_tables(25)
    res = optimize_bet_type_tpe(
        ai, _featured_index(25), rp, BetType.UMAREN,
        n_trials=4, min_bets=10_000, seed=0,      # 到底満たせない
    )
    assert res["best_params"] is None
