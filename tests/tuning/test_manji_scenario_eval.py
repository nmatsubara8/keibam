"""①.5 Step4 シナリオ OOS 選抜ハーネスのテスト（決定的 predict_fn スタブ）。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.tuning._manji_scenario_eval import (
    evaluate_scenario,
    numeric_feature_cols,
    walk_forward_roi,
)


def _stub_predict(train, test, feature_cols, seed):
    """manji_score が特徴量にあればそれで勝率を決める（無ければ中立0.5）。

    ＝『manji 補正が見えるモデルは勝ち馬を当て、見えない baseline は当てられない』を模す。
    """
    if "manji_score" in feature_cols:
        m = pd.to_numeric(test["manji_score"], errors="coerce").fillna(0.0).to_numpy()
        return 1.0 / (1.0 + np.exp(-m))
    return np.full(len(test), 0.5)


def _scenario_df(n_races=60):
    """3頭立て・全馬オッズ3.0。各レース1頭が勝ち、その馬だけ manji_score=+2（他 −1）。

    manji_score が勝ち馬を示す（当てれば単勝3.0）。baseline は全馬同確率で妙味を見抜けない。
    """
    rows = []
    for i in range(n_races):
        winner = i % 3
        for h in range(3):
            rows.append({
                "race_id": f"r{i:03d}",
                "horse_id": f"h{i}_{h}",
                ResultsCols.UMABAN: h + 1,
                ResultsCols.RANK: 1 if h == winner else 2,
                ResultsCols.TANSHO_ODDS: 3.0,
                "date": pd.Timestamp("2020-01-01") + pd.Timedelta(days=i),
                "manji_score": 2.0 if h == winner else -1.0,
            })
    return pd.DataFrame(rows).set_index("race_id")


def test_numeric_feature_cols_excludes_targets_and_odds():
    df = _scenario_df(3)
    full = numeric_feature_cols(df, include_manji=True)
    base = numeric_feature_cols(df, include_manji=False)
    assert "manji_score" in full and "manji_score" not in base
    for bad in (ResultsCols.RANK, ResultsCols.TANSHO_ODDS, "date", "horse_id"):
        assert bad not in full


def test_walk_forward_roi_uses_manji_signal():
    df = _scenario_df()
    full = walk_forward_roi(df, feature_cols=["manji_score"], folds=5,
                            ev_threshold=1.0, predict_fn=_stub_predict)
    # 勝ち馬(manji高)だけ買って単勝3.0 → 回収率は 1 を大きく超える
    assert full["n_bets"] > 0
    assert full["roi"] > 2.0


def test_evaluate_scenario_lift_and_placebo():
    df = _scenario_df()
    res = evaluate_scenario(df, folds=5, ev_threshold=1.0,
                            predict_fn=_stub_predict, n_placebo=20, seed=0)
    # manji を見るモデルは baseline を上回る（lift>0）
    assert res["lift"] > 0
    assert res["roi"] > res["baseline_roi"]
    # placebo（manji_score shuffle）では優位が消える → 実 lift はほぼ最上位
    assert res["placebo_pct"] >= 0.9


def test_evaluate_scenarios_end_to_end_ranks_by_lift():
    """prepare_shared→build→evaluate の一連が走り、lift 降順の表を返す。"""
    from src.tuning._manji_scenario_eval import evaluate_scenarios

    # parity にエッジのある合成 featured（奇数が高オッズで勝つ）
    frames = []
    for bi in range(6):
        start = pd.Timestamp("2016-01-01") + pd.Timedelta(days=bi * 400)
        for uma, win_frac in ((1, 0.5), (2, 0.15)):
            nwin = int(round(80 * win_frac))
            ranks = [1] * nwin + [2] * (80 - nwin)
            frames.append(pd.DataFrame({
                "race_id": [f"b{bi}u{uma}r{i}" for i in range(80)],
                "horse_id": [f"h{bi}_{uma}_{i}" for i in range(80)],
                ResultsCols.UMABAN: uma,
                ResultsCols.RANK: ranks,
                ResultsCols.TANSHO_ODDS: 3.0,
                "date": pd.date_range(start, periods=80, freq="h"),
            }))
    featured = pd.concat(frames, ignore_index=True).set_index("race_id")

    from src.tuning._manji_scenario import Scenario
    import src.tuning._manji_scenario as scn_mod
    # 小さな評価用に一時シナリオを登録
    scn_mod.SCENARIOS["_test_parity"] = Scenario("_test_parity", factors=("umaban_parity",))
    try:
        table = evaluate_scenarios(
            featured, ["_test_parity"], n_blocks=6, folds=5, ev_threshold=1.0,
            predict_fn=_stub_predict, n_placebo=5, seed=0,
        )
    finally:
        scn_mod.SCENARIOS.pop("_test_parity", None)
    assert list(table.columns)[:4] == ["scenario", "roi", "baseline_roi", "lift"]
    assert (table["lift"].values == np.sort(table["lift"].values)[::-1]).all()  # lift 降順
