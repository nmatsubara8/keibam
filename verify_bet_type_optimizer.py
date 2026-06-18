"""券種別パラメータ最適化の動作確認スクリプト（合成データ E2E）。

モデル予測が市場（単勝オッズ）より真の勝率に近い合成市場を作り、
HistoricalOddsProvider → ExpectedValueBetPolicy → Simulator の最適化経路で
券種別パラメータ（EV 閾値・温度）をグリッド探索し、既定パラメータより
回収率が改善することを確認する。

実データが無い環境での「枠組み」の動作確認であり、実運用では
featured_data + return_tables で同じ optimize_all を回す。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app._bet_type_optimizer import backtest_bet_type
from app._bet_type_optimizer import optimize_bet_type
from app._bet_type_optimizer import results_to_frame
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols
from src.policies import _harville as harville
from src.policies._bet_type_params import default_params
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB

TAKEOUT = 0.2


class _FakeAI:
    def __init__(self, table: pd.DataFrame) -> None:
        self._table = table

    def calc_score(self, X, policy):  # noqa: ANN001
        return self._table


class _FakeReturnProcessor:
    def __init__(self, tables: dict) -> None:
        self.preprocessed_data = tables


def build_market(n_races=200, n_horses=8, seed=5):
    """モデル≈真値・市場=平坦+ノイズ の合成市場と umaren 払戻を作る。"""
    rng = np.random.default_rng(seed)
    score_rows, score_idx = [], []
    umaren_rows = {}

    for r in range(n_races):
        rid = 202601010000 + r
        # 真の勝率（人気に偏り）
        true_p = rng.dirichlet(np.linspace(2.5, 0.5, n_horses))
        # 市場勝率: 真値を平坦化（^0.6）してノイズ → 市場は真値より精度が低い
        mkt = true_p ** 0.6 * rng.uniform(0.85, 1.15, n_horses)
        mkt = mkt / mkt.sum()
        tansho_odds = (1.0 - TAKEOUT) / np.clip(mkt, 1e-3, None)
        # モデル勝率: 真値 + 小ノイズ（市場よりは真値に近い）
        model = np.clip(true_p + rng.normal(0, 0.01, n_horses), 1e-4, None)
        model = model / model.sum()

        for h in range(n_horses):
            score_rows.append({ResultsCols.UMABAN: h + 1, PROB: float(model[h]),
                               CURRENT_ODDS: float(tansho_odds[h])})
            score_idx.append(str(rid))

        # Plackett-Luce で着順をサンプリング（真の勝率）→ 1-2 着
        order = rng.choice(np.arange(1, n_horses + 1), size=2, replace=False,
                           p=true_p)
        first, second = int(order[0]), int(order[1])
        # 払戻 = 当該組合せの市場フェア倍率（馬連）
        mkt_map = {i + 1: float(mkt[i]) for i in range(n_horses)}
        q = harville.combo_probability(BetType.UMAREN, mkt_map, (first, second))
        payout = (1.0 - TAKEOUT) / max(q, 1e-6) * 100.0
        umaren_rows[rid] = {"win_0": tuple(sorted((first, second))), "return_0": round(payout)}

    score_table = pd.DataFrame(score_rows, index=score_idx)
    score_table.index.name = "race_id"

    tables = {bt: pd.DataFrame() for bt in (
        BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN, BetType.UMAREN,
        BetType.UMATAN, BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
    )}
    tables[BetType.UMAREN] = pd.DataFrame.from_dict(umaren_rows, orient="index")
    return _FakeAI(score_table), _FakeReturnProcessor(tables)


def main() -> int:
    ai, rp = build_market()
    bet_type = BetType.UMAREN

    # 既定パラメータでのバックテスト
    base_params = default_params(bet_type)
    base_summary, _ = backtest_bet_type(ai, pd.DataFrame(), rp, bet_type, base_params)

    print(f"=== 券種: {bet_type} / 200 レース ===")
    print(f"  既定パラメータ: ev_threshold={base_params.ev_threshold}, temperature=1.0")
    if base_summary:
        print(f"    → 回収率 {base_summary['return_rate'] * 100:.1f}% / "
              f"的中率 {base_summary['hit_rate'] * 100:.1f}% / 賭け {base_summary['n_bets']} 枚")
    else:
        print("    → 賭け不成立")
    print()

    # グリッド最適化
    res = optimize_bet_type(
        ai, pd.DataFrame(), rp, bet_type,
        grid={"ev_thresholds": [1.0, 1.1, 1.3, 1.6, 2.0],
              "temperatures": [0.7, 1.0, 1.3, 1.6], "prob_scales": [1.0]},
        objective="return_rate", min_bets=20,
    )
    df = results_to_frame(res)
    print("=== グリッド探索（回収率 上位 6）===")
    if not df.empty:
        top = df.head(6).copy()
        top["return_rate"] = (top["return_rate"] * 100).round(1)
        top["hit_rate"] = (top["hit_rate"] * 100).round(1)
        print(top[["ev_threshold", "temperature", "return_rate", "hit_rate", "n_bets"]]
              .to_string(index=False))
    print()

    best = res["best_params"]
    best_rr = res["best_summary"].get("return_rate")
    print("=== 判定 ===")
    if best is None:
        print("  [WARN] min_bets を満たす組合せが無かった")
        return 2
    base_rr = base_summary.get("return_rate", 0.0) if base_summary else 0.0
    print(f"  最適: ev_threshold={best.ev_threshold}, temperature={best.temperature}"
          f" → 回収率 {best_rr * 100:.1f}%")
    print(f"  既定: 回収率 {base_rr * 100:.1f}%")
    if best_rr >= base_rr:
        print(f"  [OK] 最適化で回収率が改善（+{(best_rr - base_rr) * 100:.1f} ポイント）")
        ok = True
    else:
        print("  [WARN] 最適化が既定を下回った（合成パラメータ依存）")
        ok = False
    print()
    print("=== 完了 ===")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
