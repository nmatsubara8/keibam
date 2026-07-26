"""オッズ力学モデル評価の動作確認スクリプト（合成スナップショット E2E）。

実スナップショット（VPS cron で 4〜8 週蓄積）が無い環境でも、
`evaluate-odds-dynamics` CLI と同一の計算チェーンを合成データで駆動して検証する:

    OddsSnapshot 群（複数レース×複数フェーズ）
      → snapshots_to_phase_table → race_share_sequences
      → race_winners（results から勝ち馬番）
      → evaluate_dynamics_models（identity/dirichlet/kalman/particle/ensemble）

確認項目:
  1. 全モデル + アンサンブルの KL / シェアMAE / オッズMAPE が算出される
  2. **winner_logloss が NaN でなく埋まる**（CLI への winners 配線の検証）
  3. 締切に向け人気が収斂する「市場の重力」下で、力学モデル/アンサンブルが
     identity（最新観測をそのまま使う素朴予測）の KL を下回り得る

合成市場: CLR 空間のランダムウォークで 1 番人気に firming drift を入れ、
各フェーズのシェアから単勝オッズ（控除率込み）を生成する。
"""

from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.constants._odds_phases import PHASE_MAX_MINUTES
from src.constants._odds_phases import PHASE_TIMELINE
from src.constants._results_cols import ResultsCols
from src.preparing._odds_snapshot import make_snapshot
from src.training._odds_dynamics_eval import evaluate_dynamics_models
from src.training._odds_dynamics_eval import race_winners
from src.training._odds_feature_builder import snapshots_to_phase_table
from src.training._simplex import race_share_sequences

TAKEOUT = 0.20  # 単勝控除率（約 20%）


def _clr_inv(x: np.ndarray) -> np.ndarray:
    e = np.exp(x - x.max())
    return e / e.sum()


def build_snapshots_and_results(n_races=120, n_horses=10, vol=0.04,
                                fav_drift=0.10, seed=11):
    """合成 OddsSnapshot 群と results（勝ち馬）を生成する。

    各レースで CLR ランダムウォークによりフェーズ順（前日→T0）にシェアを進め、
    1 番人気に firming drift を与える。シェアから単勝オッズを生成して
    フェーズごとに OddsSnapshot を作る。勝ち馬は T0 シェアからサンプリングする。
    """
    rng = np.random.default_rng(seed)
    post = dt.datetime(2026, 1, 10, 15, 0, 0)
    snapshots = []
    result_rows = []
    # 前日→T0 の順（古い→新しい）に進める。各フェーズの captured_at を逆算。
    phases_old_to_new = list(PHASE_TIMELINE)
    for r in range(n_races):
        race_id = f"2026011000{r:03d}"  # str（snapshot 契約に合わせる）
        base = np.sort(rng.uniform(0.3, 2.5, n_horses))[::-1]
        x = np.log(_clr_inv(base) + 1e-9)
        fav = int(np.argmax(x))
        last_shares = None
        for phase in phases_old_to_new:
            x = x + rng.normal(0, vol, n_horses)
            x[fav] += fav_drift  # 人気側に資金集中（市場の重力）
            shares = _clr_inv(x)
            last_shares = shares
            mins = max(1, PHASE_MAX_MINUTES[phase] - 1)
            captured = post - dt.timedelta(minutes=mins)
            for h in range(n_horses):
                odds = float((1.0 - TAKEOUT) / max(shares[h], 1e-4))
                snapshots.append(
                    make_snapshot(race_id, BetType.TANSHO, (h + 1,), odds, post, captured)
                )
        # 勝ち馬を T0 シェアからサンプリング（馬番は 1 始まり）
        winner = int(rng.choice(n_horses, p=last_shares)) + 1
        for h in range(n_horses):
            result_rows.append({
                "race_id": int(race_id),
                ResultsCols.RANK: 1 if (h + 1) == winner else h + 2,
                ResultsCols.UMABAN: h + 1,
            })
    return snapshots, pd.DataFrame(result_rows)


def main() -> int:
    snapshots, results = build_snapshots_and_results()
    print(f"=== 合成スナップショット: {len(snapshots)} 行 / "
          f"results {len(results)} 行 ===")

    table = snapshots_to_phase_table(snapshots, BetType.TANSHO)
    sequences = race_share_sequences(table)
    winners = race_winners(results)
    print(f"  race 系列数 = {len(sequences)} / 勝ち馬導出 = {len(winners)} レース")
    sample = next(iter(winners.items()))
    print(f"  winners サンプル: race_id={sample[0]} -> umaban={sample[1]}")
    print()

    evaluation = evaluate_dynamics_models(sequences, holdout_frac=0.2, winners=winners)
    results_metrics = evaluation["results"]
    weights = evaluation["ensemble_weights"]

    print(f"=== 評価結果（holdout=0.2 / n_train={evaluation['n_train_races']}）===")
    print(f"  {'model':<10} {'KL↓':>8} {'MAE↓':>8} {'MAPE↓':>8} "
          f"{'winLL↓':>8} {'ens_w':>7}")
    for name, m in results_metrics.items():
        w = weights.get(name)
        wtxt = f"{w:.3f}" if w is not None else "   -"
        print(f"  {name:<10} {m['kl_mean']:>8.4f} {m['share_mae']:>8.4f} "
              f"{m['odds_mape']:>8.3f} {m['winner_logloss']:>8.4f} {wtxt:>7}")
    print()

    # 判定
    print("=== 判定 ===")
    ok = True
    # 1. winner_logloss が全モデルで有限（CLI 配線の検証）
    if all(np.isfinite(m["winner_logloss"]) for m in results_metrics.values()):
        print("  [OK] winner_logloss が全モデルで算出（winners 配線が機能）")
    else:
        print("  [NG] winner_logloss に NaN あり（winners 未配線）")
        ok = False
    # 2. KL/MAE/MAPE が全モデルで有限
    if all(np.isfinite(m["kl_mean"]) and np.isfinite(m["odds_mape"])
           for m in results_metrics.values()):
        print("  [OK] KL / シェアMAE / オッズMAPE が全モデルで算出")
    else:
        print("  [NG] KL/MAE/MAPE に NaN あり")
        ok = False
    # 3. アンサンブル重みが正規化
    if abs(sum(weights.values()) - 1.0) < 1e-6:
        print(f"  [OK] アンサンブル重み Σ=1（{ {k: round(v,3) for k,v in weights.items()} }）")
    else:
        print("  [NG] アンサンブル重みが正規化されていない")
        ok = False
    # 4. 力学モデル/アンサンブルが identity を KL で下回るか（情報目的）
    id_kl = results_metrics["identity"]["kl_mean"]
    best_dyn = min((n for n in results_metrics if n != "identity"),
                   key=lambda n: results_metrics[n]["kl_mean"])
    best_kl = results_metrics[best_dyn]["kl_mean"]
    if best_kl < id_kl:
        print(f"  [OK] {best_dyn} KL {best_kl:.4f} < identity {id_kl:.4f}"
              "（力学モデルが素朴予測に勝つ）")
    else:
        print(f"  [INFO] identity KL {id_kl:.4f} が最小（合成パラメータ依存。"
              "実データでの蓄積後評価が本番）")

    print()
    print("=== 完了 ===")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
