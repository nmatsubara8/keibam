"""券種別バックテストの動作確認スクリプト（合成データ E2E）。

実 featured_data / 払戻テーブルが無い環境でも、バックテストページ（5_backtest.py の
「券種別バックテスト」タブ）と同一の計算経路を合成データで駆動して検証する:

    recent_race_slice 相当のテスト期間
      → ai.calc_score（StdScorePolicy）→ BetPolicy*.judge → Simulator.calc_returns
      （実払戻テーブルの代わりに合成払戻テーブルを DI）

全 8 券種（単勝・複勝・枠連・馬連・馬単・ワイド・三連複・三連単）で
回収率・的中率・賭け枚数が算出されることを確認する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from app._model_compare import BET_POLICY_CHOICES
from app._model_compare import cumulative_profit
from app._model_compare import simulate_model
from src.constants._bet_types import BetType
from src.constants._results_cols import ResultsCols


class _FakeAI:
    """calc_score が固定スコア表を返すスタブ KeibaAI。"""

    def __init__(self, score_table: pd.DataFrame) -> None:
        self._score_table = score_table

    def calc_score(self, X, policy):  # noqa: ANN001
        return self._score_table


class _FakeReturnProcessor:
    def __init__(self, tables: dict) -> None:
        self.preprocessed_data = tables


def build_synthetic(n_races=60, n_horses=10, seed=3):
    """合成スコア表 + 全券種払戻テーブルを生成する。

    各レースで真の強さ順に着順を決め（上位ほど勝ちやすい）、スコアは真の強さ +
    ノイズ。払戻は人気（強さ）に応じたオッズ風の値。スコア上位馬が当たりやすいので
    BOX 馬券もそれなりに的中する。
    """
    rng = np.random.default_rng(seed)
    score_rows = []
    tansho, fukusho, wakuren, umaren, umatan, wide, sanpuku, santan = ({} for _ in range(8))

    for r in range(n_races):
        rid = int(f"2026010100{r:03d}")
        strength = rng.normal(size=n_horses)
        order = np.argsort(-strength)  # 強い順の馬 index
        umaban = np.arange(1, n_horses + 1)
        wakuban = np.minimum((umaban + 1) // 2, 8)  # 馬番→枠番（おおよそ）
        # スコア = 真の強さ + ノイズ（レース内標準化は calc_score 側の StdScorePolicy が担うが、
        # ここでは _FakeAI なので事前に標準化しておく）
        raw = strength + rng.normal(scale=0.5, size=n_horses)
        score = (raw - raw.mean()) / (raw.std() + 1e-9)
        for h in range(n_horses):
            score_rows.append({
                "race_id": str(rid), "score": float(score[h]),
                ResultsCols.UMABAN: int(umaban[h]), ResultsCols.WAKUBAN: int(wakuban[h]),
                "wakuban_flag": 1,
            })
        # 着順上位 3 頭（馬番）
        first, second, third = (int(umaban[order[i]]) for i in range(3))
        w1, w2, w3 = (int(wakuban[order[i]]) for i in range(3))

        def _odds(base):  # 適当な払戻（円）
            return int(base * rng.uniform(1.2, 3.0))

        tansho[rid] = {"win_0": first, "return_0": _odds(300)}
        fukusho[rid] = {"win_0": first, "return_0": _odds(130),
                        "win_1": second, "return_1": _odds(140),
                        "win_2": third, "return_2": _odds(150)}
        wakuren[rid] = {"win_0": tuple(sorted((w1, w2))), "return_0": _odds(800)}
        umaren[rid] = {"win_0": tuple(sorted((first, second))), "return_0": _odds(1500)}
        umatan[rid] = {"win_0": (first, second), "return_0": _odds(3000)}
        wide[rid] = {"win_0": tuple(sorted((first, second))), "return_0": _odds(500),
                     "win_1": tuple(sorted((first, third))), "return_1": _odds(600),
                     "win_2": tuple(sorted((second, third))), "return_2": _odds(700)}
        sanpuku[rid] = {"win_0": tuple(sorted((first, second, third))), "return_0": _odds(5000)}
        santan[rid] = {"win_0": (first, second, third), "return_0": _odds(30000)}

    score_table = pd.DataFrame(score_rows).set_index("race_id")
    score_table.index.name = "race_id"

    def _df(d):
        return pd.DataFrame.from_dict(d, orient="index")

    tables = {
        BetType.TANSHO: _df(tansho), BetType.FUKUSHO: _df(fukusho),
        BetType.WAKUREN: _df(wakuren), BetType.UMAREN: _df(umaren),
        BetType.UMATAN: _df(umatan), BetType.WIDE: _df(wide),
        BetType.SANRENPUKU: _df(sanpuku), BetType.SANRENTAN: _df(santan),
    }
    return score_table, _FakeReturnProcessor(tables)


def main() -> int:
    score_table, rp = build_synthetic()
    ai = _FakeAI(score_table)
    threshold = 0.8  # レース内標準化スコアの閾値

    print(f"=== 合成データ: {score_table.index.nunique()} レース / "
          f"{len(score_table)} 出走 / スコア閾値={threshold} ===")
    print(f"  {'券種':<8} {'回収率':>8} {'的中率':>8} {'賭け枚数':>8} "
          f"{'対象R':>6} {'損益':>10}")

    ok = True
    for label in BET_POLICY_CHOICES:
        summary, per_race, diag = simulate_model(
            ai, pd.DataFrame(), label, threshold, return_processor=rp
        )
        if not summary:
            print(f"  {label:<8} {'—':>8} (matched={diag['n_matched_races']}, "
                  f"covered={diag['n_covered_races']})")
            ok = False
            continue
        rr = summary["return_rate"]
        hr = summary["hit_rate"]
        profit = cumulative_profit(per_race).iloc[-1] if not per_race.empty else 0.0
        print(f"  {label:<8} {rr * 100:>7.1f}% {hr * 100:>7.1f}% "
              f"{summary['n_bets']:>8,} {summary['n_races']:>6} {profit:>10.1f}")

    print()
    print("=== 判定 ===")
    if ok:
        print("  [OK] 全 8 券種でバックテストが成立（回収率・的中率・損益を算出）")
    else:
        print("  [WARN] 一部券種で集計が空（合成データのスコア閾値・払戻を確認）")
    print()
    print("=== 完了 ===")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
