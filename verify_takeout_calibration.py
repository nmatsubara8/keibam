"""控除率較正の end-to-end 検証（実データと同じコード経路を合成データで通す）。

実レースデータが無い環境でも、`tansho_odds_by_race_from_table` /
`payout_lookup_from_return_processor` / `calibrate_takeout_from_payouts` /
`HistoricalOddsProvider` という**本番と同じ関数**を通して、

  1. 払戻実績から券種別の実効控除率が正しく逆算されるか（既知 takeout の復元）
  2. 較正値が連系推定オッズ（HistoricalOddsProvider）に反映されるか
  3. ライブ EV 選定（run_prediction）が較正値で変化するか

を確認する。合成データは Harville（Plackett-Luce）の生成モデルに厳密に従うため、
較正は理論上 1 - t_true を不偏推定する（Σ_c P(c)=1）。

実行: python verify_takeout_calibration.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._bet_types import BetType
from src.policies import _harville as harville
from src.policies._takeout_calibration import calibrate_takeout_from_payouts
from src.policies._takeout_calibration import payout_lookup_from_return_processor
from src.policies._takeout_calibration import takeout_map
from src.policies._takeout_calibration import tansho_odds_by_race_from_table
from src.policies._odds_provider import HistoricalOddsProvider

# 券種ごとの「真の」控除率（これを払戻に埋め込み、復元できるか確かめる）
TRUE_TAKEOUT = {
    BetType.FUKUSHO: 0.20,
    BetType.UMAREN: 0.225,
    BetType.UMATAN: 0.225,
    BetType.WIDE: 0.225,
    BetType.SANRENPUKU: 0.25,
    BetType.SANRENTAN: 0.25,
}

N_RACES = 600
N_HORSES = 14
RNG = np.random.default_rng(42)


def _sample_order(umabans: list[int], probs: dict[int, float]) -> list[int]:
    """Plackett-Luce（=Harville の生成モデル）で着順を 1 つサンプルする。"""
    remaining = list(umabans)
    order: list[int] = []
    while remaining:
        w = np.array([probs[u] for u in remaining], dtype=float)
        w = w / w.sum()
        pick = RNG.choice(len(remaining), p=w)
        order.append(remaining.pop(pick))
    return order


def _winning_combos(order: list[int]) -> dict[str, list[tuple]]:
    """着順から各券種の的中組合せを導く（複勝/ワイドは複数）。"""
    a, b, c = order[0], order[1], order[2]
    return {
        BetType.FUKUSHO: [(a,), (b,), (c,)],
        BetType.UMAREN: [(a, b)],
        BetType.UMATAN: [(a, b)],
        BetType.WIDE: [(a, b), (a, c), (b, c)],
        BetType.SANRENPUKU: [(a, b, c)],
        BetType.SANRENTAN: [(a, b, c)],
    }


def _build_synthetic():
    """results テーブル + 払戻テーブル（ReturnProcessor 互換）を合成する。"""
    results_rows = []
    # 払戻: {bet_type: list of row dict}（後で DataFrame 化）
    payout_rows: dict[str, list[dict]] = {bt: [] for bt in TRUE_TAKEOUT}

    for r in range(N_RACES):
        race_id = f"2024{r:08d}"
        umabans = list(range(1, N_HORSES + 1))
        # 馬力 → 単勝オッズ（控除 0.2 を仮の市場として付与。較正は combo 側の t を見るので任意）
        strength = RNG.gamma(2.0, 1.0, size=N_HORSES)
        p = strength / strength.sum()
        odds = np.round((1.0 - 0.2) / p, 1)
        for u, o in zip(umabans, odds, strict=True):
            results_rows.append({"race_id": race_id, "馬番": u, "単勝": float(o)})

        implied = {u: 1.0 / float(o) for u, o in zip(umabans, odds, strict=True)}
        order = _sample_order(umabans, implied)

        for bet_type, combos in _winning_combos(order).items():
            t = TRUE_TAKEOUT[bet_type]
            row: dict = {"race_id": race_id}
            for i, combo in enumerate(combos):
                prob = harville.combo_probability(bet_type, implied, list(combo))
                if prob <= 0:
                    continue
                actual_odds = (1.0 - t) / prob  # 払戻金/100 に相当
                row[f"win_{i}"] = "-".join(str(x) for x in combo)
                row[f"return_{i}"] = round(actual_odds * 100.0, 0)
            payout_rows[bet_type].append(row)

    results = pd.DataFrame(results_rows).set_index("race_id")

    class _FakeReturnProcessor:
        def __init__(self, tables):
            self.preprocessed_data = tables

    tables = {
        bt: pd.DataFrame(rows).set_index("race_id") for bt, rows in payout_rows.items()
    }
    return results, _FakeReturnProcessor(tables)


def main() -> None:
    print("=" * 72)
    print("控除率較正 end-to-end 検証（本番関数を合成データで通す）")
    print("=" * 72)

    results, rp = _build_synthetic()

    # --- 1. 本番と同じビルダーで入力を構築 ---
    tansho_map = tansho_odds_by_race_from_table(results, "馬番", "単勝")
    payout_lookup = payout_lookup_from_return_processor(rp)
    print(f"\n[input] {len(tansho_map)} レースの単勝 / {len(payout_lookup)} 件の払戻実績")

    # --- 2. 較正（既知 takeout を復元できるか）---
    calib = calibrate_takeout_from_payouts(tansho_map, payout_lookup, min_samples=20)
    print("\n[1] 払戻実績からの控除率逆算（true → 復元）")
    print(f"    {'券種':<12}{'true':>8}{'復元':>10}{'誤差':>10}{'n':>8}  source")
    max_err = 0.0
    for bt, true_t in TRUE_TAKEOUT.items():
        info = calib[bt]
        err = abs(info["takeout"] - true_t)
        max_err = max(max_err, err)
        print(f"    {bt:<12}{true_t:>8.3f}{info['takeout']:>10.4f}"
              f"{err:>10.4f}{info['n']:>8}  {info['source']}")
    ok1 = max_err < 0.01
    print(f"    → 最大誤差 {max_err:.4f}  {'OK' if ok1 else 'NG'}（許容 <0.01）")

    # --- 3. 較正値が HistoricalOddsProvider に反映されるか ---
    tmap = takeout_map(calib)
    sample_race = next(iter(tansho_map))
    prov_nominal = HistoricalOddsProvider(tansho_map, takeout=0.2)
    prov_calib = HistoricalOddsProvider(tansho_map, takeout=tmap)
    print("\n[2] 連系推定オッズへの反映（同一 combo、nominal 0.2 vs 較正）")
    print(f"    {'券種':<12}{'nominal':>10}{'較正':>10}{'比':>8}")
    for bt in (BetType.UMAREN, BetType.SANRENTAN):
        combo = [1, 2] if bt == BetType.UMAREN else [1, 2, 3]
        on = prov_nominal.get_odds(sample_race, bt, combo)
        oc = prov_calib.get_odds(sample_race, bt, combo)
        print(f"    {bt:<12}{on:>10.1f}{oc:>10.1f}{oc / on:>8.3f}")
    # 三連単は較正 takeout 0.25 > 0.2 なので推定オッズは下がるはず
    san_combo = [1, 2, 3]
    ok2 = prov_calib.get_odds(sample_race, BetType.SANRENTAN, san_combo) < \
        prov_nominal.get_odds(sample_race, BetType.SANRENTAN, san_combo)
    print(f"    → 三連単(t={tmap[BetType.SANRENTAN]:.3f}>0.2) で推定オッズ低下: "
          f"{'OK' if ok2 else 'NG'}")

    # --- 4. ライブ EV 選定（run_prediction）が較正値で変化するか ---
    print("\n[3] ライブ EV 選定（run_prediction）への反映")
    ok3 = _verify_live_path()

    print("\n" + "=" * 72)
    allok = ok1 and ok2 and ok3
    print(f"結果: {'✅ 全項目 PASS' if allok else '❌ 失敗あり'}")
    print("=" * 72)
    raise SystemExit(0 if allok else 1)


def _verify_live_path() -> bool:
    """run_prediction に券種別 takeout を渡すと馬連 EV が下がることを確認。"""
    from app._prediction_service import run_prediction
    from src.operation._config import OperationConfig
    from src.constants._results_cols import ResultsCols

    class _StubModel:
        def __init__(self, probs):
            self._p = np.asarray(probs)

        def predict_proba(self, x):
            return np.column_stack([1.0 - self._p, self._p])

    X = pd.DataFrame(
        [
            {ResultsCols.UMABAN: 1, ResultsCols.WAKUBAN: 1, ResultsCols.TANSHO_ODDS: 2.0, "feat": 0.1},
            {ResultsCols.UMABAN: 2, ResultsCols.WAKUBAN: 2, ResultsCols.TANSHO_ODDS: 5.0, "feat": 0.2},
            {ResultsCols.UMABAN: 3, ResultsCols.WAKUBAN: 3, ResultsCols.TANSHO_ODDS: 20.0, "feat": 0.3},
        ],
        index=["r1"] * 3,
    )
    model = _StubModel([0.65, 0.25, 0.10])
    op = OperationConfig(
        bankroll=100_000.0, kelly_fraction_ratio=0.5, per_bet_cap_ratio=0.1, max_daily_ratio=1.0
    )
    th = {BetType.UMAREN: 0.0}

    def _umaren_ev(takeout):
        res = run_prediction(model, X, op, thresholds=th, takeout=takeout)
        evs = [c.expected_value for c in res if c.bet_type == BetType.UMAREN]
        return max(evs) if evs else None

    low = _umaren_ev({BetType.UMAREN: 0.0})
    high = _umaren_ev({BetType.UMAREN: 0.5})
    print(f"    馬連 max EV: takeout0.0 → {low:.3f} / takeout0.5 → {high:.3f}")
    ok = low is not None and high is not None and high < low
    print(f"    → 控除率↑ で EV↓: {'OK' if ok else 'NG'}")
    return ok


if __name__ == "__main__":
    main()
