"""単勝エッジの検証: 閾値スイープ + プラセボ(予測シャッフル) + 特徴量リーク監査。

A/B比較で単勝が「閾値↑で回収率↑(1.155)・Sharpe↑」という本物のエッジ兆候を示したため、
それが (1) モデルの真の予測力か (2) オッズ選択の人気薄バイアス由来の見かけか
(3) 特徴量リークかを切り分ける。

- 閾値スイープ: 閾値を上げて回収率・的中率・Sharpe が単調改善するか（真のエッジの兆候）。
- プラセボ: race 内で予測勝率をシャッフルし prob↔馬 の対応を壊す。これでも回収率が
  >1 のままなら「エッジはモデルでなくオッズ選択の副産物」。0.8 付近に落ちれば真の予測力。
- リーク監査: モデル入力特徴量に現在レース結果(着順/単勝/人気/タイム等)が混入していないか。

実行:
  python validate_edge.py                 # 単勝で検証
  python validate_edge.py --bet-type umaren
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# 現在レースの「結果」を表す列名（モデル入力に混じればリーク）。
_RESULT_LEAK_TERMS = (
    "着順", "rank", "単勝", "人気", "タイム", "time_seconds", "着差", "通過",
    "corner", "上がり", "nobori", "pace", "払戻", "return", "オッズ", "odds", "popular",
)

_THRESHOLDS = [0.8, 1.0, 1.1, 1.2, 1.3, 1.5, 1.8, 2.2, 3.0]


def _fmt(x):
    return "—" if x is None else f"{x:.3f}"


def _shuffle_prob_within_race(table: pd.DataFrame, prob_col: str, seed: int = 0) -> pd.DataFrame:
    """race(=index) 内で勝率列をシャッフルし、prob↔馬 の対応を壊す（プラセボ）。"""
    rng_t = table.copy()
    rng_t[prob_col] = (
        rng_t.groupby(level=0)[prob_col]
        .transform(lambda s: s.sample(frac=1, random_state=seed).to_numpy())
    )
    return rng_t


class _FixedScoreAI:
    """calc_score が固定スコア表を返すラッパ（backtest_bet_type を再利用するため）。"""

    def __init__(self, table: pd.DataFrame) -> None:
        self._table = table

    def calc_score(self, X, policy):  # noqa: ANN001
        return self._table


def _sweep(score_ai, rp, bet_type, label: str) -> None:
    from app._bet_type_optimizer import backtest_bet_type
    from src.policies._bet_type_params import BetTypeParams

    print(f"\n[{label}] 閾値スイープ（券種={bet_type}）")
    print(f"  {'EV閾値':>7}{'買い目':>10}{'的中率':>9}{'回収率':>9}{'Sharpe':>9}")
    for th in _THRESHOLDS:
        params = BetTypeParams(ev_threshold=th)
        summary, _ = backtest_bet_type(score_ai, pd.DataFrame(), rp, bet_type, params, 0.2)
        n = summary.get("n_bets", 0)
        print(f"  {th:>7.2f}{int(n):>10}{_fmt(summary.get('hit_rate')):>9}"
              f"{_fmt(summary.get('return_rate')):>9}{_fmt(summary.get('sharpe_ratio')):>9}")


def _leak_audit(featured_slice: pd.DataFrame) -> None:
    from src.policies._score_policy import META_COLS
    from src.policies._score_policy import _DROP_FOR_PREDICT

    print("\n[リーク監査] モデル入力特徴量に現在レース結果が混入していないか")
    dropped = set(_DROP_FOR_PREDICT)
    input_cols = [c for c in featured_slice.columns if c not in dropped]
    print(f"  全列 {len(featured_slice.columns)} / 予測前に除外 {len(dropped)}（{sorted(dropped)}）"
          f" / モデル入力 {len(input_cols)} 列")

    def _is_suspect(col: str) -> bool:
        c = str(col).lower()
        # 過去走集計（_5R/_allR 等の suffix や jockey_/sire_ 等の prefix）は当該レース結果でない
        if any(tag in c for tag in ("_5r", "_9r", "_20r", "_allr", "rate", "median", "mean",
                                    "std", "_at_", "recent", "jockey_", "trainer_", "sire_",
                                    "interval", "agedays", "zscore")):
            return False
        return any(term.lower() in c for term in _RESULT_LEAK_TERMS)

    suspects_dropped = [c for c in featured_slice.columns if c in dropped and
                        any(t.lower() in str(c).lower() for t in _RESULT_LEAK_TERMS)]
    suspects_input = [c for c in input_cols if _is_suspect(c)]
    print(f"  結果系で正しく除外済み: {suspects_dropped}")
    if suspects_input:
        print(f"  ⚠ モデル入力に結果系の疑い: {suspects_input}  ← 要確認（リークの可能性）")
    else:
        print("  ✅ モデル入力に現在レース結果列の混入は検出されず")
    _ = META_COLS


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="単勝エッジの検証（スイープ+プラセボ+リーク監査）")
    ap.add_argument("--bet-type", default="tansho", help="検証する券種（既定 tansho）")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--version", default=None)
    ap.add_argument(
        "--max-odds", type=float, default=float("inf"),
        help="このオッズ以下の馬だけを対象にする（config の検証済み戦略は tansho・≤15）。既定は無制限",
    )
    args = ap.parse_args()

    from app._data_loader import find_model_paths
    from app._data_loader import load_model_by_version
    from app._data_loader import load_model_from_path
    from app._model_compare import recent_race_slice
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.policies._score_policy import CURRENT_ODDS
    from src.policies._score_policy import PROB
    from src.policies._score_policy import ExpectedValueScorePolicy

    featured = load_featured_data()
    rp = _load_return_processor()
    if featured is None or featured.empty or rp is None:
        logger.error("featured_data / return_tables が読み込めません")
        return
    if args.version:
        ai = load_model_by_version(args.version)
    else:
        paths = find_model_paths("models")
        if not paths:
            logger.error("学習済みモデルがありません")
            return
        ai = load_model_from_path(paths[0])

    featured_slice = recent_race_slice(featured, args.test_frac)
    logger.info("[validate] 検証 %d レース / 券種=%s", featured_slice.index.nunique(), args.bet_type)

    _leak_audit(featured_slice)

    # スコア表を1回だけ計算し、本番とプラセボで使い回す
    table = ai.calc_score(featured_slice, ExpectedValueScorePolicy)
    if args.max_odds != float("inf"):
        before = len(table)
        table = table[table[CURRENT_ODDS] <= args.max_odds]
        logger.info(
            "[validate] オッズ≤%.1f に絞り込み: %d→%d 頭（%d レース）",
            args.max_odds, before, len(table), table.index.nunique(),
        )
    real_ai = _FixedScoreAI(table)
    placebo_ai = _FixedScoreAI(_shuffle_prob_within_race(table, PROB))

    print("\n" + "=" * 60)
    print("単勝エッジ検証: 本番 vs プラセボ(予測シャッフル)")
    print("=" * 60)
    _sweep(real_ai, rp, args.bet_type, "本番（モデル予測）")
    _sweep(placebo_ai, rp, args.bet_type, "プラセボ（race内シャッフル）")
    print("\n判定:")
    print(" - 本番が閾値↑で回収率↑＆プラセボは0.8付近で横ばい → 真の予測力（エッジ本物）")
    print(" - 本番もプラセボも同様に>1 → オッズ選択の副産物（人気薄バイアス）。エッジ偽")
    print("=" * 60)


if __name__ == "__main__":
    main()
