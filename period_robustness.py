"""時系列頑健性: 単勝エッジ(オッズ無しモデル)が特定期間の偶然でないかを検証する。

アブレーションで「オッズを見なくても +13%程度のハンデエッジ」が確認できたが、それが
検証期間(直近20%)の中で安定しているか／特定の年に偏った偶然かを切り分ける。

手順:
  1. オッズ特徴量を除いた featured でモデルを1回学習（実戦的なオッズ無しモデル）。
  2. テスト区間(直近20%)の予測を年度(race_id 先頭4桁)で層別。
  3. 年度×EV閾値ごとに単勝の回収率・的中率・買い目数を出す。

判定:
  - どの年度でも閾値↑で回収率>1が出る → エッジは時系列に頑健（本物）。
  - 特定年だけ>1で他は<1 → その年の偶然（過学習/特殊要因）。

実行: python period_robustness.py   （学習1回を含むため数分）
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)

_THRESHOLDS = [1.5, 2.0, 2.5]


def _fmt(x):
    return "—" if x is None else f"{x:.3f}"


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="単勝エッジの時系列頑健性（年度層別）")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--bet-type", default="tansho")
    ap.add_argument("--keep-odds", action="store_true",
                    help="オッズ特徴量を除かずベースラインで検証（既定はオッズ無し）")
    args = ap.parse_args()

    import ablate_odds_features as ab
    import validate_edge as ve
    from app._bet_type_optimizer import backtest_bet_type
    from app._model_compare import recent_race_slice
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.policies._bet_type_params import BetTypeParams
    from src.policies._score_policy import ExpectedValueScorePolicy

    featured = load_featured_data()
    rp = _load_return_processor()
    if featured is None or featured.empty or rp is None:
        logger.error("featured_data / return_tables が読み込めません")
        return

    if args.keep_odds:
        train_feat = featured
        mode = "ベースライン（全特徴量）"
    else:
        odds_cols = ab._identify_odds_features(featured)
        train_feat = featured.drop(columns=odds_cols)
        mode = f"オッズ無し（除外 {odds_cols}）"

    print("=" * 78)
    print(f"単勝エッジ 時系列頑健性（{mode}）")
    print("=" * 78)
    print("\n■ モデル学習中…")
    ai = ab._train(train_feat)

    test_slice = recent_race_slice(train_feat, args.test_frac)
    table = ai.calc_score(test_slice, ExpectedValueScorePolicy)
    years = pd.Index([str(r)[:4] for r in table.index], name="year")

    print(f"\n■ テスト区間 {table.index.nunique()} レース / 年度: "
          f"{sorted(set(years))}")

    print(f"\n{'年度':<8}{'EV閾値':>8}{'買い目':>9}{'的中率':>9}{'回収率':>9}{'Sharpe':>9}")
    print("-" * 78)
    for year in sorted(set(years)):
        sub = table[years.to_numpy() == year]
        if sub.index.nunique() < 50:
            continue  # レース数が少なすぎる年はスキップ
        for th in _THRESHOLDS:
            params = BetTypeParams(ev_threshold=th)
            summary, _ = backtest_bet_type(
                ve._FixedScoreAI(sub), pd.DataFrame(), rp, args.bet_type, params, 0.2
            )
            n = summary.get("n_bets", 0)
            mark = " ◎" if (summary.get("return_rate") or 0) > 1.0 else ""
            print(f"{year:<8}{th:>8.1f}{int(n):>9}{_fmt(summary.get('hit_rate')):>9}"
                  f"{_fmt(summary.get('return_rate')):>9}{_fmt(summary.get('sharpe_ratio')):>9}{mark}")
        print("-" * 78)

    print("\n判定:")
    print(" - どの年度でも高閾値で回収率>1(◎) → 時系列に頑健な本物のエッジ")
    print(" - 特定年だけ>1で他は<1 → その年の偶然（過学習/特殊要因）。実戦は要警戒")
    print("=" * 78)


if __name__ == "__main__":
    main()
