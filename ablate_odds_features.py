"""オッズ特徴量アブレーション: 単勝エッジが「確定オッズ依存」か「純粋ハンデ力」かを判定。

監査でモデルが単勝_z(z-score化した確定オッズ)を特徴量に使っていることが判明した。
単勝エッジ(回収率1.155〜3.7)が、この確定オッズ情報に依存しているのか、それとも
オッズを見なくても成立する純粋なハンデ予測力なのかを切り分ける。

手順:
  1. オッズ/人気由来の特徴量(単勝_z 等)を featured から除く（生の単勝は EV 用に残す）。
  2. 除いた featured で再学習（オッズを見ないモデル）。
  3. ベースライン(全特徴量モデル)と アブレーション(オッズなしモデル)で単勝の閾値スイープを比較。

判定:
  - アブレーションでも閾値↑で回収率↑が残る → 純粋ハンデ力（オッズ無しで市場を上回る＝
    締切前オッズでも効く見込み。実戦性◎）
  - アブレーションで回収率が 0.8 付近に崩れる → エッジは確定オッズ情報依存（締切前では目減り）

実行: python ablate_odds_features.py   （再学習を含むため数分かかる）
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _identify_odds_features(featured: pd.DataFrame) -> list[str]:
    """オッズ/人気 由来の特徴量列を列挙する（生の単勝は EV 用に残すので除外しない）。"""
    from src.constants._results_cols import ResultsCols

    raw_keep = {ResultsCols.TANSHO_ODDS}  # 生の単勝は EV 計算に必要なので残す
    out = []
    for c in featured.columns:
        if c in raw_keep:
            continue
        cl = str(c).lower()
        if ("単勝" in str(c) or "オッズ" in str(c) or "人気" in str(c)
                or "odds" in cl or "popular" in cl):
            out.append(c)
    return out


def _train(featured: pd.DataFrame):
    """featured で stacking モデルを学習して返す（保存はしない）。"""
    from src.pipeline._retrain import RetrainConfig
    from src.training._keiba_ai_factory import KeibaAIFactory

    cfg = RetrainConfig()
    ai = KeibaAIFactory.create(featured, test_size=cfg.test_size, valid_size=cfg.valid_size)
    ai.train_with_stacking(meta_ratio=cfg.meta_ratio, with_tuning=False)
    return ai


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="オッズ特徴量アブレーションで単勝エッジを検証")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--bet-type", default="tansho")
    args = ap.parse_args()

    import validate_edge as ve
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from app._model_compare import recent_race_slice
    from src.policies._score_policy import PROB
    from src.policies._score_policy import ExpectedValueScorePolicy

    featured = load_featured_data()
    rp = _load_return_processor()
    if featured is None or featured.empty or rp is None:
        logger.error("featured_data / return_tables が読み込めません")
        return

    odds_cols = _identify_odds_features(featured)
    print("=" * 64)
    print("オッズ特徴量アブレーション")
    print("=" * 64)
    print(f"\n除外するオッズ/人気由来の特徴量 {len(odds_cols)} 列:\n  {odds_cols}")
    if not odds_cols:
        print("  オッズ由来の特徴量が見つかりません（既に除外済み？）。")
        return
    ablated = featured.drop(columns=odds_cols)

    print(f"\n■ ベースライン学習（全 {featured.shape[1]} 列）…")
    ai_base = _train(featured)
    print(f"■ アブレーション学習（{ablated.shape[1]} 列、オッズ特徴量なし）…")
    ai_abl = _train(ablated)

    def _sweep_for(ai, feat, label):
        sl = recent_race_slice(feat, args.test_frac)
        table = ai.calc_score(sl, ExpectedValueScorePolicy)
        ve._sweep(ve._FixedScoreAI(table), rp, args.bet_type, label)
        # 念のためプラセボ（このモデルのエッジが本物か）
        ve._sweep(ve._FixedScoreAI(ve._shuffle_prob_within_race(table, PROB)), rp,
                  args.bet_type, f"{label}・プラセボ")

    print("\n" + "=" * 64)
    print("単勝エッジ: ベースライン(全特徴量) vs アブレーション(オッズ無し)")
    print("=" * 64)
    _sweep_for(ai_base, featured, "ベースライン（全特徴量）")
    _sweep_for(ai_abl, ablated, "アブレーション（オッズ無し）")

    print("\n判定:")
    print(" - アブレーションでも閾値↑で回収率↑ → 純粋ハンデ力（締切前オッズでも効く見込み・実戦性◎）")
    print(" - アブレーションで回収率が 0.8 付近に崩れる → エッジは確定オッズ情報依存（締切前で目減り）")
    print("=" * 64)


if __name__ == "__main__":
    main()
