"""特徴量重要度: オッズ無しモデルの単勝エッジを何が駆動しているかを可視化する。

エッジ強化(C)の第一歩。市場が見落とす特徴量を足す前に、現状のオッズ無しモデルが
どの特徴量を重視しているかを把握する。重要度上位を見て「既に効いている領域」と
「手薄な領域（=伸びしろ）」を判断する。

実行: python feature_importance.py [--top 40] [--keep-odds]
"""

from __future__ import annotations

import argparse
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="オッズ無しモデルの特徴量重要度")
    ap.add_argument("--top", type=int, default=40)
    ap.add_argument("--keep-odds", action="store_true", help="オッズ特徴量を残す")
    args = ap.parse_args()

    import lightgbm as lgb

    import ablate_odds_features as ab
    from app._model_eval import load_featured_data
    from src.constants._bet_thresholds import TrainingWeights
    from src.training._keiba_ai_factory import KeibaAIFactory

    featured = load_featured_data()
    if featured is None or featured.empty:
        logger.error("featured_data が読み込めません")
        return

    if args.keep_odds:
        feat = featured
        mode = "全特徴量"
    else:
        odds_cols = ab._identify_odds_features(featured)
        feat = featured.drop(columns=odds_cols)
        mode = f"オッズ無し（除外 {odds_cols}）"

    print("=" * 70)
    print(f"特徴量重要度（{mode}）")
    print("=" * 70)

    ai = KeibaAIFactory.create(feat, test_size=0.2, valid_size=0.2)
    X_train = ai.datasets.X_train
    y_train = ai.datasets.y_train
    print(f"\n学習行 {len(X_train)} / 特徴量 {X_train.shape[1]} 列で重要度を算出…")

    model = lgb.LGBMClassifier(
        scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
        objective="binary", n_estimators=300, num_leaves=63, verbose=-1,
    )
    model.fit(X_train, y_train)

    # gain ベースの重要度（split 数より「予測への寄与」を反映）
    gain = pd.Series(
        model.booster_.feature_importance(importance_type="gain"),
        index=X_train.columns,
    ).sort_values(ascending=False)

    print(f"\n■ 重要度（gain）上位 {args.top}")
    total = gain.sum()
    for i, (col, val) in enumerate(gain.head(args.top).items(), 1):
        print(f"  {i:>3}. {col:<40} {val / total * 100:>6.2f}%")

    # 領域別の集計（どのカテゴリにエッジが偏っているか）
    def _bucket(col: str) -> str:
        c = str(col)
        if "jockey" in c:
            return "騎手"
        if "trainer" in c:
            return "調教師"
        if "sire" in c or c.startswith("peds"):
            return "血統/種牡馬"
        if "pace" in c or "leg_type" in c:
            return "脚質/ペース"
        if "at_distance" in c or "course" in c or "race_type" in c:
            return "距離/コース"
        if "着順" in c or "rank" in c:
            return "過去着順集計"
        if any(t in c for t in ("斤量", "体重", "年齢", "性", "枠", "馬番", "n_horses")):
            return "馬の基本属性"
        if "interval" in c or "agedays" in c:
            return "間隔/年齢日数"
        return "その他"

    buckets = gain.groupby(_bucket).sum().sort_values(ascending=False)
    print("\n■ 領域別の重要度シェア（伸びしろの判断材料）")
    for cat, val in buckets.items():
        print(f"  {cat:<14} {val / total * 100:>6.2f}%")

    print("\n→ シェアの低い領域＝市場も見落としやすく、強化の伸びしろがある可能性。")
    print("=" * 70)


if __name__ == "__main__":
    main()
