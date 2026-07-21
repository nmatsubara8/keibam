"""特徴量 harm 分析: 入力に混入していて AUC を悪化させている項目を検出する。

gain 重要度（feature_importance.py）は「モデルが何を使っているか」しか分からない。
本スクリプトは **permutation importance（AUC 基準）** で「入れることで予測力を
下げている項目」を切り分ける:

  importance(col) = baseline_AUC − (col をシャッフルした AUC)   ← R 回平均

  - importance > 0 : その列は AUC に寄与（有用）
  - importance ≈ 0 : 無用（あってもなくても変わらない＝ノイズ/冗長）
  - importance < 0 : **害**（シャッフルで AUC が上がる＝混入がスコアを悪化させている）

留意: 相関の強い列は「冗長コピー」があると harm を過小評価しうる（片方を壊しても
もう片方が情報を保つ）。グループ単位で同時シャッフルする --by-group で相関マスクを緩和する。
確定判定は該当列を落として再学習し AUC 変化を見ること（本スクリプトはスクリーニング）。

実行:
  python feature_harm.py                 # 全特徴量・上位/下位を表示
  python feature_harm.py --top 30 --repeats 5
  python feature_harm.py --by-group      # グループ単位の joint permutation も表示
  python feature_harm.py --since-year 2016   # 直近のみで高速化
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# validate_edge のリーク監査でフラグされた「結果系の疑い」列（要重点確認）。
_LEAK_SUSPECTS = [
    "owner_avg_rank", "wet_rel_rank", "around_rel_rank", "avg_rank_type_ground",
    "avg_rank_same_class", "単勝_log", "単勝_z", "単勝_log_z",
    "owner_avg_rank_z", "wet_rel_rank_z", "around_rel_rank_z",
    "avg_rank_type_ground_z", "avg_rank_same_class_z",
]


def _group_of(col: str) -> str:
    """特徴量を大まかな領域に分類する（harm のグループ集計用）。"""
    c = str(col)
    cl = c.lower()
    if "単勝" in c or "オッズ" in c or "人気" in c or "odds" in cl or "popular" in cl:
        return "オッズ/人気（市場模写）"
    if any(k in c for k in ("overlay", "implied")):
        return "市場シグナル(overlay)"
    if "speed_fig" in cl or "spd" in cl:
        return "スピード指数"
    if "elo" in cl or "rating" in cl or "_z" == c[-2:] and "elo" in cl:
        return "レーティング(Elo)"
    if "yoso" in cl or "予想" in c or "印" in c:
        return "予想印(yoso)"
    if c.startswith("sire_") or c.startswith("damsire_") or "血統" in c or "ped" in cl:
        return "血統(sire/ped)"
    if c.startswith("jockey_") or c.startswith("trainer_") or c.startswith("owner_"):
        return "騎手/調教師/馬主"
    if "rank" in cl or "着" in c:
        return "着順集計（過去走）"
    if "te" in cl.split("_") or cl.endswith("_te"):
        return "ターゲットエンコーディング"
    if any(k in cl for k in ("dist", "kinryo", "age", "interval", "around", "ground", "type")):
        return "コース/条件/適性"
    if any(k in cl for k in ("month", "season", "day", "cycle", "date")):
        return "日付/周期"
    return "その他"


def _auc(model, X: pd.DataFrame, y: np.ndarray) -> float:
    from sklearn.metrics import roc_auc_score

    p = model.predict_proba(X)[:, 1]
    return float(roc_auc_score(y, p))


def _permutation_importance(
    model, X: pd.DataFrame, y: np.ndarray, cols, repeats: int, seed: int = 0,
):
    """各列（または列グループ）をシャッフルして AUC 低下量を測る。

    cols が str のリストなら列単位、list[list] ならグループ単位（同時シャッフル）。
    返り値: {key: (mean_importance, std)}  importance = baseline_auc − permuted_auc
    """
    rng = np.random.default_rng(seed)
    base = _auc(model, X, y)
    out = {}
    n = len(X)
    for item in cols:
        group = [item] if isinstance(item, str) else item
        present = [c for c in group if c in X.columns]
        if not present:
            continue
        deltas = []
        orig = {c: X[c].to_numpy(copy=True) for c in present}
        for _ in range(repeats):
            perm = rng.permutation(n)
            for c in present:
                X[c] = orig[c][perm]  # 全列同一置換で行内相関を保ったまま列⟷行の対応を壊す
            deltas.append(base - _auc(model, X, y))
        for c in present:  # 復元
            X[c] = orig[c]
        key = present[0] if isinstance(item, str) else "+".join(present[:1]) + f"（{len(present)}列）"
        out[key] = (float(np.mean(deltas)), float(np.std(deltas)))
    return base, out


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="特徴量 harm 分析（permutation importance・AUC 基準）")
    ap.add_argument("--top", type=int, default=25, help="表示する上位/下位件数")
    ap.add_argument("--repeats", type=int, default=3, help="permutation 反復回数（安定性）")
    ap.add_argument("--since-year", type=int, default=None, help="この年以降の行だけで学習・評価（高速化）")
    ap.add_argument("--by-group", action="store_true", help="グループ単位の joint permutation も表示")
    args = ap.parse_args()

    import lightgbm as lgb

    from app._model_eval import load_featured_data
    from src.constants._bet_thresholds import TrainingWeights
    from src.training._keiba_ai_factory import KeibaAIFactory

    featured = load_featured_data()
    if featured is None or featured.empty:
        logger.error("featured_data が読み込めません")
        return
    if args.since_year:
        rid = featured.index.astype(str)
        featured = featured[rid.str[:4].astype(int) >= args.since_year]
        logger.info("[harm] --since-year %d: %d 行", args.since_year, len(featured))

    ai = KeibaAIFactory.create(featured, test_size=0.2, valid_size=0.2)
    X_train, y_train = ai.datasets.X_train, ai.datasets.y_train
    X_test, y_test = ai.datasets.X_test.copy(), np.asarray(ai.datasets.y_test)
    logger.info("[harm] 学習 %d 行 / 検証 %d 行 / 特徴量 %d 列", len(X_train), len(X_test), X_test.shape[1])

    model = lgb.LGBMClassifier(
        scale_pos_weight=TrainingWeights.SCALE_POS_WEIGHT,
        objective="binary", n_estimators=300, num_leaves=63, verbose=-1,
    )
    model.fit(X_train, y_train)

    cols = list(X_test.columns)
    base, imp = _permutation_importance(model, X_test, y_test, cols, args.repeats)
    ser = pd.Series({k: v[0] for k, v in imp.items()}).sort_values()

    print("=" * 72)
    print(f"特徴量 harm 分析（permutation importance・AUC 基準） baseline AUC={base:.4f}")
    print(f"  学習 {len(X_train)} 行 / 検証 {len(X_test)} 行 / repeats={args.repeats}")
    print("=" * 72)

    harmful = ser[ser < 0]
    print(f"\n■ 害の疑い（importance<0＝シャッフルで AUC 改善） {len(harmful)} 列 / 下位 {args.top}")
    print(f"  {'importance':>12}  列名")
    for col, val in ser.head(args.top).items():
        mark = " ⚠害" if val < 0 else ""
        print(f"  {val:>+12.5f}  {col}{mark}")

    print(f"\n■ 有用（importance>0）上位 {min(args.top, 10)}")
    for col, val in ser.sort_values(ascending=False).head(min(args.top, 10)).items():
        print(f"  {val:>+12.5f}  {col}")

    # グループ集計（列単位 importance の合計＝領域ごとの純寄与）
    grp = pd.Series({k: v[0] for k, v in imp.items()})
    by_group = grp.groupby(_group_of).sum().sort_values()
    print("\n■ 領域別の純寄与（列単位 importance 合計・負＝その領域が全体として害）")
    print(f"  {'合計imp':>12}  領域")
    for g, val in by_group.items():
        mark = " ⚠" if val < 0 else ""
        print(f"  {val:>+12.5f}  {g}{mark}")

    # リーク監査でフラグされた列を重点表示
    print("\n■ リーク監査フラグ列の harm（validate_edge が結果系と疑った列）")
    print(f"  {'importance':>12}  列名")
    for col in _LEAK_SUSPECTS:
        if col in imp:
            val = imp[col][0]
            mark = " ⚠害" if val < 0 else (" 有用" if val > 0.0005 else " ~0")
            print(f"  {val:>+12.5f}  {col}{mark}")

    if args.by_group:
        from collections import defaultdict

        groups = defaultdict(list)
        for c in cols:
            groups[_group_of(c)].append(c)
        _, gimp = _permutation_importance(
            model, X_test, y_test, list(groups.values()), args.repeats, seed=1,
        )
        gser = pd.Series(
            dict(zip(groups.keys(), [v[0] for v in gimp.values()], strict=False))
        ).sort_values()
        print("\n■ グループ joint permutation（相関マスクを緩和・領域全体を同時に壊す）")
        print(f"  {'importance':>12}  領域（同時シャッフル）")
        for g, val in gser.items():
            mark = " ⚠害" if val < 0 else ""
            print(f"  {val:>+12.5f}  {g}{mark}")

    print("\n" + "=" * 72)
    print("読み方: importance<0 の列＝壊すと AUC が上がる＝混入がスコアを悪化。ただし相関冗長で")
    print("過小評価されうるため、候補は retrain で該当列を落として AUC 変化を確認して確定すること。")
    print("=" * 72)


if __name__ == "__main__":
    main()
