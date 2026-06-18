"""マルチ GBDT スタッキング動作確認スクリプト（合成データ E2E）。

実 featured_data が無い環境でも、KeibaAI.train_with_stacking の本番経路と
app/_model_eval.compute_stacking_auc（モデルラボの「スタッキング寄与」チャートの
計算ロジック）をそのまま使い、以下を検証する:

  1. configs/base_models_nn.example.json（LGB+XGB+CatBoost+NN）で学習が完走するか
  2. base 別 AUC（LightGBM/XGBoost/CatBoost/NN）と meta AUC が個別に算出されるか
  3. meta が各単体 base を上回る（少なくとも最良 base 以上の）多様性が出るか

学習可能な信号を持つ合成データを生成する（特徴量→勝率に依存）。
"""

from __future__ import annotations

import sys

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

from src.constants._results_cols import ResultsCols
from src.preprocessing._prepared_features import PreparedFeatures
from src.training._base_models_config import load_base_models_config
from src.training._data_splitter import DataSplitter
from src.training._keiba_ai import KeibaAI
from app._model_eval import compute_stacking_auc


def make_prepared_features(n_races=1200, horses_per_race=10, seed=7) -> PreparedFeatures:
    """学習可能な信号を持つ合成 featured_data（gbdt + nn ストリーム）。

    各レースで馬ごとに潜在強度 z を特徴量から構成し、レース内 softmax で
    勝ち馬（rank=1）を 1 頭サンプリングする。特徴量と勝敗に実relが入るので
    base 各モデルの AUC は 0.5 を有意に上回る。
    """
    rng = np.random.default_rng(seed)
    base_date = pd.Timestamp("2020-01-01")
    rows = []
    for i in range(n_races):
        race_id = f"race_{i:05d}"
        date = base_date + pd.Timedelta(hours=i)  # 厳密に単調増加（時系列分割用）
        # レース内の馬ごと特徴量
        f1 = rng.normal(size=horses_per_race)          # 連続（線形効果）
        f2 = rng.normal(size=horses_per_race)          # 連続（非線形効果）
        f3 = rng.uniform(-1, 1, size=horses_per_race)  # 連続（交互作用）
        course = rng.integers(0, 6, size=horses_per_race)  # entity（embedding 対象）
        course_effect = np.array([0.0, 0.3, -0.2, 0.5, -0.4, 0.1])[course]

        # 潜在強度: 線形 + 非線形 + 交互作用 + entity 効果 + ノイズ
        z = (
            1.2 * f1
            - 0.8 * (f2 ** 2)
            + 1.0 * (f1 * f3)
            + course_effect
            + rng.normal(scale=0.5, size=horses_per_race)
        )
        # レース内 softmax で勝ち馬を 1 頭サンプル
        p = np.exp(z - z.max())
        p = p / p.sum()
        winner = rng.choice(horses_per_race, p=p)
        ranks = np.zeros(horses_per_race, dtype=int)
        ranks[winner] = 1
        # 単勝オッズは強度の逆数っぽく（EV 重み計算が落ちないようダミーでも妥当な値）
        odds = np.clip(1.0 / (p + 1e-3), 1.1, 99.0)

        for h in range(horses_per_race):
            rows.append(
                {
                    "race_id": race_id,
                    "date": date,
                    "rank": int(ranks[h]),
                    ResultsCols.TANSHO_ODDS: float(odds[h]),
                    ResultsCols.UMABAN: h + 1,
                    "horse_id": int(course[h]),  # entity 列（NN embedding）
                    "feat_f1": float(f1[h]),
                    "feat_f2": float(f2[h]),
                    "feat_f3": float(f3[h]),
                    "feat_course": int(course[h]),
                }
            )
    gbdt_df = pd.DataFrame(rows).set_index("race_id")
    # NN ストリーム: entity（category） + numeric。date/rank/odds は含めない。
    nn_df = gbdt_df[["horse_id", "feat_f1", "feat_f2", "feat_f3"]].copy()
    nn_df["horse_id"] = nn_df["horse_id"].astype("category")
    return PreparedFeatures(gbdt=gbdt_df, nn=nn_df)


def main() -> int:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "configs/base_models_nn.example.json"
    cfg = load_base_models_config(config_path)
    print(f"=== config: {config_path} ===")
    print(f"  models = {cfg.models}")
    print(f"  tune_per_model = {cfg.tune_per_model}")
    print(f"  nn_params = {cfg.nn_params}")
    print()

    pf = make_prepared_features()
    print(f"=== synthetic featured_data: {len(pf.gbdt)} rows / "
          f"{pf.gbdt.index.nunique()} races / win-rate={pf.gbdt['rank'].mean():.3f} ===")

    ds = DataSplitter(pf, test_size=0.2, valid_size=0.2)
    print(f"  has_nn_stream = {ds.has_nn_stream}")
    print(f"  nn entity cards = {ds.nn_categorical_cardinalities}")
    print()

    ai = KeibaAI(ds)
    print("=== train_with_stacking (with_tuning=False) ... ===")
    ai.train_with_stacking(with_tuning=False, base_models_config=cfg)
    print(f"  base_model_names_ = {ai.base_model_names_}")
    print(f"  feature_names_ count = {len(ai.feature_names_)}")
    print()

    # モデルラボの「スタッキング寄与」と同一ロジックでテストセット AUC を算出
    res = compute_stacking_auc(ai, pf.gbdt, test_size=0.2, valid_size=0.2)
    if res is None:
        print("!! compute_stacking_auc が None を返しました（スタッキング非検出）")
        return 1

    y = res["y_true"]
    print("=== テストセット AUC（モデルラボ『スタッキング寄与』と同一計算）===")
    base_aucs = {}
    for name, probs in zip(res["base_names"], res["base_probs"]):
        auc = roc_auc_score(y, probs)
        base_aucs[name] = auc
        print(f"  base  {name:<10} AUC = {auc:.4f}")
    meta_auc = roc_auc_score(y, res["meta_probs"])
    print(f"  meta  {'(stack)':<10} AUC = {meta_auc:.4f}")
    print()

    # 判定
    best_base = max(base_aucs.values())
    n_models = len(base_aucs)
    print("=== 判定 ===")
    ok = True
    if n_models != len(cfg.models):
        print(f"  [WARN] base モデル数 {n_models} != config 指定 {len(cfg.models)}")
    else:
        print(f"  [OK] base モデル {n_models} 種すべてが個別 AUC を出力")
    if all(a > 0.55 for a in base_aucs.values()):
        print(f"  [OK] 全 base が AUC>0.55（学習成立、最良 base={best_base:.4f}）")
    else:
        print(f"  [WARN] AUC<=0.55 の base あり: "
              f"{ {k: round(v, 3) for k, v in base_aucs.items()} }")
    # meta はスタッキングの目的上「最良 base 以上」を期待（誤差許容 0.005）
    if meta_auc >= best_base - 0.005:
        print(f"  [OK] meta AUC {meta_auc:.4f} >= 最良 base {best_base:.4f}（多様性が活きている）")
    else:
        print(f"  [WARN] meta AUC {meta_auc:.4f} < 最良 base {best_base:.4f}")
        ok = False

    print()
    print("=== 完了 ===")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
