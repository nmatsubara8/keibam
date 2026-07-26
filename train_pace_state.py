"""ペース状態 P(z) 予測器の学習・rolling-origin 評価・ストア生成 CLI（実データフェーズ タスク1）。

やること:
1) horse_results の「ペース」列（前半-後半）を (horse_id, date) で featured に結合し、
   レース単位の教師ラベル z（slow/normal/fast・条件相対 33/66 分位）を作る。
2) 発走前特徴量（先行勢比率・逃げ候補頭数・距離・馬場…）をレース単位に集計する。
3) rolling-origin（〜Y学習→Y+1評価）で LightGBM 多クラスを評価する
   （**fit は各 fold の過去データのみ**＝時間方向リークを構造遮断）。
   判定: 一様事前分布に対する OOS ΔlogLoss < 0 と accuracy。
4) 全期間で最終モデルを学習し、**各レースの P(z) は「その年を含まない直前 fold のモデル」の
   OOS 予測**で埋めたストアを保存する（β 学習・Mixture-PL 評価にそのまま使える前進安全な列）。
5) SHAP/gain で「何が Slow/Fast を決めたか」を表示（説明可能性の維持）。

実行例:
    python train_pace_state.py                 # 全量
    python train_pace_state.py --limit 50000   # 動作確認
出力: data/raw/pace_states.pkl（label + p_slow/p_normal/p_fast）, models/pace_state_lgbm.txt
"""
from __future__ import annotations

import argparse
import os

import pandas as pd

from src.constants._local_paths import LocalPaths
from src.preprocessing._pace_state import (
    PZ_FEATURE_COLS,
    build_race_features,
    evaluate_pz,
    explain_pz,
    fit_pz,
    label_pace_states,
    predict_pz,
    race_pace_balance,
)

PACE_MODEL_PATH = os.path.join("models", "pace_state_lgbm.txt")
MIN_TRAIN_YEARS = 3


def _load(path: str, name: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise SystemExit(f"{name} がありません: {path}（データ環境で実行してください）")
    return pd.read_pickle(path)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=0, help="featured 行数上限（動作確認用）")
    ap.add_argument("--min-train-years", type=int, default=MIN_TRAIN_YEARS)
    args = ap.parse_args()

    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None or featured.empty:
        raise SystemExit("featured がありません（retrain/rebuild-featured 後に実行）")
    if args.limit:
        featured = featured.iloc[: args.limit]
    hr = _load(LocalPaths.RAW_HORSE_RESULTS_PATH, "horse_results")

    # ── 1) 教師ラベル ─────────────────────────────────────────────
    fk = pd.DataFrame({
        "race_id": featured.index.astype(str),
        "horse_id": featured.get("horse_id"),
        "date": featured.get("date"),
    }).dropna()
    balance = race_pace_balance(fk, hr)
    print(f"ペース文字列が結合できたレース: {len(balance):,}")
    if balance.empty:
        raise SystemExit("ペース列の結合が0件（horse_results の取得範囲を確認）")

    # 条件グループ（race_type × 距離帯）でスケール差を吸収
    rid = featured.index.astype(str)
    grp = pd.DataFrame(index=pd.Index(balance.index, name="race_id"))
    for col, label in (("race_type", "rt"), ("course_len", "dist")):
        if col in featured.columns:
            s = featured[col].groupby(rid).first().reindex(balance.index)
            if col == "course_len":
                s = (pd.to_numeric(s, errors="coerce") // 400 * 400).astype("Int64")
            grp[label] = s.astype(str)
    labels = label_pace_states(balance, grp if not grp.empty else None)
    print("ラベル分布:", labels.value_counts().to_dict())

    # ── 2) 発走前特徴量 ───────────────────────────────────────────
    feats = build_race_features(featured).reindex(labels.index)
    have = [c for c in PZ_FEATURE_COLS if feats[c].notna().any()]
    print(f"有効特徴量 {len(have)}/{len(PZ_FEATURE_COLS)}: {have}")

    # ── 3) rolling-origin 評価（fit は過去のみ）──────────────────
    year = pd.Series(labels.index.str[:4].astype(int), index=labels.index)
    years = sorted(year.unique())
    oos_pred: list[pd.DataFrame] = []
    print(f"\nrolling-origin: {years[0]}–{years[-1]}")
    for i in range(args.min_train_years, len(years)):
        ty = years[i]
        tr_idx = labels.index[year < ty]
        te_idx = labels.index[year == ty]
        if len(tr_idx) < 500 or len(te_idx) == 0:
            continue
        model = fit_pz(feats.loc[tr_idx], labels.loc[tr_idx])
        pred = predict_pz(model, feats.loc[te_idx])
        oos_pred.append(pred)
        rep = evaluate_pz(pred, labels.loc[te_idx])
        print(f"  {ty}: n={rep['n']:>6,}  logloss={rep['logloss']:.4f} "
              f"(一様 {rep['logloss_prior']:.4f}  Δ{rep['d_logloss']:+.4f})  "
              f"acc={rep['accuracy']:.3f}")
    if not oos_pred:
        raise SystemExit("評価 fold が作れません（年数不足）")
    pooled_pred = pd.concat(oos_pred)
    pooled = evaluate_pz(pooled_pred, labels)
    print(f"pooled OOS: n={pooled['n']:,}  ΔlogLoss={pooled['d_logloss']:+.4f}  "
          f"acc={pooled['accuracy']:.3f}"
          f"  → {'情報あり（β学習の前提を満たす）' if pooled['d_logloss'] < 0 else '情報なし（β学習は不可）'}")

    # ── 4) ストア保存（P(z) は OOS 予測のみ＝前進安全）＋最終モデル ──
    store = pooled_pred.rename(columns={z: f"p_{z}" for z in pooled_pred.columns})
    store.insert(0, "label", labels.reindex(store.index))
    os.makedirs(os.path.dirname(LocalPaths.PACE_STATES_PATH), exist_ok=True)
    store.to_pickle(LocalPaths.PACE_STATES_PATH)
    final_model = fit_pz(feats, labels)
    os.makedirs(os.path.dirname(PACE_MODEL_PATH), exist_ok=True)
    final_model.save_model(PACE_MODEL_PATH)
    print(f"\n保存: {LocalPaths.PACE_STATES_PATH}（{len(store):,}レース・OOS予測のみ）")
    print(f"保存: {PACE_MODEL_PATH}（ライブ予測用・全期間学習）")

    # ── 5) 説明可能性 ─────────────────────────────────────────────
    print("\n何が Slow/Fast を決めたか（SHAP/gain 上位）:")
    for name, imp in explain_pz(final_model, feats.dropna(how="all")):
        print(f"  {name:<18} {imp:,.1f}")


if __name__ == "__main__":
    main()
