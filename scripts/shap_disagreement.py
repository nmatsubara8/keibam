"""不一致レースの「二頭差分 SHAP」＝なぜ LGBM は市場1番人気でなく別馬を上に置いたかの説明。

研究テーマ（市場×モデル不一致の説明）における点4。SHAP は**判断根拠の説明**であり、ここから
条件を抽出して同じ 2025–2026 で購入ルールを作ると多重探索になる（禁止）。

やること:
  各不一致レースで LGBM本命と市場本命の 2 頭の TreeSHAP 寄与を取り、特徴ごとに
    ΔSHAP_j = SHAP_j(LGBM本命) − SHAP_j(市場本命)
  を計算する（＝「その特徴が、LGBM が市場本命より当該馬を上に置いた理由にどれだけ効いたか」）。
  LightGBM の pred_contrib（raw margin/log-odds 空間）を使うため shap パッケージ不要。

出力:
  ① 全不一致における 平均|ΔSHAP| 上位（不一致を最も説明する特徴）。
  ② 2025・2026 別の上位（年をまたいで方向が同じ特徴が頑健）。
  ③ モデル勝利群 vs 市場勝利群 の 平均ΔSHAP 差（勝敗と結びつく理由の候補・記述のみ）。
  ④ 各レースの上位5理由（|ΔSHAP| 上位、符号付き）。

使い方:
  python scripts/shap_disagreement.py --csv data/disagreement.csv --model-version wf_2025
  # featured/model はローカル（gitignore の巨大成果物）を参照。CI/本コンテナには無いことがある。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _get_booster(eff):
    """較正ラッパー越しに LightGBM Booster を辿る（_model_n_features と同じ探索順）。無ければ None。"""
    for cand in (eff, getattr(eff, "_base_model", None), getattr(eff, "base_estimator", None),
                 getattr(eff, "estimator", None)):
        if cand is None:
            continue
        b = getattr(cand, "booster_", None)
        if b is not None:
            return b
        if cand.__class__.__name__ == "Booster":       # すでに素の Booster
            return cand
    return None


def summarize_delta_shap(delta, feat_names, years, lgbm_won, *, topn=15, per_race_top=5):
    """二頭差分 SHAP 行列(レース×特徴)を要約する純関数（モデル/データ非依存＝単体テスト可能）。

    Parameters
    ----------
    delta : (n_race, n_feat) 配列 ΔSHAP = SHAP(LGBM本命) − SHAP(市場本命)
    feat_names : 長さ n_feat の特徴名
    years : 長さ n_race の年ラベル
    lgbm_won : 長さ n_race の 0/1（そのレースで LGBM本命が勝ったか。決着レースのみ有意）
    """
    import numpy as np
    d = np.asarray(delta, float)
    feat_names = list(feat_names)
    years = list(years)
    lw = np.asarray(lgbm_won, float)
    n_race, n_feat = d.shape
    absmean = np.nanmean(np.abs(d), axis=0)
    order = list(np.argsort(-absmean))

    overall = [(feat_names[j], float(absmean[j]), float(np.nanmean(d[:, j]))) for j in order[:topn]]

    by_year = {}
    for y in sorted(set(years)):
        m = np.array([yy == y for yy in years])
        if not m.any():
            continue
        am = np.nanmean(np.abs(d[m]), axis=0)
        oy = list(np.argsort(-am))[:topn]
        by_year[y] = [(feat_names[j], float(am[j]), float(np.nanmean(d[m][:, j]))) for j in oy]

    # ③ モデル勝利群 vs 市場勝利群 の平均ΔSHAP 差（決着レースのみ）
    won = lw == 1
    lost = lw == 0
    group_diff = []
    if won.any() and lost.any():
        gw = np.nanmean(d[won], axis=0)
        gl = np.nanmean(d[lost], axis=0)
        gd = gw - gl
        for j in np.argsort(-np.abs(gd))[:topn]:
            group_diff.append((feat_names[j], float(gd[j]), float(gw[j]), float(gl[j])))

    # 年をまたいで平均ΔSHAP の符号が一致する特徴（方向が頑健）
    consistent = []
    ys = sorted(set(years))
    if len(ys) >= 2:
        means = {y: np.nanmean(d[np.array([yy == y for yy in years])], axis=0) for y in ys}
        for j in order:
            signs = {np.sign(means[y][j]) for y in ys}
            if len(signs) == 1 and 0 not in signs:
                consistent.append((feat_names[j], float(absmean[j]),
                                   {y: float(means[y][j]) for y in ys}))
        consistent = consistent[:topn]

    per_race = []
    for i in range(n_race):
        row = d[i]
        top = np.argsort(-np.abs(np.nan_to_num(row)))[:per_race_top]
        per_race.append([(feat_names[j], float(row[j])) for j in top])

    return {"overall": overall, "by_year": by_year, "group_diff": group_diff,
            "consistent": consistent, "per_race": per_race, "n_race": n_race}


def _print_summary(s, per_race_races, *, per_race_show=8):
    print(f"\n[①全不一致 平均|ΔSHAP|上位] n={s['n_race']:,}レース（ΔSHAP=LGBM本命−市場本命, raw margin）")
    print(f"  {'特徴':<26}{'平均|Δ|':>10}{'平均Δ(符号)':>13}")
    for f, am, mn in s["overall"]:
        print(f"  {f:<26}{am:>10.4f}{mn:>+13.4f}")
    print("  → 平均|Δ|大＝不一致を最も生む特徴。符号+はLGBM本命側を押し上げた特徴。")

    for y, rows in s["by_year"].items():
        print(f"\n[②{y} 平均|ΔSHAP|上位]")
        print(f"  {'特徴':<26}{'平均|Δ|':>10}{'平均Δ(符号)':>13}")
        for f, am, mn in rows[:10]:
            print(f"  {f:<26}{am:>10.4f}{mn:>+13.4f}")

    if s["consistent"]:
        print("\n[年跨ぎで平均Δの符号が一致する特徴]（方向が頑健＝説明として信頼度高）")
        for f, am, per in s["consistent"][:12]:
            cells = " ".join(f"{y}={v:+.4f}" for y, v in per.items())
            print(f"  {f:<26}{am:>10.4f}  {cells}")

    if s["group_diff"]:
        print("\n[③モデル勝利群−市場勝利群 の平均ΔSHAP差]（決着レースのみ・記述）")
        print(f"  {'特徴':<26}{'群差':>10}{'モ勝μ':>10}{'市勝μ':>10}")
        for f, gd, gw, gl in s["group_diff"]:
            print(f"  {f:<26}{gd:>+10.4f}{gw:>+10.4f}{gl:>+10.4f}")
        print("  → 群差が大きい特徴は『勝敗と結びつく理由』候補。ただし記述であり買い目化しない。")

    print(f"\n[④各レースの上位5理由]（先頭 {per_race_show} レースのみ表示）")
    for rid, reasons in list(zip(per_race_races, s["per_race"], strict=False))[:per_race_show]:
        parts = ", ".join(f"{f}({v:+.3f})" for f, v in reasons)
        print(f"  {rid}: {parts}")


def main() -> int:
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from scripts.sim_ticket_strategy_roi import _select_usable_lgbm_model
    from src.constants._results_cols import ResultsCols
    from src.policies._score_policy import _coerce_for_predict

    ap = argparse.ArgumentParser(description="不一致の二頭差分SHAP（説明・非買い目化）")
    ap.add_argument("--csv", default="data/disagreement.csv")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--model-version", default=None, help="wf_2025 等（未指定は featured 整合の最新）")
    ap.add_argument("--topn", type=int, default=15)
    args = ap.parse_args()

    if not Path(args.csv).exists():
        print(f"CSV がありません: {args.csv}", file=sys.stderr)
        return 1
    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読み込めません（gitignore の巨大成果物。ローカルで実行してください）",
              file=sys.stderr)
        return 2
    model, path, feat_names, report = _select_usable_lgbm_model(
        feat, explicit_version=args.model_version)
    if model is None:
        print("featured と整合する LGBM モデルが models/ にありません:", file=sys.stderr)
        for name, why in report:
            print(f"  - {name}: {why}", file=sys.stderr)
        return 3
    eff = getattr(model, "effective_model", model)
    booster = _get_booster(eff)
    if booster is None:
        print(f"Booster を辿れません（model_class={type(eff).__name__}）", file=sys.stderr)
        return 4
    print(f"[model] {path}  class={type(eff).__name__}  n_feat={len(feat_names)}", file=sys.stderr)

    df = pd.read_csv(args.csv, dtype={"race_id": str, "year": str})
    for c in ("lgbm_hit", "market_hit"):
        df[c] = pd.to_numeric(df.get(c), errors="coerce")

    # featured を (race_id#馬番) で位置引きできるようにする
    uma = pd.to_numeric(feat[ResultsCols.UMABAN], errors="coerce").astype("Int64").astype(str)
    key = np.asarray(feat.index.astype(str), dtype=object) + "#" + np.asarray(uma, dtype=object)
    pos = {k: i for i, k in enumerate(key)}
    X = _coerce_for_predict(feat.reindex(columns=feat_names)).values

    def _k(rid, u):
        u = pd.to_numeric(pd.Series([u]), errors="coerce").astype("Int64").astype(str).iloc[0]
        return f"{rid}#{u}"

    rows, lg_pos, mk_pos = [], [], []
    for _, r in df.iterrows():
        a = pos.get(_k(r["race_id"], r["lgbm_top"]))
        b = pos.get(_k(r["race_id"], r["market_fav"]))
        if a is None or b is None:
            continue
        rows.append(r)
        lg_pos.append(a)
        mk_pos.append(b)
    if not rows:
        print("不一致CSVの本命が featured に照合できません（年/版のズレ）", file=sys.stderr)
        return 5
    sub = pd.DataFrame(rows)
    contrib_lg = np.asarray(booster.predict(X[lg_pos], pred_contrib=True))[:, :-1]  # 末尾=base値を除く
    contrib_mk = np.asarray(booster.predict(X[mk_pos], pred_contrib=True))[:, :-1]
    delta = contrib_lg - contrib_mk

    print(f"=== 二頭差分SHAP（不一致 {len(sub):,}/{len(df):,} レースを照合） {args.csv} ===")
    lgbm_won = pd.to_numeric(sub["lgbm_hit"], errors="coerce").fillna(0).to_numpy()
    s = summarize_delta_shap(delta, feat_names, sub["year"].astype(str).tolist(),
                             lgbm_won, topn=args.topn)
    _print_summary(s, sub["race_id"].astype(str).tolist())
    print("\n※ SHAPは判断根拠の説明。ここで見た特徴で 2025-2026 に購入ルールを作ると多重探索。"
          "条件化するなら事前登録し将来年度で完全OOS検証すること。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
