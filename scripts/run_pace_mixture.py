"""P(z)予測器→Mixture-PL の本番評価＝学習した P(z) が市場アンカーを超えるかを完全OOSで測る。

Step3 の識別性知見「一様 P(z)+勝者NLL では β 識別不能」を、発走前特徴から学習した**レース別 P(z)**で
解く。本スクリプトは rolling-origin（テスト年より過去のみで fit）で:
  baseline  : 市場そのもの（uniform P(z)・β=0 → mixture は q に厳密退化）
  challenger: 学習 P(z) ＋ fit した β(style,z)（residual=0 で pace-mixture 寄与だけを分離）
を compare_models（ΔNLL/ΔECE/Bootstrap/LRT）で比較し、P(z) 自体の品質(Δlogloss)も fold 別に出す。

成功判定（事前定義・_mixture_pl docstring）: ΔNLL<0 かつ CI/LRT 有意 かつ ECE 非悪化。ROI 単独で判断しない。

leak 規律: ラベル z は結果由来(教師・可)、**特徴量は発走前のみ**。fit は各 fold の過去レースのみ。
featured/horse_results はローカルの巨大成果物（このスクリプトはローカル実行）。純粋部は単体テスト済。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def build_race_records(featured):
    """featured(per-horse・index=race_id) → レース単位レコード列（発走前情報＋勝ち馬）。

    各レコード: {race_id, year, winner(馬番), odds:{馬番:単勝}, styles:{馬番:脚質}}。
    脚質は pace_median(過去走の位置比・発走前)から style_from_pace_ratio で決める。
    """
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    from src.policies._mixture_pl import style_from_pace_ratio
    rid = pd.Series(featured.index.astype(str), index=featured.index)
    uma = pd.to_numeric(featured.get(ResultsCols.UMABAN), errors="coerce")
    odds = pd.to_numeric(featured.get(ResultsCols.TANSHO_ODDS), errors="coerce")
    rank = pd.to_numeric(featured.get(ResultsCols.RANK), errors="coerce")
    pm = (pd.to_numeric(featured["pace_median"], errors="coerce")
          if "pace_median" in featured.columns else pd.Series(index=featured.index, dtype=float))
    recs: dict[str, dict] = {}
    for i, r in enumerate(rid):
        u = uma.iloc[i]
        o = odds.iloc[i]
        if pd.isna(u) or pd.isna(o) or o <= 0:
            continue
        rec = recs.setdefault(r, {"race_id": r, "year": int(r[:4]) if r[:4].isdigit() else None,
                                  "winner": None, "odds": {}, "styles": {}})
        ub = int(u)
        rec["odds"][ub] = float(o)
        rec["styles"][ub] = style_from_pace_ratio(None if pd.isna(pm.iloc[i]) else float(pm.iloc[i]))
        if pd.notna(rank.iloc[i]) and int(rank.iloc[i]) == 1:
            rec["winner"] = ub
    return [r for r in recs.values() if r["winner"] is not None and len(r["odds"]) >= 3]


def _label_groups(features):
    """ラベル分割用グループ（芝ダ×距離帯）を features から作る（レース条件相対の分位切り用）。"""
    import pandas as pd
    band = pd.cut(pd.to_numeric(features.get("course_len"), errors="coerce"),
                  bins=[0, 1400, 1800, 2200, 9999], labels=["s", "m", "l", "xl"])
    return pd.DataFrame({"is_dirt": features.get("is_dirt"), "band": band}, index=features.index)


def main() -> int:
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.pipeline._ingestion import load_raw
    from src.constants._local_paths import LocalPaths
    from src.policies._market_residual import market_probs
    from src.policies._mixture_pl import fit_beta_fast, mixture_win_probs
    from src.preprocessing._pace_state import (
        build_race_features, fit_pz, label_pace_states, predict_pz, pz_dict, race_pace_balance,
    )
    from src.simulation._rolling_origin import rolling_origin_compare

    ap = argparse.ArgumentParser(description="P(z)→Mixture-PL 完全OOS評価")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=1000)
    ap.add_argument("--l2-beta", type=float, default=0.1)
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行してください）", file=sys.stderr)
        return 2
    records = build_race_records(feat)
    print(f"[records] {len(records):,} レース（勝ち馬確定・3頭以上）")

    # 発走前特徴（全レース・pre-race）と 教師ラベル z（結果由来・可）を一度だけ作る。
    features_all = build_race_features(feat)
    hr = load_raw(LocalPaths.RAW_HORSE_RESULTS_PATH)
    fk = pd.DataFrame({"race_id": feat.index.astype(str),
                       "date": pd.to_numeric(feat.get("date"), errors="coerce")
                       if "date" in feat.columns else None})
    if "horse_id" in feat.columns:
        fk["horse_id"] = feat["horse_id"].astype(str)
    bal = race_pace_balance(fk.drop_duplicates("race_id"), hr) if hr is not None else pd.Series(dtype=float)
    labels_all = label_pace_states(bal, _label_groups(features_all)) if not bal.empty else pd.Series(dtype=object)
    print(f"[labels] ペースラベル付き {labels_all.notna().sum():,} レース"
          f"（balance 復元 {len(bal):,}）")
    if labels_all.notna().sum() < 500:
        print("  [警告] ラベル数が少なすぎます（horse_results の『ペース』列を確認）。", file=sys.stderr)

    # baseline: 市場（uniform P(z)・β=0 → mixture は q へ退化）
    def fit_baseline(_train):
        return None

    def prob_baseline(_p, race):
        return market_probs(race["odds"])

    # challenger: 学習 P(z) ＋ fit β（各 fold の過去のみで fit）
    def fit_challenger(train):
        ids = [r["race_id"] for r in train]
        xy_ids = features_all.index.intersection(pd.Index(ids)).intersection(labels_all.dropna().index)
        if len(xy_ids) < 300:
            return {"pz": None, "beta": None}
        try:
            pzm = fit_pz(features_all.loc[xy_ids], labels_all.loc[xy_ids])
        except Exception as e:  # noqa: BLE001 — lightgbm 不在/失敗は baseline 同等へ
            print(f"  [fit_pz 失敗→β=0] {e}", file=sys.stderr)
            return {"pz": None, "beta": None}
        # train 各レースに 学習 P(z) を付けて β を fit（residual=0 で pace 寄与のみ）
        tr_feat = features_all.reindex([r["race_id"] for r in train])
        pzp = predict_pz(pzm, tr_feat)
        races_b = [{"odds": r["odds"], "styles": r["styles"], "winner": r["winner"],
                    "pace_probs": pz_dict(pzp.loc[r["race_id"]])}
                   for r in train if r["race_id"] in pzp.index]
        beta = fit_beta_fast(races_b, l2_beta=args.l2_beta)
        return {"pz": pzm, "beta": beta}

    def prob_challenger(params, race):
        if params.get("pz") is None or params.get("beta") is None:
            return market_probs(race["odds"])
        rid = race["race_id"]
        if rid not in features_all.index:
            return market_probs(race["odds"])
        pz = pz_dict(predict_pz(params["pz"], features_all.loc[[rid]]).loc[rid])
        return mixture_win_probs(race["odds"], {}, race["styles"], params["beta"], pz)

    res = rolling_origin_compare(
        records, fit_baseline, prob_baseline, fit_challenger, prob_challenger,
        min_train_years=args.min_train_years, k_extra_params=12, n_boot=args.n_boot,
    )
    pooled = res["pooled"]
    print(f"\n=== P(z)→Mixture-PL vs 市場（rolling-origin {res['n_folds']} folds）===")
    print(f"ΔNLL={pooled.get('d_nll'):+.5f}  95%CI[{pooled.get('d_nll_lo'):+.5f},"
          f"{pooled.get('d_nll_hi'):+.5f}]  ΔECE={pooled.get('d_ece'):+.5f}  "
          f"LRT_p={pooled.get('lrt_p')}")
    print(f"  {'年':>6}{'n':>7}{'ΔNLL':>10}")
    for f in res["folds"]:
        print(f"  {f['year']:>6}{f['n']:>7,}{f['d_nll']:>+10.5f}")
    dn = pooled.get("d_nll")
    lo, hi = pooled.get("d_nll_lo"), pooled.get("d_nll_hi")
    de = pooled.get("d_ece")
    ok = (dn is not None and dn < 0 and hi is not None and hi < 0
          and de is not None and de <= 1e-4)
    print("\n判定: " + ("✅ 採用候補（ΔNLL<0・CI上限<0・ECE非悪化）" if ok
                        else "❌ 不採用（市場アンカーを有意には超えない）") +
          "。※事前登録＝pace-mixture のみ（residual head は別軸）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
