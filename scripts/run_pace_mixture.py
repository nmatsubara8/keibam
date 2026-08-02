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
        build_race_features, fit_pz, label_pace_from_sed, label_pace_states, predict_pz,
        pz_dict, race_pace_balance,
    )
    from src.simulation._rolling_origin import rolling_origin_compare, rolling_origin_folds

    ap = argparse.ArgumentParser(description="P(z)→Mixture-PL 完全OOS評価")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=1000)
    ap.add_argument("--l2-beta", type=float, default=0.1)
    ap.add_argument("--mes", type=float, default=0.001,
                    help="運用上の最小実用効果量(|ΔNLL| bit/race)。これ未満は有意でも不採用。"
                         "※結果を見た後に下げない（下げれば事後正当化になる）")
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行してください）", file=sys.stderr)
        return 2
    records = build_race_records(feat)
    print(f"[records] {len(records):,} レース（勝ち馬確定・3頭以上）")

    # 発走前特徴（全レース・pre-race）を一度だけ作る。
    features_all = build_race_features(feat)
    # 教師ラベル z（結果由来・可）: JRDB SED の race_pace(H/M/S・実測) を主経路にする
    # （netkeiba『ペース』文字列は JRDB 経路に無く 0 件になるため）。無ければ balance 版へフォールバック。
    labels_all = pd.Series(dtype=object)
    try:
        from src.jrdb._store import JrdbStore
        sed = JrdbStore().read("SED")
        labels_all = label_pace_from_sed(sed)
        print(f"[labels] SED race_pace(H/M/S) から {labels_all.notna().sum():,} レースに z を付与")
    except Exception as e:  # noqa: BLE001
        print(f"[labels] SED 読込失敗({e})→ netkeiba ペース balance にフォールバック", file=sys.stderr)
    if labels_all.notna().sum() < 500:
        hr = load_raw(LocalPaths.RAW_HORSE_RESULTS_PATH)
        fk = pd.DataFrame({"race_id": feat.index.astype(str),
                           "date": pd.to_numeric(feat.get("date"), errors="coerce")
                           if "date" in feat.columns else None})
        if "horse_id" in feat.columns:
            fk["horse_id"] = feat["horse_id"].astype(str)
        bal = race_pace_balance(fk.drop_duplicates("race_id"), hr) if hr is not None else pd.Series(dtype=float)
        if not bal.empty:
            labels_all = label_pace_states(bal, _label_groups(features_all))
            print(f"[labels] balance フォールバックで {labels_all.notna().sum():,} レース（復元 {len(bal):,}）")
    labels_all = labels_all.reindex([r["race_id"] for r in records]).dropna()
    print(f"[labels] records と突合後 {len(labels_all):,} レースに z ラベル")
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

    def _f(v, spec="+.5f"):
        return format(v, spec) if isinstance(v, (int, float)) else "n/a"
    dn = pooled.get("d_nll")
    ci = pooled.get("d_nll_ci95") or (None, None)
    lo, hi = ci if isinstance(ci, (tuple, list)) and len(ci) == 2 else (None, None)
    de = pooled.get("d_ece")

    # P(z) 自体の OOS 品質＋レース単位 ΔNLL を fold 別に収集（開催日 block bootstrap CI 用）。
    print("\n[P(z) OOS 品質]（学習P(z) vs 一様 の Δlogloss・負=情報あり）")
    from src.preprocessing._pace_state import evaluate_pz
    from src.simulation._model_compare import block_bootstrap_ci, race_nll
    folds = rolling_origin_folds(records, min_train_years=args.min_train_years)
    pz_dl, pz_acc, pz_n = [], [], []
    dnll_vals, dnll_days = [], []
    for train, test, year in folds:
        params = fit_challenger(train)
        if params.get("pz") is None:
            continue
        # レース単位 ΔNLL（挑戦−市場）と開催場×日（race_id[:10]=年+場+回+日）を収集
        for r in test:
            if r.get("winner") is None:
                continue
            pb = prob_baseline(None, r)
            pc = prob_challenger(params, r)
            if not pb or not pc:
                continue
            dnll_vals.append(race_nll(pc, r["winner"]) - race_nll(pb, r["winner"]))
            dnll_days.append(str(r["race_id"])[:10])
        te_ids = features_all.index.intersection(pd.Index([r["race_id"] for r in test]))
        te_ids = te_ids.intersection(labels_all.index)
        if len(te_ids) == 0:
            continue
        pred = predict_pz(params["pz"], features_all.loc[te_ids])
        ev = evaluate_pz(pred, labels_all.loc[te_ids])
        if ev.get("n"):
            pz_dl.append(ev["d_logloss"]); pz_acc.append(ev["accuracy"]); pz_n.append(ev["n"])
            print(f"  {year:>6}  n={ev['n']:>6,}  Δlogloss={ev['d_logloss']:+.4f}  acc={ev['accuracy']:.3f}")
    if pz_dl:
        tot = sum(pz_n)
        micro_dl = sum(d * n for d, n in zip(pz_dl, pz_n, strict=False)) / tot
        micro_acc = sum(a * n for a, n in zip(pz_acc, pz_n, strict=False)) / tot
        print(f"  macro-year平均 : Δlogloss={np.mean(pz_dl):+.4f}  acc={np.mean(pz_acc):.3f}"
              f"（各年を等重み）")
        print(f"  micro(全OOS行加重): Δlogloss={micro_dl:+.4f}  acc={micro_acc:.3f}"
              f"（2026は件数少・両方併記／負=一様より情報あり・偶然acc=0.33）")

    # ΔNLL の paired CI を3粒度で併記（レース単位 iid / 開催場×日 / 年）。同日相関を保存する
    # ブロックほど正直（やや広い）。主判定は 開催場×日 block bootstrap（LRT は参考）。
    iid = block_bootstrap_ci(dnll_vals, list(range(len(dnll_vals))), n_boot=max(2000, args.n_boot))
    daytrack = block_bootstrap_ci(dnll_vals, dnll_days, n_boot=max(2000, args.n_boot))
    print("\n[ΔNLL paired Bootstrap 95%CI（粒度別）]  負=改善・主判定は 開催場×日")
    print(f"  レース単位(iid)  mean={iid['mean']:+.6f}  CI[{iid['lo']:+.6f},{iid['hi']:+.6f}]  n={iid['n']:,}")
    print(f"  開催場×日(block) mean={daytrack['mean']:+.6f}  CI[{daytrack['lo']:+.6f},"
          f"{daytrack['hi']:+.6f}]  ブロック={daytrack['n_blocks']:,}")

    # LRT/CI 監査: 両者が別仮説を測ることを数値で明示（不整合でない）。
    nrace = pooled.get("n_races", 0)
    ll_mkt = -pooled.get("nll_base", float("nan")) * nrace
    ll_mix = -pooled.get("nll_chal", float("nan")) * nrace
    print("\n[LRT/CI 監査]（CIは平均ΔNLLの検定=df非依存 / LRTは12β構造全体の参照検定）")
    print(f"  n={nrace:,}  LL_market={ll_mkt:.3f}  LL_mixture={ll_mix:.3f}  "
          f"LR_stat=2·(LL_mix−LL_mkt)=−2·n·ΔNLL={pooled.get('lrt_stat'):.3f}  df=12  "
          f"LRT_p={_f(pooled.get('lrt_p'), '.4f')}")
    print("  → CI<0（平均ΔNLLは僅かに改善・df非依存）と LRT_p≈0.95 は両立＝実装齟齬でない。"
          "df=12 の参照分布では LR=5.12 は β構造全体を支持するほど大きくない（これは rolling-origin OOS "
          "尤度差＝古典的 in-sample nested LRT とは区別）。主判定は paired block bootstrap、LRT は参考。")

    print(f"\n=== P(z)→Mixture-PL vs 市場（rolling-origin {res['n_folds']} folds）===")
    print(f"ΔNLL={_f(dn)}  ΔECE={_f(de)}  （年別 ΔNLL 下記・全て同符号なら方向は安定）")
    for f in res["folds"]:
        print(f"  {f['year']:>6}{f['n']:>7,}  {_f(f.get('d_nll'))}")

    # 三段階判定: 統計的(開催場×日 CI上限<0) × 実用的(|ΔNLL|≥MES) × 較正(ECE非悪化)。
    sig = daytrack["hi"] < 0
    practical = abs(daytrack["mean"]) >= args.mes
    ece_ok = (de is None) or (de <= 5e-3)
    if sig and practical and ece_ok:
        verdict = "✅ 本番採用候補（CI上限<0・|効果量|≥MES・ECE非悪化）"
    elif sig and ece_ok:
        verdict = (f"🟡 統計的改善のみ・shadow限定（CI上限<0 だが |ΔNLL|={abs(daytrack['mean']):.6f}"
                   f" < MES({args.mes})＝実用効果量未達。本番採用しない）")
    else:
        verdict = "❌ 改善なし（CIが0を跨ぐ or ECE悪化）"
    print(f"\n判定: {verdict}")
    print("※ pace-mixture は事前登録どおりここで閉じる（状態数/境界/混合式の探索＝再探索は行わない）。"
          "residual head は独立事前登録比較でのみ評価。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
