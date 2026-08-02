"""B: 市場 vs 市場+残差ヘッド の事前登録比較（完全OOS・pace-mixとは独立の仮説）。

_market_residual の定義で baseline=市場(θ≡0→P≡q)、challenger=市場+線形残差ヘッド r_θ(x)
（事前登録した特徴集合・レース内z-score・L2）を rolling-origin OOS で比較する。判定は
paired 開催場×日 block bootstrap の ΔNLL（CI上限<0）× |ΔNLL|≥MES（実用）× ECE非悪化 の3段階。

**事前登録の規律**: 特徴集合(--features)と L2(--l2)と MES(--mes)は**結果を見る前に固定**する
（見てから選ぶと多重探索）。残差ヘッドは pace-mixture の延長でなく独立仮説として評価する。
JRA限定推奨（NAR stub 除外）。featured はローカル成果物。純部は単体テスト済。

使い方（例）:
  python scripts/run_residual_head.py --features d_kinryo_per_weight,jockey_win_te,idm --l2 1.0
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def zscore_within_race(values):
    """レース内 z-score（母集団std・std=0/1頭は0）。numpy 配列を返す純関数。"""
    import numpy as np
    v = np.asarray(values, dtype=float)
    fin = np.isfinite(v)
    if fin.sum() < 2:
        return np.zeros_like(v)
    mu = v[fin].mean()
    sd = v[fin].std()
    z = np.zeros_like(v)
    if sd > 0:
        z[fin] = (v[fin] - mu) / sd
    return z


def build_residual_records(featured, feature_names, *, jra_only=True):
    """featured → レース単位 {race_id,year,winner,odds,feats:{馬番:{f:zscore}}}（発走前特徴のみ）。

    各特徴は**レース内 z-score**（残差ヘッドはスケール依存を除いた相対量で学習）。
    jra_only=True で場コード01-10のみ（NAR stub 除外）。
    """
    import numpy as np
    import pandas as pd

    from src.constants._results_cols import ResultsCols
    rid_all = featured.index.astype(str)
    keep = np.ones(len(featured), dtype=bool)
    if jra_only:
        keep = pd.Series(rid_all).str[4:6].isin({f"{i:02d}" for i in range(1, 11)}).to_numpy()
    df = featured[keep]
    rid = df.index.astype(str)
    uma = pd.to_numeric(df.get(ResultsCols.UMABAN), errors="coerce").to_numpy()
    odds = pd.to_numeric(df.get(ResultsCols.TANSHO_ODDS), errors="coerce").to_numpy()
    rank = pd.to_numeric(df.get(ResultsCols.RANK), errors="coerce").to_numpy()
    feat_cols = [c for c in feature_names if c in df.columns]
    missing = [c for c in feature_names if c not in df.columns]
    if missing:
        print(f"  [警告] featured に無い特徴（無視）: {missing}", file=sys.stderr)
    fvals = {c: pd.to_numeric(df[c], errors="coerce").to_numpy() for c in feat_cols}
    recs: dict[str, dict] = {}
    order: dict[str, list] = {}
    for i, r in enumerate(rid):
        rec = recs.setdefault(r, {"race_id": r, "year": int(r[:4]) if r[:4].isdigit() else None,
                                  "winner": None, "odds": {}, "feats": {}, "_idx": []})
        u = uma[i]
        if u != u:
            continue
        rec["_idx"].append(i)
    # レース内 z-score を作って詰める
    out = []
    for r, rec in recs.items():
        idx = rec["_idx"]
        if len(idx) < 3:
            continue
        umas = [int(uma[i]) for i in idx]
        z = {c: zscore_within_race([fvals[c][i] for i in idx]) for c in feat_cols}
        for k, i in enumerate(idx):
            o = odds[i]
            if o != o or o <= 0:
                continue
            ub = umas[k]
            rec["odds"][ub] = float(o)
            rec["feats"][ub] = {c: float(z[c][k]) for c in feat_cols}
            if rank[i] == 1:
                rec["winner"] = ub
        rec.pop("_idx", None)
        if rec["winner"] is not None and len(rec["odds"]) >= 3:
            out.append(rec)
    return out, feat_cols


def main() -> int:
    import numpy as np

    from app._model_eval import load_featured_data
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, race_nll
    from src.simulation._rolling_origin import rolling_origin_compare, rolling_origin_folds

    ap = argparse.ArgumentParser(description="市場 vs 市場+残差ヘッド（事前登録・完全OOS）")
    ap.add_argument("--features", default=None,
                    help="事前登録する特徴名（カンマ区切り・結果を見る前に固定すること）")
    ap.add_argument("--list-features", action="store_true",
                    help="featured の候補特徴名を出して終了（事前登録の前に実名を確認する）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--l2", type=float, default=1.0, help="残差ヘッドの L2（事前登録）")
    ap.add_argument("--mes", type=float, default=0.001, help="運用上の最小実用効果量（結果後に下げない）")
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=2000)
    ap.add_argument("--allow-nar", action="store_true", help="NAR も含める（既定は JRA 限定）")
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行）", file=sys.stderr)
        return 2

    if args.list_features:
        import numpy as np
        num = feat.select_dtypes(include=[np.number, "bool"]).columns
        # 市場非依存の候補になりうる族を優先表示（オッズ/impl は直交でないため注記）
        groups = {
            "JRDB指数": [c for c in num if str(c).startswith("jrdb_")],
            "予想(yoso)": [c for c in num if str(c).startswith("yoso_")],
            "能力(elo/speed)": [c for c in num if str(c).startswith(("elo_", "speed_fig"))],
            "適性(wet/dist/ground)": [c for c in num if any(k in str(c) for k in
                                       ("wet_", "at_distance", "type_ground"))],
            "斤量/馬体": [c for c in num if any(k in str(c) for k in ("kinryo", "体重", "斤量"))],
        }
        print(f"[featured 候補特徴]  全数値列 {len(num)}。市場非依存になりうる族（オッズ/impl は直交でない）:")
        for g, cols in groups.items():
            print(f"  [{g}] {cols[:20]}")
        odds_like = [c for c in num if any(k in str(c) for k in ("単勝", "odds", "impl", "kijun_odds"))]
        print(f"  [注意: 市場由来＝残差ヘッドに入れない] {odds_like[:20]}")
        return 0

    if not args.features:
        print("--features を指定（先に --list-features で実名確認・結果を見る前に事前登録）", file=sys.stderr)
        return 1
    feats_req = [c.strip() for c in args.features.split(",") if c.strip()]
    records, feat_cols = build_residual_records(feat, feats_req, jra_only=not args.allow_nar)
    print(f"[事前登録] 特徴={feat_cols}  L2={args.l2}  MES={args.mes}  "
          f"JRA限定={not args.allow_nar}")
    print(f"[records] {len(records):,} レース（勝ち馬確定・3頭以上・レース内z-score）")
    if not records or not feat_cols:
        print("レコード/特徴が空。--features を確認。", file=sys.stderr)
        return 3

    def fit_baseline(_train):
        return None

    def prob_baseline(_p, race):
        return market_probs(race["odds"])

    def fit_challenger(train):
        return fit_residual_head(train, feat_cols, l2=args.l2)

    def prob_challenger(theta, race):
        if not theta or all(v == 0 for v in theta.values()):
            return market_probs(race["odds"])
        return residual_win_probs(race["odds"], race["feats"], theta)

    res = rolling_origin_compare(
        records, fit_baseline, prob_baseline, fit_challenger, prob_challenger,
        min_train_years=args.min_train_years, k_extra_params=len(feat_cols), n_boot=args.n_boot)
    pooled = res["pooled"]

    # 開催場×日 block bootstrap（同日相関を保存）で ΔNLL の主判定
    dnll, days = [], []
    for train, test, _y in rolling_origin_folds(records, min_train_years=args.min_train_years):
        theta = fit_challenger(train)
        for r in test:
            if r.get("winner") is None:
                continue
            pb, pc = prob_baseline(None, r), prob_challenger(theta, r)
            if pb and pc:
                dnll.append(race_nll(pc, r["winner"]) - race_nll(pb, r["winner"]))
                days.append(str(r["race_id"])[:10])
    bb = block_bootstrap_ci(dnll, days, n_boot=max(2000, args.n_boot))
    de = pooled.get("d_ece")

    print(f"\n=== 市場+残差ヘッド vs 市場（rolling-origin {res['n_folds']} folds）===")
    print(f"ΔNLL(pooled)={pooled.get('d_nll'):+.6f}  ΔECE={de:+.6f}  LRT_p={pooled.get('lrt_p')}")
    print(f"[開催場×日 block CI] mean={bb['mean']:+.6f} 95%CI[{bb['lo']:+.6f},{bb['hi']:+.6f}] "
          f"ブロック={bb['n_blocks']:,}")
    print(f"  {'年':>6}{'n':>7}{'ΔNLL':>11}")
    for f in res["folds"]:
        v = f.get("d_nll")
        print(f"  {f['year']:>6}{f['n']:>7,}  {v:+.6f}" if isinstance(v, float) else f"  {f['year']} n/a")

    sig = bb["hi"] < 0
    practical = abs(bb["mean"]) >= args.mes
    ECE_TOL = 5e-3  # 事前固定（_model_compare 標準「ΔECE ≤ +0.005」・結果を見て変えない）
    ece_ok = (de is None) or (de <= ECE_TOL)
    print(f"  [ECE] ΔECE={de:+.6f}  許容=+{ECE_TOL}（事前固定・_model_compare標準／符号は微悪化だが帯域内）"
          if de is not None and de > 0 else f"  [ECE] ΔECE={de}")
    if sig and practical and ece_ok:
        verdict = "✅ 本番採用候補（CI上限<0・|効果量|≥MES・ECE非悪化）"
    elif sig and ece_ok:
        verdict = f"🟡 統計的改善のみ・shadow限定（|ΔNLL|={abs(bb['mean']):.6f}<MES {args.mes}）"
    else:
        verdict = "❌ 改善なし（CIが0跨ぎ or ECE悪化）＝市場に無い直交情報を残差ヘッドは持たない"
    print(f"\n判定: {verdict}")
    print("※ 特徴集合/L2/MES は事前登録済み前提。ここで特徴を足して再試行すると多重探索。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
