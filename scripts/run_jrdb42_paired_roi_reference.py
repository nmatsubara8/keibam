"""市場本命 / 既存B(5特徴) / JRDB(41特徴) の **同一 fold・同一レース paired 比較**（非証拠診断）。

⚠ confirmation ではない。ROI 仮説を 2027 family に登録する価値があるかを判断する前段の**非証拠**診断。
development(2015-2024) rolling-origin OOF で 3 モデルを **同一レース**に対し本命1点(argmax P)で対置し、
絶対 ROI だけでなく **paired 差** を測る:
  ・Δ的中率 / ΔROI（JRDB−市場本命, JRDB−B）
  ・選択馬一致率（JRDB vs 市場本命 / vs B）
  ・市場本命から変更したレースだけの ROI（そこにしか増分は宿らない）
  ・venue×日 block bootstrap の paired ΔROI CI（1レース1点ゆえ per-race paired 差で厳密）

3 モデル:
  market : 市場本命 = argmax q（最終単勝オッズ最小馬・学習不要）
  B5     : frozen 5特徴 residual head（run_residual_head_2027.FROZEN）
  J41    : 凍結41特徴 residual head（run_jrdb42_confirm.FROZEN）

限界: 非証拠(過適合含みうる)・**最終単勝で選択と精算**の近似(購入時オッズでない)・selection 域限定
(2025+ fail-closed)。回収率が控除後(≈0.8)水準に張り付くのは既検証経路の作業帰無参照。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_jrdb42_confirm import FROZEN as J41  # noqa: E402
from scripts.run_residual_head_2027 import FROZEN as B5  # noqa: E402
from src.training._temporal_split import filter_selection_domain, rolling_folds  # noqa: E402


def _pick(probs, odds, winner):
    """本命1点(argmax P)の (uma, hit, payout)。probs 空なら (None,0,0.0)。"""
    if not probs:
        return None, 0, 0.0
    uma = max(probs, key=probs.get)
    o = float(odds.get(uma, 0.0))
    hit = 1 if uma == winner else 0
    return uma, hit, (o if hit else 0.0)


def main() -> int:
    import numpy as np
    from scripts.run_residual_head_2027 import _load_featured
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci

    ap = argparse.ArgumentParser(
        description="市場/ B5 / J41 の同一fold paired ROI 比較（非証拠診断）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl")
    ap.add_argument("--first-eval-year", type=int, default=2018)
    ap.add_argument("--n-boot", type=int, default=10000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    print("=" * 88)
    print("市場本命 / B(5特徴) / JRDB(41特徴) 同一fold paired ROI 比較"
          "（development rolling-origin OOF・⚠非証拠）")
    try:
        feat = _load_featured(args.featured)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2

    from scripts.run_residual_head import build_residual_records
    union = sorted(set(J41["features"]) | set(B5["features"]))
    records, feat_cols = build_residual_records(feat, union, jra_only=True)
    dev, used_years = filter_selection_domain(records)   # 2015-2024 のみ・2025+ fail-closed
    b_cols = [c for c in B5["features"] if c in feat_cols]
    j_cols = [c for c in J41["features"] if c in feat_cols]
    miss_b = [c for c in B5["features"] if c not in feat_cols]
    miss_j = [c for c in J41["features"] if c not in feat_cols]
    if miss_b or miss_j:
        print(f"  [注意] 未実体化 B={miss_b} J41={miss_j}（--with-myspeed 等を確認）", file=sys.stderr)
    folds = rolling_folds(used_years, args.first_eval_year)
    if not folds:
        print("[STOP] fold が作れない。", file=sys.stderr)
        return 3
    by_year: dict = {}
    for r in dev:
        by_year.setdefault(int(r["year"]), []).append(r)
    print(f"[設定] B5={len(b_cols)}列(l2={B5['l2']})  J41={len(j_cols)}列(l2={J41['l2']})  "
          f"folds={[(f'{min(t)}-{max(t)}', e) for t, e in folds]}  ⚠最終単勝で精算(近似)")

    # per-race 蓄積（同一レースで3モデル対応づけ）
    blocks, hit_m, hit_b, hit_j = [], [], [], []
    pay_m, pay_b, pay_j = [], [], []
    agree_mj, agree_bj, changed_mj = [], [], []
    for tr_years, ey in folds:
        train = [r for y in tr_years for r in by_year.get(y, [])]
        test = by_year.get(ey, [])
        if not test:
            continue
        th_b = fit_residual_head(train, b_cols, l2=B5["l2"])
        th_j = fit_residual_head(train, j_cols, l2=J41["l2"])
        for r in test:
            w = r.get("winner")
            odds = r.get("odds") or {}
            if w is None or not odds:
                continue
            q = market_probs(odds)
            pb = residual_win_probs(odds, r["feats"], th_b)
            pj = residual_win_probs(odds, r["feats"], th_j)
            um, hm, pm = _pick(q, odds, w)
            ub, hb, pbp = _pick(pb, odds, w)
            uj, hj, pjp = _pick(pj, odds, w)
            if um is None or uj is None:
                continue
            blocks.append(str(r["race_id"])[:10])
            hit_m.append(hm); hit_b.append(hb); hit_j.append(hj)
            pay_m.append(pm); pay_b.append(pbp); pay_j.append(pjp)
            agree_mj.append(1 if uj == um else 0)
            agree_bj.append(1 if (ub is not None and uj == ub) else 0)
            changed_mj.append(uj != um)

    n = len(blocks)
    if not n:
        print("[STOP] 評価 race が無い。", file=sys.stderr)
        return 3
    pay_m, pay_b, pay_j = map(np.asarray, (pay_m, pay_b, pay_j))
    hit_m, hit_b, hit_j = map(np.asarray, (hit_m, hit_b, hit_j))
    changed = np.asarray(changed_mj, dtype=bool)

    print(f"\n[絶対（本命1点・{n:,}レース）]  {'モデル':>8}{'的中率':>8}{'回収率':>8}")
    for name, hh, pp in (("market", hit_m, pay_m), ("B5", hit_b, pay_b), ("J41", hit_j, pay_j)):
        print(f"  {'':<20}{name:>8}{float(hh.mean()):>8.3f}{float(pp.mean()):>8.3f}")

    def _ci(diff):
        bb = block_bootstrap_ci(list(diff), blocks, n_boot=max(2000, args.n_boot), seed=args.seed)
        return bb["mean"], bb["lo"], bb["hi"]

    print(f"\n[paired 差（venue×日 block bootstrap・1レース1点の per-race 差）]")
    for label, dpay, dhit in (("J41 − market", pay_j - pay_m, hit_j - hit_m),
                              ("J41 − B5", pay_j - pay_b, hit_j - hit_b)):
        m, lo, hi = _ci(dpay)
        dh = float(dhit.mean())
        verdict = "改善(CI下限>0)" if lo > 0 else ("悪化(CI上限<0)" if hi < 0 else "有意差なし(CIが0を跨ぐ)")
        print(f"  {label:<14} ΔROI={m:+.4f} 95%CI[{lo:+.4f},{hi:+.4f}]  Δ的中率={dh:+.4f}  → {verdict}")

    print(f"\n[選択の重なり]  J41 vs market 一致率={float(np.mean(agree_mj)):.3f}  "
          f"J41 vs B5 一致率={float(np.mean(agree_bj)):.3f}")
    nch = int(changed.sum())
    if nch:
        print(f"[市場本命から変更したレースのみ]（n={nch:,}・増分が宿る所）")
        print(f"  そのレースでの ROI  market={float(pay_m[changed].mean()):.3f}  "
              f"J41={float(pay_j[changed].mean()):.3f}  的中率 market={float(hit_m[changed].mean()):.3f}  "
              f"J41={float(hit_j[changed].mean()):.3f}")
        cb = [b for b, c in zip(blocks, changed) if c]
        bb = block_bootstrap_ci(list(pay_j[changed] - pay_m[changed]), cb,
                                n_boot=max(2000, args.n_boot), seed=args.seed)
        print(f"  変更レースの ΔROI(J41−market)={bb['mean']:+.4f} 95%CI[{bb['lo']:+.4f},{bb['hi']:+.4f}]")

    print("\n" + "=" * 88)
    print("⚠ 非証拠（development OOF・過適合含みうる・最終単勝で精算の近似）。回収率が控除後(≈0.8)に")
    print("  張り付くのは既検証経路の作業帰無。paired ΔROI の CI が 0 を明確に跨ぐ/下回るなら、ROI 仮説を")
    print("  2027 family に足す積極的理由はない＝登録せず閉じる。CI 下限>0 が安定して初めて登録を検討。")
    print("  登録する場合も選択/精算を bet-time(TYB) スナップショットへ切替が必須（最終オッズは楽観側）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
