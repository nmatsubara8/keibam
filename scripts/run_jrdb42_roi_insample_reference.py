"""JRDB42 の **in-sample 的中率/回収率 参考値**（development 2015-2024・rolling-origin・⚠非証拠）。

⚠ confirmation ではない。NLL 参考(run_jrdb42_insample_reference.py)の ROI 版。development_known の内部
rolling-origin で、41特徴を入れた market-anchored residual head の勝率 P から 2 運用点で的中率/回収率を測る:
  ・本命（的中率重視）: 各レース argmax P を1点買い。
  ・EV妙味（回収率重視）: EV=P·odds>閾値 の馬だけ買い、無ければ見送り。
selection 域限定（2025+ は fail-closed）。features=41・l2=1.0 は凍結一致。

**重大な限界（必読）**:
- **非証拠**（selection 域・過適合を含みうる）。採否は ROI family を別途事前登録し 2027 で一度だけ。
- 精算は featured の **最終単勝オッズ**を選択にも決済にも使う近似（購入時オッズでない）。実運用の
  bet-time 契約(TYB)とは別。参考値の域を出ない。
- 市場半強効率で 5経路 null 済み＝**回収率は ≈ 1−控除率**に張り付くのが帰無。負に出て当然。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.run_jrdb42_confirm import FROZEN  # noqa: E402
from src.training._temporal_split import filter_selection_domain, rolling_folds  # noqa: E402


def _bet_metrics(test, theta):
    """test records を本命/EV運用点で評価し、per-race honmei PnL と EV bets を返す（純ロジック）。"""
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import residual_win_probs
    honmei_pnl, honmei_blocks, honmei_hit = [], [], []
    ev_bets = {th: {"n": 0, "win": 0, "pay_list": [], "blocks": []}
               for th in (1.0, 1.1, 1.2, 1.3)}
    use = bool(theta) and not all(v == 0 for v in theta.values())
    for r in test:
        w = r.get("winner")
        odds = r.get("odds") or {}
        if w is None or not odds:
            continue
        probs = residual_win_probs(odds, r["feats"], theta) if use else market_probs(odds)
        if not probs:
            continue
        blk = str(r["race_id"])[:10]
        # 本命: argmax P を1点
        star = max(probs, key=probs.get)
        o_star = float(odds.get(star, 0.0))
        win = 1 if star == w else 0
        payout = o_star if win else 0.0
        honmei_pnl.append(payout - 1.0)
        honmei_blocks.append(blk)
        honmei_hit.append(win)
        # EV妙味: EV=P*odds>閾値 の全馬（block 単位 bootstrap 用に per-bet payout/block も保持）
        for uma, p in probs.items():
            o = float(odds.get(uma, 0.0))
            ev = p * o
            for th, acc in ev_bets.items():
                if ev > th:
                    hit = 1 if uma == w else 0
                    acc["n"] += 1
                    acc["win"] += hit
                    acc["pay_list"].append(o if hit else 0.0)
                    acc["blocks"].append(blk)
    return honmei_pnl, honmei_blocks, honmei_hit, ev_bets


def main() -> int:
    import numpy as np
    from scripts.run_jrdb42_confirm import _build_records
    from scripts.run_residual_head_2027 import _load_featured
    from src.policies._residual_head import fit_residual_head
    # ROI=Σpayout/Σbets の venue×日 block bootstrap は block_bootstrap_ci（bsum/bcnt=Σ値/Σ件）で
    # 賄える。bet 独立でなく block 単位で再標本化し高配当数件に効く CI を得る（専用実装は不要）。
    from src.simulation._model_compare import block_bootstrap_ci
    from src.training._feature_materialization import missing_frozen_hint

    ap = argparse.ArgumentParser(
        description="JRDB42 in-sample 的中率/回収率 参考（development 2015-2024・非証拠）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl")
    ap.add_argument("--first-eval-year", type=int, default=2018)
    ap.add_argument("--n-boot", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    print("=" * 84)
    print("JRDB42 in-sample 的中率/回収率 参考（development 2015-2024・rolling-origin・⚠非証拠）")
    try:
        feat = _load_featured(args.featured)
    except RuntimeError as e:
        print(f"[エラー] {e}", file=sys.stderr)
        return 2
    records, feat_cols = _build_records(feat)
    dev, used_years = filter_selection_domain(records)   # 2015-2024 のみ・2025+ fail-closed
    missing = [c for c in FROZEN["features"] if c not in feat.columns]
    if missing:
        print(f"[STOP] 凍結特徴 未実体化 {len(missing)} 列{missing_frozen_hint(missing)}: "
              f"{missing[:8]}", file=sys.stderr)
        return 3

    folds = rolling_folds(used_years, args.first_eval_year)
    if not folds:
        print("[STOP] fold が作れない。", file=sys.stderr)
        return 3
    by_year: dict = {}
    for r in dev:
        by_year.setdefault(int(r["year"]), []).append(r)
    print(f"[設定] features={len(FROZEN['features'])} l2={FROZEN['l2']} "
          f"folds={[(f'{min(t)}-{max(t)}', e) for t, e in folds]}  ⚠最終単勝オッズで精算(近似)")

    all_pnl, all_blk, all_hit = [], [], []
    ev_tot = {th: {"n": 0, "win": 0, "pay_list": [], "blocks": []}
              for th in (1.0, 1.1, 1.2, 1.3)}
    print(f"\n  本命(argmax P・1点/レース)  {'eval年':>6}{'races':>8}{'的中率':>8}{'回収率':>8}")
    for tr_years, ey in folds:
        train = [r for y in tr_years for r in by_year.get(y, [])]
        test = by_year.get(ey, [])
        if not test:
            continue
        theta = fit_residual_head(train, feat_cols, l2=FROZEN["l2"])
        pnl, blk, hit, ev = _bet_metrics(test, theta)
        all_pnl.extend(pnl); all_blk.extend(blk); all_hit.extend(hit)
        for th in ev_tot:
            ev_tot[th]["n"] += ev[th]["n"]
            ev_tot[th]["win"] += ev[th]["win"]
            ev_tot[th]["pay_list"].extend(ev[th]["pay_list"])
            ev_tot[th]["blocks"].extend(ev[th]["blocks"])
        hr = float(np.mean(hit)) if hit else 0.0
        rr = float(np.mean([p + 1.0 for p in pnl])) if pnl else 0.0
        print(f"  {'':<26}{ey:>6}{len(pnl):>8,}{hr:>8.3f}{rr:>8.3f}")

    if not all_pnl:
        print("[STOP] 評価 race が無い。", file=sys.stderr)
        return 3
    hr = float(np.mean(all_hit))
    rr = float(np.mean([p + 1.0 for p in all_pnl]))
    bb = block_bootstrap_ci(all_pnl, all_blk, n_boot=max(2000, args.n_boot), seed=args.seed)
    print(f"\n[本命 pooled] races={len(all_pnl):,}  的中率={hr:.3f}  回収率={rr:.3f}  "
          f"(ROI-1) 95%CI[{bb['lo']:+.4f},{bb['hi']:+.4f}]  ※CI 上限<0 なら控除に負ける")
    # EV妙味: venue×日 block bootstrap で ROI=総払戻/総購入額 を各リサンプルで再計算（bet 単位でなく
    # block 単位で再標本化・複数頭買いレースの相関を保つ）。少数 bet・高配当に効く CI を必ず併記する。
    print(f"\n[EV妙味 pooled]  {'閾値':>6}{'bets':>9}{'的中率':>8}{'回収率':>8}{'ROI 95%CI(venue×日block)':>26}")
    for th, acc in ev_tot.items():
        n = acc["n"]
        hh = acc["win"] / n if n else 0.0
        rrr = (sum(acc["pay_list"]) / n) if n else 0.0
        blk = acc["blocks"]
        ci = ""
        if n >= 20 and len(set(blk)) >= 2:   # block_bootstrap_ci は <2 block で ValueError
            bb_ev = block_bootstrap_ci(acc["pay_list"], blk, n_boot=max(2000, args.n_boot),
                                       seed=args.seed)
            ci = f"[{bb_ev['lo']:.3f},{bb_ev['hi']:.3f}]"
        print(f"  {'':<12}{th:>6.1f}{n:>9,}{hh:>8.3f}{rrr:>8.3f}{ci:>26}")

    print("\n" + "=" * 84)
    print("⚠ 非証拠（development rolling-origin OOF・selection 設計は 2015-2024 を見た後＝独立証拠でない・")
    print("  過適合含みうる・最終単勝で選択と精算する近似＝購入時オッズでない楽観側）。")
    print("  既検証経路では実用的な控除超過を確認していない＝回収率≈控除後(≈0.8)を作業上の帰無参照とする")
    print("  （市場の半強効率性を証明した訳ではない・未ブリッジ source/映像等は未検証）。")
    print("  採否は市場本命/B との同一fold paired 比較（run_jrdb42_paired_roi_reference.py）を先に見て判断。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
