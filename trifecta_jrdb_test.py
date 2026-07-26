"""三連単×JRDB展開指数 VOI テスト — 連系の順序に JRDB の着順予測が edge を持つか。

背景（本セッションの確定台帳）: 公開データ・JRDB・オッズ変動の全経路で、最終JRA**単勝**
市場は効率的＝edge無しが確定した。残る未探索は「単勝以外＝連系プール」。仮説:
**群衆は単勝(P1着)を効率的に価格づけするが、三連単の順序(2/3着)には非効率が残り、JRDBの
展開予想（ゴール順位/位置指数）がそれを捉える。**

機構: 位置特化トリフェクタ（_harville.prob_trifecta_place_strength）。
  1着=市場単勝そのまま π=softmax(log q_win)。
  2/3着=JRDB調整の place 強度 σ=τ=softmax(log q_win + Σ coef_c·z(JRDB_c))。
  coef≡0 で素の Harville に退化（帰無）。coef は train の trifecta NLL 最小で fit。
共通ハーネス（レース読込・place強度較正・係数fit）は src.simulation._order_model に集約。

判定（2段）:
- Stage A（本スクリプト・JRDB データのみで可）: 実着順(SED chakujun)の trifecta listwise NLL を
  baseline(Harville from 単勝) と比較。OOS＋Bootstrap CI＋placebo(signalシャッフル)。
  **確認済み(2025-26中央4,526R・フル直前suite9指数): ΔNLL−0.0223 CI上端<0 / placebo で消失。**
- Stage B（要 return_tables＝連系払戻・ユーザー環境）: 実際の三連単配当で ROI を測り、
  控除(約27.5%)を超えるか。NLL改善(較正)が ROI に化けるかは別問題（multibet_roi_test）。

実行: python trifecta_jrdb_test.py --jrdb-dir /tmp/jrdb_all
"""
from __future__ import annotations

import argparse

import numpy as np

from src.simulation._order_model import load_races
from src.simulation._order_model import fit_signal_coef as fit_coef
from src.simulation._order_model import trifecta_nll as _tri_nll


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--n-boot", type=int, default=1000)
    ap.add_argument("--with-tyb", action="store_true",
                    help="TYB直前(パドック指数/オッズ指数)も order signal に加える")
    args = ap.parse_args()

    races, signals = load_races(args.jrdb_dir, with_tyb=args.with_tyb)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    print(f"完全順序レース: {len(races):,}（train {len(tr):,} / test {len(te):,}）"
          f"  signals={signals}")

    coef = fit_coef(tr, signals)
    print(f"train最適係数: { {k: round(v, 2) for k, v in coef.items()} }")

    base = np.array([_tri_nll(r, None) for r in te])
    chal = np.array([_tri_nll(r, coef) for r in te])
    d = chal - base
    rng = np.random.default_rng(0)
    boot = np.array([d[rng.integers(0, len(d), len(d))].mean() for _ in range(args.n_boot)])
    ci = (float(np.percentile(boot, 2.5)), float(np.percentile(boot, 97.5)))
    print("\n== Stage A: 三連単順序 VOI（位置特化・市場=単勝Harville帰無）==")
    print(f"  baseline NLL={base.mean():.4f} → JRDB込み={chal.mean():.4f}"
          f"  ΔNLL={d.mean():+.5f} CI95=({ci[0]:+.5f},{ci[1]:+.5f})")
    print(f"  → {'有意改善（JRDBは連系順序に情報を持つ）' if ci[1] < 0 else '有意でない（CI が0跨ぎ）'}")

    # placebo: signal をレース内シャッフル → 改善が消えれば本物
    te_p = []
    for r in te:
        keys = list(r["sig"])
        vals = [r["sig"][u] for u in keys]
        rng.shuffle(vals)
        rp = dict(r)
        rp["sig"] = dict(zip(keys, vals, strict=False))
        te_p.append(rp)
    dp = np.array([_tri_nll(r, coef) for r in te_p]) - base
    print(f"  placebo(signalシャッフル) ΔNLL={dp.mean():+.5f}"
          f"  → {'帰無OK（改善消失＝本物）' if dp.mean() > -0.001 else 'placeboでも改善（要警戒）'}")

    print("\n注意: ΔNLL は較正改善であって ROI ではない。三連単控除(約27.5%)を超えるかは")
    print("Stage B（return_tables の実配当で ROI 検証・ユーザー環境）が必要。小さな較正改善が")
    print("控除を超える保証は無い（超えるとすれば特定条件のサブセット）。")


if __name__ == "__main__":
    main()
