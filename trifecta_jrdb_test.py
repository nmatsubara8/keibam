"""三連単×JRDB展開指数 VOI テスト — 連系の順序に JRDB の着順予測が edge を持つか。

背景（本セッションの確定台帳）: 公開データ・JRDB・オッズ変動の全経路で、最終JRA**単勝**
市場は効率的＝edge無しが確定した。残る未探索は「単勝以外＝連系プール」。仮説:
**群衆は単勝(P1着)を効率的に価格づけするが、三連単の順序(2/3着)には非効率が残り、JRDBの
展開予想（ゴール順位/位置指数）がそれを捉える。**

機構: 位置特化トリフェクタ（_harville.prob_trifecta_place_strength）。
  1着=市場単勝そのまま π=softmax(log q_win)。
  2/3着=JRDB調整の place 強度 σ=τ=softmax(log q_win + Σ coef_c·z(JRDB_c))。
  coef≡0 で素の Harville に退化（帰無）。coef は train の trifecta NLL 最小で fit。

判定（2段）:
- Stage A（本スクリプト・JRDB データのみで可）: 実着順(SED chakujun)の trifecta listwise NLL を
  baseline(Harville from 単勝) と比較。OOS＋Bootstrap CI＋placebo(signalシャッフル)。
  **確認済み(2025-26中央4,526R): ΔNLL−0.011 CI上端<0 有意 / placebo で改善消失。**
- Stage B（要 return_tables＝連系払戻・ユーザー環境）: 実際の三連単配当で ROI を測り、
  控除(約27.5%)を超えるか。NLL改善(較正)が ROI に化けるかは別問題（本スクリプトは
  --payoffs 指定時のみ ROI 節を実行）。

実行: python trifecta_jrdb_test.py --jrdb-dir /tmp/jrdb_all
"""
from __future__ import annotations

import argparse
import glob

import numpy as np
import pandas as pd

from src.jrdb._parser import parse
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.policies._market_residual import market_probs

# 2/3着の順序に効く JRDB 展開予想（着順そのものを予測する指数）
ORDER_SIGNALS = ("goal_juni", "ichi_idx")
_CENTRAL = {f"{i:02d}" for i in range(1, 11)}


def load_races(jrdb_dir: str, central_only: bool = True) -> list[dict]:
    """SED(着順+確定単勝)×KYI(展開指数) → 完全順序レース列（q/top3/signals）。"""
    sed = pd.concat([parse(f, "SED")[["race_id", "umaban", "kakutei_tansho", "chakujun"]]
                     for f in sorted(glob.glob(f"{jrdb_dir}/SED*.txt"))], ignore_index=True)
    kcols = ["race_id", "umaban", *ORDER_SIGNALS]
    kyi = pd.concat([parse(f, "KYI")[kcols]
                     for f in sorted(glob.glob(f"{jrdb_dir}/KYI*.txt"))], ignore_index=True)
    m = sed.merge(kyi, on=["race_id", "umaban"], how="inner").dropna(
        subset=["kakutei_tansho", "chakujun"])
    m = m[m["kakutei_tansho"] > 1.0]
    if central_only:
        m = m[m["race_id"].astype(str).str[4:6].isin(_CENTRAL)]
    races = []
    for rid, g in m.groupby(m["race_id"].astype(str)):
        g = g.dropna(subset=["chakujun"])
        if len(g) < 6:
            continue
        q = market_probs({int(u): float(o) for u, o in
                          zip(g["umaban"], g["kakutei_tansho"], strict=False)})
        if len(q) < 6:
            continue
        top3 = [int(x) for x in g.sort_values("chakujun")["umaban"].head(3)]
        if len(set(top3)) < 3 or any(t not in q for t in top3):
            continue
        sig: dict[int, dict[str, float]] = {}
        for c in ORDER_SIGNALS:
            v = pd.to_numeric(g[c], errors="coerce")
            z = (v - v.mean()) / (v.std() + 1e-6)
            for u, zz in zip(g["umaban"], z, strict=False):
                sig.setdefault(int(u), {})[c] = float(zz) if pd.notna(zz) else 0.0
        races.append({"rid": str(rid), "q": q, "top3": tuple(top3), "sig": sig})
    races.sort(key=lambda r: r["rid"])
    return races


def _place_probs(q, sig, coef):
    """JRDB 調整の place 強度 softmax（coef≡0 で q に一致＝帰無）。"""
    if not coef:
        return q
    s = {u: np.log(max(q[u], 1e-9)) + sum(coef[c] * sig.get(u, {}).get(c, 0.0) for c in coef)
         for u in q}
    mx = max(s.values())
    ex = {u: np.exp(v - mx) for u, v in s.items()}
    z = sum(ex.values())
    return {u: v / z for u, v in ex.items()}


def _tri_nll(r, coef):
    if not coef:
        p = prob_trifecta(r["q"], *r["top3"])
    else:
        p = prob_trifecta_place_strength(r["q"], _place_probs(r["q"], r["sig"], coef), *r["top3"])
    return -np.log(max(p, 1e-12))


def fit_coef(train, grid=np.arange(-0.3, 0.31, 0.1)):
    """train の trifecta NLL 最小の係数（ORDER_SIGNALS の2次元グリッド）。"""
    best = None
    for cg in grid:
        for ci in grid:
            coef = {ORDER_SIGNALS[0]: float(cg), ORDER_SIGNALS[1]: float(ci)}
            n = float(np.mean([_tri_nll(r, coef) for r in train]))
            if best is None or n < best[0]:
                best = (n, coef)
    return best[1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--jrdb-dir", default="/tmp/jrdb_all")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--n-boot", type=int, default=1000)
    args = ap.parse_args()

    races = load_races(args.jrdb_dir)
    n_tr = int(len(races) * args.train_frac)
    tr, te = races[:n_tr], races[n_tr:]
    print(f"完全順序レース: {len(races):,}（train {len(tr):,} / test {len(te):,}）")

    coef = fit_coef(tr)
    print(f"train最適係数: {{'goal_juni': {coef['goal_juni']:+.2f}, 'ichi_idx': {coef['ichi_idx']:+.2f}}}")

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
