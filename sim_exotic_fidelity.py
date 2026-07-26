"""連系(馬連・3連複)の依存構造 忠実度検証: 膝較正済み 2D 物理が着順の**同時分布**を再現するか。

sim_fidelity.py が測るのは周辺（各馬の勝率・序盤位置）だが、連系(exotics)の本質は
**joint ≠ 周辺の積**＝どの馬が一緒に来るかの共起依存。ここを実測と比べる。

物理レバー(脚質×展開)に直結する2軸で、実測(RANK 由来)と sim(top3)の双方から同じ統計を作る:
  (A) 馬連 同脚質リフト  L = P(2着が1着と同脚質) / 独立時の期待値（>1 で同脚質が束で来る依存）
  (B) 1着→2着 脚質遷移行列 P(2着脚質 | 1着脚質)（対角優勢＝同脚質共起）
  (C) 3連複 top3 の同脚質ペア数(0..3)  実 / sim / 順列ヌル
  (D) 枠順の共起: 1-2着の枠距離  実 / sim / 順列ヌル（隣枠が一緒に来る依存）

規律: これは**物理忠実度**の確認。依存構造が実測に一致しても市場エッジとは無関係
（§10 の echo0.989・ΔR²≤0 は動かない）。連系オッズへの優位性を主張するものではない。

実行例: python sim_exotic_fidelity.py --engine 2d --calibrated --limit 6000 --n-sim 400 --max-year 2021
"""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def _load_calibration(cfg_cls, fname, use_calibrated, default_absig):
    """models/<fname> の best_params を cfg クラスの有効フィールドだけ適用。ability_sigma は分離。"""
    if not use_calibrated:
        return {}, default_absig
    cal_path = Path(__file__).resolve().parent / "models" / fname
    if not cal_path.exists():
        print(f"[calibrated] {cal_path} が無い。calibrate_sim.py を先に実行。既定で続行。")
        return {}, default_absig
    raw = dict(json.loads(cal_path.read_text()).get("best_params", {}))
    absig = raw.pop("ability_sigma", default_absig)
    valid = {f.name for f in dataclasses.fields(cfg_cls)}
    cal = {k: v for k, v in raw.items() if k in valid}
    print(f"[calibrated] {cal_path.name}: " + ", ".join(f"{k}={v:.4f}" for k, v in cal.items())
          + f", ability_sigma={absig:.4f}")
    return cal, absig


def main():
    ap = argparse.ArgumentParser(description="連系(馬連・3連複)依存構造の忠実度検証（同時分布 vs 実測）")
    ap.add_argument("--limit", type=int, default=6000)
    ap.add_argument("--max-year", type=int, default=None)
    ap.add_argument("--n-sim", type=int, default=400)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--dt", type=float, default=1.0)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--engine", choices=["2d"], default="2d",
                    help="連系依存は 2d(膝較正済みの正統エンジン)で測る。")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--calibrated", action="store_true",
                    help="models/sim_calibration_2d.json の較正物理を適用して測る。")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race_2d import SimConfig2D, monte_carlo_2d
    from src.simulation._sim_params import field_from_featured

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order_ids = list(date.index)
    if args.max_year:
        order_ids = [r for r in order_ids if str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year]
    if args.limit and len(order_ids) > args.limit:
        order_ids = order_ids[-args.limit:]
    featured = featured.loc[order_ids]

    steps = max(1, round(args.T / args.dt))
    cal_params, eff_absig = _load_calibration(SimConfig2D, "sim_calibration_2d.json",
                                              args.calibrated, args.ability_sigma)
    cfg = SimConfig2D(T=steps, dt=args.dt, **cal_params)
    print(f"[engine] {args.engine} / dt={args.dt} / 実ステップ数={steps} / n_sim={args.n_sim}")
    rng = np.random.default_rng(args.seed)

    NS = 3  # 脚質数（front/stalker/closer）
    # 集計器
    real_same12, sim_same12, base_same = [], [], []     # (A) 馬連 同脚質
    M_real = np.zeros((NS, NS))
    M_sim = np.zeros((NS, NS))                          # (B) 1着→2着 脚質遷移
    real_trio, sim_trio, null_trio = [], [], []         # (C) 3連複 同脚質ペア数
    real_gap12, sim_gap12, null_gap12 = [], [], []      # (D) 1-2着 枠距離
    n_races = 0

    def _same_pairs(styles3):
        """3頭の脚質配列 → 同脚質の無順序ペア数(0..3)。"""
        a, b, c = styles3
        return int(a == b) + int(a == c) + int(b == c)

    for rid in order_ids:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        nH = len(rd)
        if nH < 5:
            continue
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        if not np.isfinite(rank).all() or len(np.unique(rank)) < nH:
            continue
        field = field_from_featured(rd, ability_spread=args.ability_spread)
        styles = np.asarray(field.style, dtype=int)
        if styles.min() < 0 or styles.max() >= NS:
            continue

        # 実測の着順（1着,2着,3着 の馬 index）
        real_order = np.argsort(rank, kind="stable")
        r0, r1, r2 = real_order[0], real_order[1], real_order[2]

        # 脚質頻度ベースライン（独立時に 2着が1着と同脚質になる期待値）
        counts = np.bincount(styles, minlength=NS)
        win_style = styles[r0]
        base = (counts[win_style] - 1) / (nH - 1)
        base_same.append(base)

        # (A)(B) 馬連 同脚質 & 遷移
        real_same12.append(float(styles[r1] == styles[r0]))
        M_real[win_style, styles[r1]] += 1.0

        # (C) 3連複 同脚質ペア数（実）と順列ヌル
        real_trio.append(_same_pairs((styles[r0], styles[r1], styles[r2])))
        # null: ランダム3頭の期待同脚質ペア数 = 3 * P(無順序2頭が同脚質)
        p_same_pair = float((counts * (counts - 1)).sum()) / (nH * (nH - 1))
        null_trio.append(3.0 * p_same_pair)

        # (D) 枠距離（レース内 0..1 正規化）
        gate = np.asarray(field.gate, dtype=float)
        gspan = float(gate.max() - gate.min())
        if gspan > 1e-9:
            g = (gate - gate.min()) / gspan
            real_gap12.append(abs(g[r0] - g[r1]))
            # null: ランダム2頭の期待枠距離
            gm = g[:, None] - g[None, :]
            iu = np.triu_indices(nH, 1)
            null_gap12.append(float(np.abs(gm[iu]).mean()))
        else:
            g = None

        # ── sim ──
        out = monte_carlo_2d(field, n_sim=args.n_sim, cfg=cfg, seed=int(rng.integers(1 << 30)),
                             ability_sigma=eff_absig, track_exotics=True)
        top3 = out["top3"]                       # (n_sim, 3) 馬 index
        s0 = styles[top3[:, 0]]
        s1 = styles[top3[:, 1]]
        s2 = styles[top3[:, 2]]
        sim_same12.append(float(np.mean(s1 == s0)))
        # 遷移行列（sim）: 1着脚質×2着脚質を確率で加算
        for a in range(NS):
            for b in range(NS):
                M_sim[a, b] += float(np.mean((s0 == a) & (s1 == b)))
        # 3連複 同脚質ペア数（sim 平均）
        same = (s0 == s1).astype(float) + (s0 == s2).astype(float) + (s1 == s2).astype(float)
        sim_trio.append(float(same.mean()))
        if g is not None:
            sim_gap12.append(float(np.mean(np.abs(g[top3[:, 0]] - g[top3[:, 1]]))))
        n_races += 1

    if n_races == 0:
        print("有効レースが0（頭数・着順・脚質の条件を満たすレースなし）")
        return

    # 集約
    L_real = float(np.mean(real_same12) / max(np.mean(base_same), 1e-9))
    L_sim = float(np.mean(sim_same12) / max(np.mean(base_same), 1e-9))
    Rr = M_real / M_real.sum(axis=1, keepdims=True).clip(1e-9)     # 行正規化 P(2着|1着)
    Rs = M_sim / M_sim.sum(axis=1, keepdims=True).clip(1e-9)
    frob = float(np.sqrt(((Rr - Rs) ** 2).sum()))

    names = ["逃先", "差", "追"]
    print("=" * 72)
    print(f"連系(馬連・3連複) 依存構造 忠実度 / {n_races:,}レース / n_sim={args.n_sim}")
    print("-" * 72)
    print("(A) 馬連 同脚質リフト  L = P(2着が1着と同脚質)/独立期待値  （1.0=依存なし・>1=同脚質が束で来る）")
    print(f"     実測 L={L_real:+.3f}   sim L={L_sim:+.3f}   （生比率 実={np.mean(real_same12):.3f} "
          f"sim={np.mean(sim_same12):.3f} / 独立期待={np.mean(base_same):.3f}）")
    print(f"     → |L_sim−L_real|={abs(L_sim - L_real):.3f}。同符号・同オーダーなら共起依存を物理再現。")
    print("-" * 72)
    print("(B) 1着→2着 脚質遷移 P(2着脚質|1着脚質)   行=1着脚質 / 列=2着脚質")
    print("      " + "".join(f"{'実 '+n:>8}" for n in names) + "   |" + "".join(f"{'sim '+n:>8}" for n in names))
    for a in range(NS):
        row = "".join(f"{Rr[a, b]:>8.3f}" for b in range(NS)) + "   |" + "".join(f"{Rs[a, b]:>8.3f}" for b in range(NS))
        print(f"  {names[a]:<3}" + row)
    print(f"     → 行列 Frobenius 距離 = {frob:.3f}（0 で完全一致）。対角が実/sim とも高いほど同脚質共起。")
    print("-" * 72)
    print("(C) 3連複 top3 同脚質ペア数(0..3)   実 / sim / 順列ヌル")
    print(f"     実測={np.mean(real_trio):.3f}   sim={np.mean(sim_trio):.3f}   ヌル(独立)={np.mean(null_trio):.3f}")
    trio_gap = abs(np.mean(real_trio) - np.mean(sim_trio))
    print(f"     → 実・sim がともにヌルを上回れば top3 に同脚質が集まる依存。実−sim の差={trio_gap:.3f}")
    if real_gap12:
        print("-" * 72)
        print("(D) 1-2着 枠距離(0..1)   実 / sim / 順列ヌル   （ヌル未満＝隣枠が一緒に来る依存）")
        print(f"     実測={np.mean(real_gap12):.3f}   sim={np.mean(sim_gap12):.3f}   ヌル={np.mean(null_gap12):.3f}")
    print("-" * 72)
    print("解釈: 実測と sim が (A)(C) でともにヌル/独立を同じ向きに外し、(B)の遷移行列が近ければ、")
    print("膝較正の物理は『周辺だけでなく着順の同時分布（連系の依存構造）』まで再現できている。")
    print("※これは物理忠実度の確認であり、連系オッズへの優位性(市場エッジ)とは無関係。")
    print("=" * 72)


if __name__ == "__main__":
    main()
