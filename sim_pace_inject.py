"""学習ペースを sim に注入し、忠実度が改善するかを A/B で測る（前進安全）。

背景: 素朴物理 sim はペース形が実と逆相関(−0.22)だった。原因は「先行多数→速い」の
符号違い。pace_forward.py で「隊列構成→ペース」は条件と独立に corr≈0.22 で学習可能と確認。
本ハーネスはその学習器(PacePredictor)を sim の pace_intensity に注入し、

  (1) ペース形 忠実度: corr(sim前傾度, 実前傾度)  — 注入で −→＋ へ動くか（※注入すれば
      上がるのは半ば自明なので主指標にしない。符号が反転するかだけ見る）
  (2) 展開機構 忠実度: 「前傾→後方脚質が相対的に前」signal を 実 / sim(baseline) / sim(注入)
      で比較。★ここが本命。ペース予測は"水準"だけを与え、誰が得するかは物理が決める。
      注入 signal が実測と同符号・近い大きさに寄れば、sim が展開×脚質を物理再現できた証拠。

学習は前半レース、評価は後半レースで、予測器は評価レースを一切見ない（前進安全）。
実行例: python sim_pace_inject.py --limit 12000 --n-sim 400 --max-year 2021
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    ap = argparse.ArgumentParser(description="学習ペース注入の忠実度 A/B（前進安全）")
    ap.add_argument("--limit", type=int, default=12000)
    ap.add_argument("--max-year", type=int, default=2021)
    ap.add_argument("--n-sim", type=int, default=400)
    ap.add_argument("--T", type=int, default=100)
    ap.add_argument("--ability-spread", type=float, default=0.20)
    ap.add_argument("--ability-sigma", type=float, default=0.35)
    ap.add_argument("--gain", type=float, default=0.25, help="pace_intensity の振れ幅(±)")
    ap.add_argument("--engine", choices=["timebox", "fixed"], default="fixed",
                    help="timebox=時間箱(旧)/fixed=固定距離(time-to-D・drafting・戦略分布)")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.simulation._agent_race import SimConfig, monte_carlo
    from src.simulation._agent_race_fixed import SimConfigFixed, monte_carlo_fixed
    from src.simulation._fidelity import pace_backness_signal, pace_shape_corr
    from src.simulation._pace_model import PacePredictor, features_to_row, pace_features
    from src.simulation._sim_params import field_from_featured

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return
    pace_path = Path(LocalPaths.RAW_DIR) / "race_pace.pkl"
    if not pace_path.exists():
        print(f"{pace_path} が無い。import_archive_laptime.py で作成してください。")
        return
    rp = pd.read_pickle(pace_path)
    pace = dict(zip(rp["race_id"].astype(str), pd.to_numeric(rp["pace_diff"], errors="coerce")))

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = [r for r in date.index
             if (not args.max_year or (str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year))
             and np.isfinite(pace.get(str(r), np.nan))]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]
    if len(order) < 200:
        print(f"レースが少なすぎます（{len(order)}）")
        return

    # 前半=予測器の学習、後半=sim 評価（予測器は評価レースを見ない＝前進安全）
    split = len(order) // 2
    fit_ids, eval_ids = order[:split], order[split:]
    print(f"ペース注入 A/B / 学習{len(fit_ids):,}レース → 評価{len(eval_ids):,}レース / n_sim={args.n_sim}")

    def _race_df(rid):
        r = featured.loc[rid]
        return r if isinstance(r, pd.DataFrame) else featured.loc[[rid]]

    # --- 予測器を前半で学習 ---
    Xf, yf = [], []
    for rid in fit_ids:
        rd = _race_df(rid)
        if len(rd) < 4:
            continue
        Xf.append(features_to_row(pace_features(rd)))
        yf.append(pace[str(rid)])
    predictor = PacePredictor(gain=args.gain).fit(np.array(Xf, float), np.array(yf, float))

    # --- エンジン別の sim ランナー（baseline/注入を同一 seed で回す） ---
    def _run(field, intensity, seed, D):
        if args.engine == "fixed":
            cfg = SimConfigFixed()
            return monte_carlo_fixed(field, D=D, n_sim=args.n_sim, cfg=cfg, seed=seed,
                                     ability_sigma=args.ability_sigma,
                                     pace_intensity=intensity, track_dynamics=True)
        cfg = SimConfig(T=args.T, pace_intensity=intensity)
        return monte_carlo(field, n_sim=args.n_sim, cfg=cfg, seed=seed,
                           ability_sigma=args.ability_sigma, track_dynamics=True)

    print(f"[engine] {args.engine}")
    rng = np.random.default_rng(args.seed)
    sp_base, sp_inj, real_pace_r = [], [], []
    back, rn_real, rn_base, rn_inj, real_pp, band_row = [], [], [], [], [], []
    intens = []

    def _band(clb):
        return ("スプリント≤1400" if clb <= 14 else "マイル1500-1800" if clb <= 18
                else "中距離1900-2200" if clb <= 22 else "長距離≥2300")

    for rid in eval_ids:
        rd = _race_df(rid)
        nH = len(rd)
        if nH < 4:
            continue
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        if not np.isfinite(rank).all():
            continue
        lt = (pd.to_numeric(rd["leg_type_binary"], errors="coerce").to_numpy()
              if "leg_type_binary" in rd.columns else np.full(nH, np.nan))
        field = field_from_featured(rd, ability_spread=args.ability_spread)

        it = predictor.predict_intensity(features_to_row(pace_features(rd)))
        intens.append(it)
        # D=固定距離(m)。course_len は 100m バケットなので ×100（メートル記録ならそのまま）
        cl = pd.to_numeric(rd["course_len"], errors="coerce")
        clv = float(cl.iloc[0]) if cl.notna().any() else 16.0
        D = clv * 100.0 if clv < 100 else clv

        sd = int(rng.integers(1 << 30))                 # 同一 seed で baseline/注入を比較
        sim_b = _run(field, 1.0, sd, D)
        sim_i = _run(field, it, sd, D)

        def _shape(s):
            e, l = s["early_speed"], s["late_speed"]
            return (e - l) / (e + l + 1e-9)

        rpv = pace[str(rid)]
        sp_base.append(_shape(sim_b)); sp_inj.append(_shape(sim_i)); real_pace_r.append(rpv)

        # 展開signal 用の馬単位行（実着順・脚質・実ペース）。sim側は着順を使わず、実の相対着順で
        # 「実データの展開機構」を測る基準にする。sim機構の寄与は別途 sim着順で測る。
        rnn = (rank - 1) / max(nH - 1, 1)
        b = lt.copy()
        nb = ~np.isfinite(b)
        if nb.any():
            b[nb] = field.style[nb] / 2.0
        # sim の相対着順（baseline/注入）: mean_rank を [0,1] 正規化
        mr_b = (sim_b["mean_rank"] - 1) / max(nH - 1, 1)
        mr_i = (sim_i["mean_rank"] - 1) / max(nH - 1, 1)
        clb = clv if clv < 100 else clv / 100.0
        lab = _band(clb)
        for i in range(nH):
            back.append(b[i]); real_pp.append(rpv); band_row.append(lab)
            rn_real.append(rnn[i]); rn_base.append(mr_b[i]); rn_inj.append(mr_i[i])

    print("=" * 72)
    c_base = pace_shape_corr(sp_base, real_pace_r)
    c_inj = pace_shape_corr(sp_inj, real_pace_r)
    print(f"(1) ペース形 忠実度  corr(sim前傾度, 実前傾度)")
    print(f"      baseline(内生のみ) = {c_base:+.3f}")
    print(f"      注入(学習ペース)   = {c_inj:+.3f}   [{len(sp_base):,}レース]")
    print(f"      → 符号が − から + へ反転すれば、素朴物理の逆相関を修正できた（水準の当て込み）。")
    print("-" * 72)

    # 展開機構: 実ペースで hi/lo 分割し、実 / sim(baseline) / sim(注入) の相対着順で signal を測る。
    # sim の signal が実測(基準)の向きに寄るかを見る。
    print("(2) 展開機構 忠実度（前傾→後方脚質が相対的に前, signal>0）  ★本命")
    sig_real = pace_backness_signal(back, rn_real, real_pp)
    sig_b = pace_backness_signal(back, rn_base, real_pp)
    sig_i = pace_backness_signal(back, rn_inj, real_pp)
    print(f"      実測(基準)   signal = {sig_real['signal']:+.3f}  "
          f"(hi={sig_real['corr_hi']:+.3f}, lo={sig_real['corr_lo']:+.3f})")
    print(f"      sim baseline signal = {sig_b['signal']:+.3f}  "
          f"(hi={sig_b['corr_hi']:+.3f}, lo={sig_b['corr_lo']:+.3f})")
    print(f"      sim 注入   signal = {sig_i['signal']:+.3f}  "
          f"(hi={sig_i['corr_hi']:+.3f}, lo={sig_i['corr_lo']:+.3f})")
    print(f"      平均 pace_intensity = {float(np.mean(intens)):.3f}  "
          f"(±{float(np.std(intens)):.3f}, 範囲 {min(intens):.3f}–{max(intens):.3f})")
    print("-" * 72)

    # (2b) 距離帯で層別（実測は中距離が最強。強い帯で sim が的を射るか）
    print("(2b) 距離帯別 signal（実測 / sim注入）  ← 実測は中距離+0.09 が最強")
    barr = np.array(band_row)
    back_a = np.array(back); rr_a = np.array(rn_real); ri_a = np.array(rn_inj); pp_a = np.array(real_pp)
    for lab in ("スプリント≤1400", "マイル1500-1800", "中距離1900-2200", "長距離≥2300"):
        m = barr == lab
        if int(m.sum()) < 200:
            print(f"      {lab:<16} n={int(m.sum()):>6}  （少数）"); continue
        sr = pace_backness_signal(back_a[m], rr_a[m], pp_a[m])
        si = pace_backness_signal(back_a[m], ri_a[m], pp_a[m])
        print(f"      {lab:<16} n={int(m.sum()):>6}  実測={sr['signal']:+.3f}  sim注入={si['signal']:+.3f}")
    print("-" * 72)
    print("解釈: (2)の注入 signal が baseline より正側に動けば、正しいペース水準を与えると")
    print("      sim が『前傾で差しが台頭』という展開×脚質の物理を再現できたことを意味する。")
    print("      これは忠実度(=物理再現)の改善であって、市場を越えるかは別問題。")
    print("=" * 72)


if __name__ == "__main__":
    main()
