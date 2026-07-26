"""標的の実測: 「前傾ペースで差し脚質の相対着順が改善するか」を距離帯で層別測定（純実測・sim不要）。

物理 sim を足す前に、追う現象が実データに存在するかを確定させる。各レースを実ペース
(pace_diff = 上がり3F − 前半3F、正＝前傾) で前傾/後傾に分け、後方脚質(backness大)ほど
相対着順が前(小)に来るかを群ごとに測る:

  corr(backness, rank_norm)  … 負ほど「後方脚質が上位」
  signal = corr_後傾 − corr_前傾  … 正＝前傾のとき差しが相対的に上位＝『前傾→差し有利』

これを距離帯（スプリント/マイル/中距離/長距離）で層別し、**レース単位のブートストラップ**で
95%区間を付けて有意性を見る（行はレース内で相関するのでレース単位で再標本する）。

判定:
- どの距離帯でも signal の95%区間が0を跨ぐ → 追う現象が実測で確認できない。物理路線は打ち止めが正当。
- 長距離帯などで signal>0 が有意 → その条件で展開機構を入れた sim を作る価値が確定。

前提: race_pace.pkl（import_archive_laptime.py）と featured_data。
実行例: python pace_target.py --limit 80000 --max-year 2021 --bootstrap 400
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 距離帯（course_len は 100m バケット単位＝1600m→16。src/preprocessing で //100 済み）。
# (下限[バケット], ラベル)。上限は次帯の下限。
BANDS = [
    (0, "スプリント ≤1400m"),
    (15, "マイル 1500–1800m"),
    (19, "中距離 1900–2200m"),
    (23, "長距離 ≥2300m"),
]


def _band(course_len: float) -> str:
    label = BANDS[0][1]
    for lo, lab in BANDS:
        if course_len >= lo:
            label = lab
    return label


def main():
    ap = argparse.ArgumentParser(description="距離×ペース層別の『前傾→差し有利』実測")
    ap.add_argument("--limit", type=int, default=80000)
    ap.add_argument("--max-year", type=int, default=2021)
    ap.add_argument("--bootstrap", type=int, default=400, help="レース単位ブートストラップ回数")
    ap.add_argument("--seed", type=int, default=0)
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.simulation._fidelity import spearman

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

    # backness 候補列（連続 pace_median 優先、二値 leg_type_binary 予備）。
    cols = [c for c in ("pace_median", "leg_type_binary") if c in featured.columns]
    if not cols:
        print("backness 列（pace_median / leg_type_binary）が featured に無い。rebuild-featured が必要。")
        return

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = [r for r in date.index
             if (not args.max_year or (str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year))
             and np.isfinite(pace.get(str(r), np.nan))]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]

    # レース単位で (band, pace_diff, {col: backness[]}, rank_norm[]) を集める
    raw: dict[str, list] = {lab: [] for _, lab in BANDS}
    for rid in order:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        nH = len(rd)
        if nH < 5:
            continue
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        cl = pd.to_numeric(rd["course_len"], errors="coerce")
        if not np.isfinite(rank).all() or cl.isna().all():
            continue
        bv = {c: pd.to_numeric(rd[c], errors="coerce").to_numpy() for c in cols}
        if max(int(np.isfinite(v).sum()) for v in bv.values()) < 4:
            continue
        rn = (rank - 1) / max(nH - 1, 1)
        cl_val = float(cl.iloc[0])
        cl_bucket = cl_val / 100.0 if cl_val > 100 else cl_val   # メートル記録なら100mバケットへ
        raw[_band(cl_bucket)].append((float(pace[str(rid)]), bv, rn))

    # backness 診断: 各候補列の pooled 分散を見て、変動する列を採用する。
    print("[backness 診断] （spearman は定数列で NaN になるため分散を確認）")
    best = None
    for c in cols:
        pooled = np.concatenate([bv[c] for lab in raw for _, bv, _ in raw[lab]]) if any(raw.values()) else np.array([])
        vf = pooled[np.isfinite(pooled)]
        std = float(vf.std()) if len(vf) else 0.0
        nuq = int(len(np.unique(np.round(vf, 4)))) if len(vf) else 0
        print(f"  {c}: finite={len(vf):,} unique={nuq} std={std:.4f}")
        if std > 1e-6 and best is None:
            best = c
    if best is None:
        print("→ どの backness 列も pooled 分散≈0（この期間の脚質特徴が全馬ほぼ同値＝縮退）。")
        print("  archive era(≤2021) の featured で 通過→first_corner→脚質 が再構築されていない疑い。")
        print("  rebuild-featured（通過保持修正版）後に脚質が変動する期間でのみ標的測定が可能。")
        return
    print(f"→ 採用 backness = {best}（0=先行 … 1=追込）")

    # 採用列で races を再構築（finite のみ、下流は (pdiff, b[], rn[])）
    races: dict[str, list] = {lab: [] for _, lab in BANDS}
    for lab in raw:
        for pdiff, bv, rn in raw[lab]:
            b = bv[best]
            m = np.isfinite(b)
            if int(m.sum()) >= 4:
                races[lab].append((pdiff, b[m], rn[m]))


    def _split_lohi(race_list, lo_thr, hi_thr):
        """pace_diff で下位=後傾(lo)/上位=前傾(hi) に3分位分割。閾値は帯全体で固定。"""
        bl, rl_, bh, rh = [], [], [], []
        for pdiff, b, rn in race_list:
            if pdiff <= lo_thr:
                bl.append(b); rl_.append(rn)
            elif pdiff >= hi_thr:
                bh.append(b); rh.append(rn)
        return bl, rl_, bh, rh

    def _signal(race_list, lo_thr, hi_thr):
        """signal = corr_後傾 − corr_前傾（正＝前傾で差しが相対的に上位）。件数も返す。"""
        bl, rl_, bh, rh = _split_lohi(race_list, lo_thr, hi_thr)
        n_lo = sum(len(x) for x in bl); n_hi = sum(len(x) for x in bh)
        if not bh or not bl:
            return float("nan"), float("nan"), float("nan"), n_lo, n_hi
        c_hi = spearman(np.concatenate(bh), np.concatenate(rh))
        c_lo = spearman(np.concatenate(bl), np.concatenate(rl_))
        sig = (c_lo - c_hi) if (np.isfinite(c_hi) and np.isfinite(c_lo)) else float("nan")
        return sig, c_lo, c_hi, n_lo, n_hi

    # pace_diff 分布の診断（同値過多で分割が潰れていないか）
    allp = np.array([p for lab in races for p, _, _ in races[lab]], float)
    if len(allp):
        print(f"[pace_diff] n={len(allp):,} 一意値={len(np.unique(np.round(allp,3))):,} "
              f"min={allp.min():+.2f} median={np.median(allp):+.2f} max={allp.max():+.2f}")

    rng = np.random.default_rng(args.seed)
    print("=" * 78)
    print(f"距離×ペース層別『前傾→差し有利』 / {len(order):,}レース / bootstrap={args.bootstrap}")
    print("signal = corr(backness,着順)_後傾 − _前傾 。正＝前傾で差しが相対的に上位＝現象あり")
    print("（前傾=pace_diff上位1/3, 後傾=下位1/3 で対比。中間1/3は除外）")
    print("-" * 78)
    print(f"{'距離帯':<20}{'レース数':>7}{'後/前':>10}{'signal':>9}{'  95%CI':>17}   判定")
    any_sig = False
    for _, lab in BANDS:
        rl = races[lab]
        n = len(rl)
        if n < 200:
            print(f"{lab:<20}{n:>7}     （少数のためスキップ）")
            continue
        ps = np.array([p for p, _, _ in rl], float)
        lo_thr, hi_thr = np.percentile(ps, [33.3, 66.7])
        if not (hi_thr > lo_thr):   # 同値過多で3分位が潰れる場合
            print(f"{lab:<20}{n:>7}   pace_diff の分散不足（一意値が少なく前傾/後傾を分離できず）")
            continue
        sig, c_lo, c_hi, n_lo, n_hi = _signal(rl, lo_thr, hi_thr)
        boot = np.empty(args.bootstrap)
        idx = np.arange(n)
        for k in range(args.bootstrap):
            samp = [rl[i] for i in rng.choice(idx, size=n, replace=True)]
            boot[k] = _signal(samp, lo_thr, hi_thr)[0]
        boot = boot[np.isfinite(boot)]
        lo95, hi95 = (np.percentile(boot, [2.5, 97.5]) if len(boot) > 10 else (np.nan, np.nan))
        sig_flag = np.isfinite(lo95) and (lo95 > 0)
        any_sig = any_sig or sig_flag
        verdict = ("★有意に正（差し有利あり）" if sig_flag
                   else ("正だが0跨ぎ" if np.isfinite(sig) and sig > 0 else "無/逆"))
        print(f"{lab:<20}{n:>7}{f'{n_lo}/{n_hi}':>10}{sig:>+9.3f}"
              f"   [{lo95:+.3f},{hi95:+.3f}]   {verdict}")
    print("-" * 78)
    if any_sig:
        print("→ 一部距離帯で『前傾→差し有利』が実測で有意。その条件で展開機構を入れた sim を作る価値あり。")
    else:
        print("→ どの距離帯でも signal は0と区別できない。追う現象が実測で確認できず、")
        print("   薄い物理で再現する対象が存在しない＝sim による展開再現の路線は打ち止めが正当。")
    print("=" * 78)


if __name__ == "__main__":
    main()
