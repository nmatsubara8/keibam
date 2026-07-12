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

# 距離帯（course_len[m] 下限, ラベル）。上限は次の帯の下限。
BANDS = [
    (0, "スプリント ≤1400"),
    (1401, "マイル 1401–1800"),
    (1801, "中距離 1801–2200"),
    (2201, "長距離 ≥2201"),
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

    has_pm = "pace_median" in featured.columns
    back_col = "pace_median" if has_pm else "leg_type_binary"
    if back_col not in featured.columns:
        print("backness 列（pace_median / leg_type_binary）が featured に無い。rebuild-featured が必要。")
        return
    print(f"backness = {back_col}（0=先行 … 1=追込, {'連続' if has_pm else '二値'}）")

    date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = [r for r in date.index
             if (not args.max_year or (str(r)[:4].isdigit() and int(str(r)[:4]) <= args.max_year))
             and np.isfinite(pace.get(str(r), np.nan))]
    if args.limit and len(order) > args.limit:
        order = order[-args.limit:]

    # レース単位で (band, pace_diff, backness[], rank_norm[]) を集める（ブートストラップ用）
    races: dict[str, list] = {lab: [] for _, lab in BANDS}
    for rid in order:
        rd = featured.loc[[rid]] if not isinstance(featured.loc[rid], pd.DataFrame) else featured.loc[rid]
        nH = len(rd)
        if nH < 5:
            continue
        rank = pd.to_numeric(rd[ResultsCols.RANK], errors="coerce").to_numpy()
        b = pd.to_numeric(rd[back_col], errors="coerce").to_numpy()
        cl = pd.to_numeric(rd["course_len"], errors="coerce")
        if not np.isfinite(rank).all() or cl.isna().all():
            continue
        m = np.isfinite(b)
        if int(m.sum()) < 4:
            continue
        rn = (rank - 1) / max(nH - 1, 1)
        band = _band(float(cl.iloc[0]))
        races[band].append((pace[str(rid)], b[m], rn[m]))

    def _signal(race_list, thr):
        """race_list を前傾/後傾に分け signal=corr_後傾−corr_前傾 を返す（rows をプール）。"""
        bl, rl, bh, rh = [], [], [], []
        for pdiff, b, rn in race_list:
            if pdiff >= thr:          # 前傾（hi）
                bh.append(b); rh.append(rn)
            else:                     # 後傾（lo）
                bl.append(b); rl.append(rn)
        if not bh or not bl:
            return float("nan"), float("nan"), float("nan")
        c_hi = spearman(np.concatenate(bh), np.concatenate(rh))
        c_lo = spearman(np.concatenate(bl), np.concatenate(rl))
        sig = (c_lo - c_hi) if (np.isfinite(c_hi) and np.isfinite(c_lo)) else float("nan")
        return sig, c_lo, c_hi

    rng = np.random.default_rng(args.seed)
    print("=" * 78)
    print(f"距離×ペース層別『前傾→差し有利』 / {len(order):,}レース / bootstrap={args.bootstrap}")
    print("signal = corr(backness,着順)_後傾 − _前傾 。正＝前傾で差しが相対的に上位＝現象あり")
    print("-" * 78)
    print(f"{'距離帯':<20}{'レース数':>8}{'signal':>9}{'  95%CI':>18}   判定")
    any_sig = False
    for _, lab in BANDS:
        rl = races[lab]
        n = len(rl)
        if n < 200:
            print(f"{lab:<20}{n:>8}     （少数のためスキップ）")
            continue
        # 帯内の pace_diff 中央値で前傾/後傾を分割（閾値は全体で固定）
        thr = float(np.median([p for p, _, _ in rl]))
        sig, c_lo, c_hi = _signal(rl, thr)
        # レース単位ブートストラップ
        boot = np.empty(args.bootstrap)
        idx = np.arange(n)
        for k in range(args.bootstrap):
            samp = [rl[i] for i in rng.choice(idx, size=n, replace=True)]
            boot[k] = _signal(samp, thr)[0]
        boot = boot[np.isfinite(boot)]
        lo95, hi95 = (np.percentile(boot, [2.5, 97.5]) if len(boot) > 10 else (np.nan, np.nan))
        sig_flag = np.isfinite(lo95) and (lo95 > 0)
        any_sig = any_sig or sig_flag
        verdict = "★有意に正（差し有利あり）" if sig_flag else ("正だが0跨ぎ" if sig > 0 else "無/逆")
        print(f"{lab:<20}{n:>8}{sig:>+9.3f}   [{lo95:+.3f}, {hi95:+.3f}]   {verdict}")
    print("-" * 78)
    if any_sig:
        print("→ 一部距離帯で『前傾→差し有利』が実測で有意。その条件で展開機構を入れた sim を作る価値あり。")
    else:
        print("→ どの距離帯でも signal は0と区別できない。追う現象が実測で確認できず、")
        print("   薄い物理で再現する対象が存在しない＝sim による展開再現の路線は打ち止めが正当。")
    print("=" * 78)


if __name__ == "__main__":
    main()
