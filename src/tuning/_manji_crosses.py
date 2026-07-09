"""因子クロス（2因子の相互作用）を回収率ベースで自動選別する。

「単独では効かないが組合せで効く」因子を拾う。各ペア A×B の合成バケットを回収率較正し、
**加算予測からの残差**（＝純粋な相互作用）が大きく被覆の厚いクロスを上位に返す。

interaction(A×B) = Σ_bucket n_b·( cross_point_b − (point_A[b_A] + point_B[b_B]) )^2 / Σ n_b

残差が大＝そのクロスは「A と B の点の和」では説明できない情報を持つ＝相互作用が寄与。
選ばれたクロス名（"A*B"）は factor_series/buckets/calibrate が透過的に扱える。
"""
from __future__ import annotations

import itertools

from src.tuning._manji_calibration import bucket_recovery, calibrate_points


def screen_crosses(
    featured,
    singles: list[str],
    *,
    top_n: int = 20,
    min_coverage: int = 500,
    min_n: int = 50,
    max_pairs: int | None = None,
    **cal_kwargs,
) -> list[str]:
    """singles の全ペアからクロスを作り、相互作用スコア上位 top_n の "A*B" 名を返す。

    高速化のため screen では universality を無効化（後段の本較正で本フィルタを掛ける）。
    """
    kw = dict(cal_kwargs)
    kw["universality_slices"] = 1        # screen は速く
    kw["residualize"] = False            # 交互作用残差は screen 側で明示計算するため生の点が要る
    kw.setdefault("min_n", min_n)
    pts = calibrate_points(featured, singles, **kw)
    # 点が付いた（＝発火した）単独因子だけをクロス候補に
    live = [f for f in singles if pts.get(f)]

    pairs = list(itertools.combinations(live, 2))
    if max_pairs:
        pairs = pairs[:max_pairs]

    scored: list[tuple[str, float, float]] = []
    for f1, f2 in pairs:
        cross = f"{f1}*{f2}"
        rec = bucket_recovery(featured, cross)
        if rec.empty:
            continue
        cpts = calibrate_points(featured, [cross], **kw).get(cross, {})
        if not cpts:
            continue
        num = den = 0.0
        for b, row in rec.iterrows():
            if b not in cpts:
                continue
            parts = b.split("|")
            if len(parts) != 2:
                continue
            add = pts.get(f1, {}).get(parts[0], 0.0) + pts.get(f2, {}).get(parts[1], 0.0)
            resid = cpts[b] - add
            n = float(row["n"])
            num += n * resid * resid
            den += n
        if den >= min_coverage:
            scored.append((cross, num / den, den))

    scored.sort(key=lambda x: -x[1])
    return [c for c, _, _ in scored[:top_n]]
