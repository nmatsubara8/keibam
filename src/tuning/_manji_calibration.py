"""Model 2 / Layer A: 因子バケットの点数を「学習期間の回収率」から較正する。

卍さんの「回収率の高い条件に加点、低い条件に減点、信頼度の低い因子は排除、普遍的な
傾向のみ採用」を前進安全に形式化する。ここに渡す featured は**学習期間のスライスだけ**で、
未来（評価fold）は一切含めない。呼び出し側（外側 walk-forward）がその分離を保証する。

点数式:
    point(bucket) = clip( λ · (recovery − 1) · √n/(√n + c) , −clip, +clip )
- recovery = そのバケットの単勝フラット回収率 = mean(単勝オッズ × [着順==1])。
- √n/(√n+c) の収縮で小標本バケットを 0 に寄せる（ノイズ採掘の抑制＝「信頼度の低い因子は排除」）。
- n < min_n のバケットは 0（不採用）。

普遍性フィルタ（「明確・普遍的な傾向のみ」）:
- 学習期間を時系列 K 分割し、各バケットの符号 sign(recovery−1) を測る。
- 全期間の符号と一致するスライスの割合が min_agree 未満のバケットは 0 に落とす（不安定→排除）。
これにより「過去の一時期だけ効いた偶然の条件」を弾き、時間に頑健な因子だけを残す。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import NA, FACTORS


def _win_and_odds(featured: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    win = (pd.to_numeric(featured[ResultsCols.RANK], errors="coerce") == 1).astype(float)
    odds = pd.to_numeric(featured[ResultsCols.TANSHO_ODDS], errors="coerce")
    return win, odds


def bucket_recovery(featured: pd.DataFrame, factor: str) -> pd.DataFrame:
    """因子 factor のバケット別 回収率(単勝フラット) と件数 n を返す。

    recovery = mean(単勝オッズ × [着順==1])。index=bucket, columns=[recovery, n]。
    """
    win, odds = _win_and_odds(featured)
    bucket = pd.Series(FACTORS[factor](featured), index=featured.index).astype(object).fillna(NA)
    ret = (odds * win)
    df = pd.DataFrame({"bucket": bucket.to_numpy(), "ret": ret.to_numpy()})
    df = df[df["bucket"] != NA]
    df = df[np.isfinite(df["ret"])]
    if df.empty:
        return pd.DataFrame(columns=["recovery", "n"])
    g = df.groupby("bucket")["ret"]
    return pd.DataFrame({"recovery": g.mean(), "n": g.size()})


def _time_slices(featured: pd.DataFrame, k: int) -> list[pd.DataFrame]:
    """発走日順にレース単位で k 分割した featured スライスのリスト。"""
    if k <= 1:
        return [featured]
    race_date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(race_date.index)
    n = len(order)
    bounds = [round(i * n / k) for i in range(k + 1)]
    out = []
    for i in range(k):
        rids = order[bounds[i]:bounds[i + 1]]
        if rids:
            out.append(featured.loc[rids])
    return out


def calibrate_points(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    lam: float = 1.0,
    shrink_c: float = 20.0,
    clip: float = 2.0,
    min_n: int = 30,
    universality_slices: int = 3,
    min_agree: float = 0.7,
) -> dict[str, dict[str, float]]:
    """学習期間 featured から points[factor][bucket] を導出する（Layer A）。

    Returns
    -------
    {factor: {bucket: point}}  point は [−clip, +clip]。不採用バケットは省略（=0点扱い）。
    """
    factor_names = factor_names or list(FACTORS)
    slices = _time_slices(featured, universality_slices) if universality_slices > 1 else []
    points: dict[str, dict[str, float]] = {}

    for f in factor_names:
        rec = bucket_recovery(featured, f)
        if rec.empty:
            continue
        # 普遍性: 各バケットの符号がサブ期間で min_agree 以上一致するか
        agree: dict[str, float] = {}
        if slices:
            full_sign = np.sign(rec["recovery"] - 1.0)
            for b in rec.index:
                fs = full_sign.get(b, 0.0)
                if fs == 0.0:
                    agree[b] = 0.0
                    continue
                hits = tot = 0
                for sl in slices:
                    sr = bucket_recovery(sl, f)
                    if b in sr.index and sr.loc[b, "n"] >= max(5, min_n // 3):
                        tot += 1
                        if np.sign(sr.loc[b, "recovery"] - 1.0) == fs:
                            hits += 1
                agree[b] = (hits / tot) if tot else 0.0

        fmap: dict[str, float] = {}
        for b, row in rec.iterrows():
            n = float(row["n"])
            if n < min_n:
                continue
            if slices and agree.get(b, 0.0) < min_agree:
                continue  # 不安定バケット→排除
            shrink = np.sqrt(n) / (np.sqrt(n) + shrink_c)
            pt = lam * (float(row["recovery"]) - 1.0) * shrink
            pt = float(np.clip(pt, -clip, clip))
            if pt != 0.0:
                fmap[b] = pt
        if fmap:
            points[f] = fmap
    return points


def calibrate_factor_weights(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    valid_frac: float = 0.3,
    w_max: float = 2.0,
    min_side: int = 200,
    **cal_kwargs,
) -> dict[str, float]:
    """符号付き因子重み w_f を前進安全に推定する（過学習防止の二重取り回避）。

    学習窓を時系列で calib(前) / valid(後) に分割し:
      1. points を calib だけで較正（点の符号を決める）。
      2. valid で各因子の「加点馬 vs 減点馬」の実回収率差を w_f とする。
         w_f = mean(単勝回収 | point>0) − mean(単勝回収 | point<0)。
         方向が valid で保てば正、逆転すれば負、分離しなければ ~0。
    最後に max|w_f|=1 に正規化（相対重み）。点は別途 全学習窓で較正して使う。

    Returns: {factor: w_f}  （−1〜+1 目安、未推定は 0.0）
    """
    factor_names = factor_names or list(FACTORS)
    race_date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    order = list(race_date.index)
    cut = int(len(order) * (1.0 - valid_frac))
    calib_races, valid_races = order[:cut], order[cut:]
    if not calib_races or not valid_races:
        return {f: 0.0 for f in factor_names}
    calib = featured.loc[calib_races]
    valid = featured.loc[valid_races]

    points = calibrate_points(calib, factor_names, **cal_kwargs)
    from src.policies._manji_factors import buckets
    win, odds = _win_and_odds(valid)
    ret = (odds * win).to_numpy()
    bk = buckets(valid, factor_names)

    weights: dict[str, float] = {}
    for f in factor_names:
        pmap = points.get(f, {})
        if not pmap:
            weights[f] = 0.0
            continue
        pts = bk[f].map(lambda b, pmap=pmap: pmap.get(b, 0.0)).to_numpy(dtype=float)
        finite = np.isfinite(ret)
        pos = ret[(pts > 0) & finite]
        neg = ret[(pts < 0) & finite]
        if len(pos) < min_side or len(neg) < min_side:
            weights[f] = 0.0
            continue
        lift = float(pos.mean() - neg.mean())  # 符号付き（加点馬が実際に回収が高いか）
        weights[f] = float(np.clip(lift, -w_max, w_max))

    m = max((abs(v) for v in weights.values()), default=0.0)
    if m > 0:
        weights = {f: v / m for f, v in weights.items()}  # max|w|=1 に正規化
    return weights
