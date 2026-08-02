"""H3 馬別・発走前適性の strictly-prior 生成コア（リーク安全の基盤・純関数）。

H3 は P2 とは別の**新規独立仮説**。この基盤は「現レースより前の走歴だけ」で馬別特徴を作るための
リーク安全プリミティブを提供する。既存 `_horse_features.add_*_stats` は horse_id 結合のみで
上流の「horse_results は当該レース日より前のみ」invariant に依存するが、H3 は invariant を信用せず
**明示的な日付ガード**（`strictly_prior_runs`）を通す（belt-and-suspenders）。

リーク防止の要（機械テスト対象）:
  - source_date < target_date（**厳密**less・同日は除外。馬は1日1走なので自己同日は無い）
  - target race 自身を集計に含めない（race_id 一致を除外）
  - 現レースの実現 pace/lap/着順/払戻など**レース後確定値は参照しない**（過去走の事実のみ使う）
  - JRA filter は特徴生成前に適用（呼び出し側の責務・ここは中立）

**特徴定義は監査(source contract/coverage)の後に freeze する**。本モジュールの aptitude 系は候補実装で
あり、audit_h3_source_contract の出力（発走前取得可否・coverage開始年・実列名）を見てから固定する。
"""
from __future__ import annotations

from typing import Mapping, Optional

# ペース状態 z の正準ラベル（SED race_pace / PACE 文字列いずれからも同じ語彙へ寄せる）
PACE_FAST = "fast"      # ハイ（前傾・前半速い）
PACE_NORMAL = "normal"  # 平均
PACE_SLOW = "slow"      # スロー
PACE_STATES = (PACE_FAST, PACE_NORMAL, PACE_SLOW)


def strictly_prior_runs(
    runs,
    target_date,
    *,
    target_race_id: Optional[object] = None,
    date_col: str = "date",
    race_id_col: str = "race_id",
):
    """1頭の過去走から「現レースより厳密に前」の走だけ返す（リーク安全の中核）。

    - `runs[date_col] < target_date`（**厳密 less**・同日除外＝同日後続からの逆流を構造的に遮断）。
    - `target_race_id` が与えられ race_id 列があれば、その race を除外（target 自身を集計しない）。
    NaT な日付は「順序不明＝安全側で除外」。純関数（入力を変更しない）。
    """
    import pandas as pd
    if runs is None or len(runs) == 0:
        return runs
    if date_col not in runs.columns:
        raise KeyError(f"strictly_prior_runs: date_col '{date_col}' が無い")
    d = pd.to_datetime(runs[date_col], errors="coerce")
    td = pd.to_datetime(target_date)
    mask = d.notna() & (d < td)          # 厳密 prior・NaT 除外
    if target_race_id is not None and race_id_col in runs.columns:
        mask &= runs[race_id_col].astype(str) != str(target_race_id)
    return runs[mask.to_numpy()]


def has_leak(runs, target_date, *, target_race_id=None, date_col="date", race_id_col="race_id") -> bool:
    """runs に「現レース以降 or target 自身」が混ざっていれば True（テスト/監査用の逆検出）。"""
    import pandas as pd
    if runs is None or len(runs) == 0:
        return False
    d = pd.to_datetime(runs[date_col], errors="coerce")
    td = pd.to_datetime(target_date)
    future = (d >= td).any()
    same = False
    if target_race_id is not None and race_id_col in runs.columns:
        same = (runs[race_id_col].astype(str) == str(target_race_id)).any()
    return bool(future or same)


# ---- 市場アンカーの過去走パフォーマンス残差（H3a の被説明量）------------------------------------

def market_anchored_perf(runs, *, pop_col="人気", rank_col="着順", n_col="頭数"):
    """過去走ごとの「市場期待をどれだけ上回ったか」= 人気相対 − 着順相対（正=市場超過）。

    perf = 人気/頭数 − 着順/頭数。すべてその過去走自身の確定値（過去の事実）で、現レースの
    outcome は使わない。人気/着順/頭数が欠ける行は NaN。純関数（Series を返す）。
    """
    import numpy as np
    import pandas as pd
    if runs is None or len(runs) == 0:
        return pd.Series(dtype=float)
    n = pd.to_numeric(runs.get(n_col), errors="coerce")
    pop = pd.to_numeric(runs.get(pop_col), errors="coerce")
    rank = pd.to_numeric(runs.get(rank_col), errors="coerce")
    with np.errstate(invalid="ignore", divide="ignore"):
        perf = (pop / n) - (rank / n)
    perf[(n <= 0) | n.isna()] = np.nan
    return perf


# ---- ペース状態 z の導出（過去走ごと）----------------------------------------------------------

def pace_state_from_balance(balance: object, *, hi: float = 0.6, lo: float = -0.6) -> Optional[str]:
    """ペースバランス(back−front 秒)→ z。balance>hi=fast(前傾/ハイ), <lo=slow, 中間=normal。

    front=前半3F, back=後半3F。前半速い(front<back)→balance>0→前傾→ハイ(fast)。閾値は事前固定既定。
    """
    try:
        b = float(balance)
    except (TypeError, ValueError):
        return None
    if b != b:  # NaN
        return None
    if b > hi:
        return PACE_FAST
    if b < lo:
        return PACE_SLOW
    return PACE_NORMAL


def pace_states_of_runs(runs, *, pace_col="ペース", sed_col="race_pace", hi=0.6, lo=-0.6):
    """過去走ごとの z（正準ラベル）。SED race_pace(H/M/S) を優先、無ければ PACE 文字列 balance。

    リーク注意: これは**各過去走の実測ペース**（その過去走の確定事実）であり、現レースの実現ペース
    ではない。純関数（Series を返す・不明は None）。
    """
    import pandas as pd
    from src.preprocessing._pace_state import parse_pace_string
    if runs is None or len(runs) == 0:
        return pd.Series(dtype=object)
    idx = runs.index
    if sed_col in runs.columns:
        m = {"H": PACE_FAST, "M": PACE_NORMAL, "S": PACE_SLOW}
        z = runs[sed_col].astype(str).str.strip().str.upper().map(m)
        if z.notna().any():
            return z
    if pace_col not in runs.columns:
        return pd.Series([None] * len(runs), index=idx, dtype=object)
    out = []
    for v in runs[pace_col]:
        fb = parse_pace_string(v)
        out.append(pace_state_from_balance(fb[1] - fb[0], hi=hi, lo=lo) if fb else None)
    return pd.Series(out, index=idx, dtype=object)


def shrink(mean_val: float, n: int, *, k: float, prior: float = 0.0) -> float:
    """縮約: n/(n+k)·mean + k/(n+k)·prior。少数標本を prior(既定0=中立)へ引く。純関数。"""
    if n <= 0:
        return prior
    w = n / (n + k)
    return w * mean_val + (1.0 - w) * prior


# ---- H3a 候補: 馬別ペース適性（監査後に freeze）------------------------------------------------

def pace_state_residuals(prior_runs, *, k: float = 5.0, **cols) -> dict:
    """各ペース状態 z での市場超過残差（縮約済）。{z: shrunk_resid, "n_z": {z:count}}。

    prior_runs は **strictly_prior_runs 済**を渡す前提（この関数は日付ガードしない）。
    perf=market_anchored_perf、z=pace_states_of_runs。純関数。
    """
    import numpy as np
    perf = market_anchored_perf(prior_runs,
                                pop_col=cols.get("pop_col", "人気"),
                                rank_col=cols.get("rank_col", "着順"),
                                n_col=cols.get("n_col", "頭数"))
    z = pace_states_of_runs(prior_runs,
                            pace_col=cols.get("pace_col", "ペース"),
                            sed_col=cols.get("sed_col", "race_pace"))
    out: dict = {}
    counts: dict = {}
    for state in PACE_STATES:
        sel = (z == state) & perf.notna()
        vals = perf[sel.to_numpy()] if hasattr(sel, "to_numpy") else perf[sel]
        n = int(len(vals))
        counts[state] = n
        out[state] = shrink(float(np.mean(vals)) if n else 0.0, n, k=k)
    out["n_z"] = counts
    return out


def pace_aptitude(prior_runs, pz_forecast: Mapping[str, float], *, k: float = 5.0, **cols) -> float:
    """H3a 候補: Σ_z P_r(z)·(縮約済 過去ペース状態別 市場超過)。発走前予想 P_r(z) で重み付け。

    pz_forecast は発走前に観測可能なペース予想分布（例: KYI pace_yosou 由来）。空/未取得なら NaN
    （安全な欠損）。prior_runs は strictly_prior_runs 済を渡すこと。純関数。
    """
    if not pz_forecast:
        return float("nan")
    resid = pace_state_residuals(prior_runs, k=k, **cols)
    tot = sum(float(pz_forecast.get(z, 0.0)) for z in PACE_STATES)
    if tot <= 0:
        return float("nan")
    return sum(float(pz_forecast.get(z, 0.0)) / tot * resid[z] for z in PACE_STATES)


# ---- H3b 候補: 馬別ラップ適性（監査後に freeze）------------------------------------------------

def lap_aptitude(prior_runs, *, baseline: Optional[Mapping] = None, k: float = 5.0,
                 nobori_col="上り", pace_col="ペース") -> dict:
    """H3b 候補: 過去走の終い(上り)速度・前後半差の馬別集約（縮約）。{late3f, front_back_diff, n}。

    - late3f: 過去走の上り3F 平均（小さいほど速い）。baseline があれば残差
      late3f − baseline["late3f"]（距離×馬場の母集団基準・**baseline も strictly-prior 前提**）。
    - front_back_diff: PACE の (back−front) 平均（正=前傾で好走してきた傾向）。
    現レースの実現ラップは使わない。prior_runs は strictly_prior_runs 済。純関数。
    """
    import numpy as np
    import pandas as pd
    from src.preprocessing._pace_state import parse_pace_string
    if prior_runs is None or len(prior_runs) == 0:
        return {"late3f": float("nan"), "front_back_diff": float("nan"), "n": 0}
    nob = pd.to_numeric(prior_runs.get(nobori_col), errors="coerce") if nobori_col in prior_runs.columns \
        else pd.Series(dtype=float)
    nob = nob.dropna()
    n = int(len(nob))
    late = float(np.mean(nob)) if n else float("nan")
    if baseline and "late3f" in baseline and late == late:
        late = late - float(baseline["late3f"])
    late = shrink(late, n, k=k) if late == late else float("nan")
    diffs = []
    if pace_col in prior_runs.columns:
        for v in prior_runs[pace_col]:
            fb = parse_pace_string(v)
            if fb:
                diffs.append(fb[1] - fb[0])
    fbd = shrink(float(np.mean(diffs)), len(diffs), k=k) if diffs else float("nan")
    return {"late3f": late, "front_back_diff": fbd, "n": n}
