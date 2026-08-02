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

# ============================================================================================
# FROZEN H3 SPEC（SED 主ソース・2026-08-02 続19・**gate A/B 通過後に最終 freeze**・性能前に固定）
# H3a（馬別ペース適性・1特徴）: 現レース pace_yosou → **fold内 calibrated** P(z_actual|pace_yosou)
#   （3×3 混同行列・Dirichlet α=1・学習期間のみ）を重みに、strictly-prior な SED 実測ペース状態
#   (race_pace) 別の馬別市場残差 μ_{h,z} を集約: x = Σ_z P_r(z)·μ_{h,z}。
#     市場残差 r = (人気 − 着順)/(N−1)（正=人気以上に好走・N=出走頭数）。ijo_kubun≠0(取消/除外/
#     中止/失格/降着)は除外。μ_{h,z}=shrink(mean r_z, n_z, λ=5)。
#   履歴なし→0・現レース予想欠測→0（そのレースでは市場を動かさない＝安全な中立）。
# H3b（馬別終い適性・1特徴）: strictly-prior な過去走の **race 内 上り(ato3f) percentile**
#   s = 0.5 − pct_asc(ato3f)（正=同走より速い終い）。距離/馬場/時計水準は同走比較で除去。
#   x = shrink(mean s, n, λ=5)。履歴なし→0。mae3f/前後半差/距離別/近走窓は**足さない**（別仮説 H4）。
# 3F は spec/物理域で欠測化（監査②で mae3f min=-89・ato3f max=984・idx -99.9 を検出）: JRDB fill/
#   空→NaN、物理域外(28.0-45.0秒＝[280,450] 0.1秒単位)→NaN。λ・域・残差式・被説明量は事前固定。
ALPHA_DIRICHLET = 1.0                   # calibration の Dirichlet 平滑（各セル）。事前固定。
H3_SHRINK_K = 5.0                       # 縮約強度 λ（n=λ で半重み）。事前固定。
THREE_F_MIN, THREE_F_MAX = 280.0, 450.0  # 3F 物理域（0.1秒単位＝28.0-45.0秒）。域外/0/空→欠測。
_IJO_OK = {"0", ""}                     # 異常区分 0=正常のみ完走扱い（≠0 は非完走で除外）


def clip_3f(values):
    """3F タイム(0.1秒単位)を物理域 [280,450] に限定し、域外/0/負/sentinel を NaN へ。純関数。"""
    import pandas as pd
    v = pd.to_numeric(pd.Series(values).reset_index(drop=True), errors="coerce")
    v = v.mask((v < THREE_F_MIN) | (v > THREE_F_MAX))     # 0/負/999等の JRDB fill もここで NaN
    return v.to_numpy(dtype=float)


def sed_pace_state(race_pace_series):
    """SED race_pace(H/M/S 実測) を正準 z へ（H→fast, M→normal, S→slow）。空/不明は None。純関数。"""
    import pandas as pd
    m = {"H": PACE_FAST, "M": PACE_NORMAL, "S": PACE_SLOW}
    return pd.Series(race_pace_series).astype(str).str.strip().str.upper().map(m)


def sed_market_perf(sed, *, race_col="race_id", ninki_col="kakutei_ninki",
                    chaku_col="chakujun", ijo_col="ijo_kubun"):
    """過去走ごとの市場超過残差 r=(人気−着順)/(N−1)（正=人気以上）。ijo_kubun≠0 と無効値は NaN。

    N=その race の SED 行数（出走頭数）。範囲 [−1,1]。取消/除外/中止/失格/降着（ijo≠0）と
    人気/着順が 1..N 外の行は除外。現レース outcome は使わない（各行は過去走の確定値）。純関数。
    """
    import numpy as np
    import pandas as pd
    n = sed.groupby(race_col)[race_col].transform("size").astype(float)
    ninki = pd.to_numeric(sed[ninki_col], errors="coerce")
    chaku = pd.to_numeric(sed[chaku_col], errors="coerce")
    with np.errstate(invalid="ignore", divide="ignore"):
        perf = (ninki - chaku) / (n - 1.0)
    bad = (n <= 1) | (ninki < 1) | (chaku < 1) | (ninki > n) | (chaku > n)
    if ijo_col in sed.columns:
        ijo = sed[ijo_col].astype(str).str.strip()
        bad = bad | ~ijo.isin(_IJO_OK)
    perf[bad.to_numpy()] = np.nan
    return perf


def sed_race_percentile_ato3f(sed, *, col="ato3f_time", race_col="race_id"):
    """過去走 上り(ato3f) の **race 内 percentile 適性** s=0.5−pct_asc。正=同走より速い終い。

    clip_3f で欠測化後、各 race 内の有効値だけで昇順 percentile（0=最速）→ s=0.5−pct∈[-0.5,0.5]。
    その過去走内の事実のみ使用＝strictly-prior 安全（現レース非含）。純関数（Series を返す）。
    """
    import pandas as pd
    clipped = pd.Series(clip_3f(sed[col].to_numpy()), index=sed.index)
    # race 内で有効値のみ昇順 percentile（欠測は除いて順位付け・pct∈[0,1]）
    pct = clipped.groupby(sed[race_col]).rank(pct=True, ascending=True)
    return 0.5 - pct


def fit_pace_calibration(pace_yosou, race_pace, *, alpha: float = ALPHA_DIRICHLET) -> dict:
    """fold内 calibration: P(z_actual=race_pace | pace_yosou) を 3×3 混同で推定（Dirichlet α）。

    pace_yosou/race_pace は**学習期間のみ**の対応配列（レース単位が理想・馬単位でも可）。返す
    {forecast_z: {actual_z: P}}（各 forecast 行は 3 actual 状態で正規化）。未知 forecast は一様。純関数。
    """
    import pandas as pd
    fz = sed_pace_state(pd.Series(pace_yosou)).reset_index(drop=True)
    az = sed_pace_state(pd.Series(race_pace)).reset_index(drop=True)
    ok = fz.notna() & az.notna()
    fz, az = fz[ok], az[ok]
    out: dict = {}
    for f in PACE_STATES:
        counts = {a: alpha for a in PACE_STATES}
        sub = az[fz == f]
        for a in sub:
            counts[a] += 1.0
        tot = sum(counts.values())
        out[f] = {a: counts[a] / tot for a in PACE_STATES}
    return out


def pr_z_from_forecast(current_pace_yosou, calibration) -> Optional[dict]:
    """現レース pace_yosou → P_r(z)（calibration の該当 forecast 行）。空/不明は None。"""
    import pandas as pd
    f = sed_pace_state(pd.Series([current_pace_yosou])).iloc[0]
    if f is None or f != f or not calibration or f not in calibration:
        return None
    return calibration[f]


def h3a_pace_aptitude(hist, current_pace_yosou, calibration, *, k: float = H3_SHRINK_K,
                      z_col="_z", perf_col="_perf") -> float:
    """H3a（frozen）: x = Σ_z P_r(z)·μ_{h,z}。P_r(z)=calibrated・μ_{h,z}=strictly-prior 市場残差縮約。

    hist は **strictly_prior_runs 済**の1頭ぶん（sed_pace_state 済 z_col・sed_market_perf 済 perf_col）。
    履歴なし→0・現レース予想欠測→0（市場を動かさない中立）。純関数。
    """
    import numpy as np
    import pandas as pd
    pr = pr_z_from_forecast(current_pace_yosou, calibration)
    if pr is None:
        return 0.0                              # 予想欠測→中立0（全馬0でそのレースは市場のまま）
    if hist is None or len(hist) == 0 or z_col not in hist.columns or perf_col not in hist.columns:
        return 0.0                              # 履歴なし→0
    perf = pd.to_numeric(hist[perf_col], errors="coerce")
    x = 0.0
    for z in PACE_STATES:
        sel = (hist[z_col] == z) & perf.notna()
        vals = perf[sel.to_numpy()].dropna()
        mu_z = shrink(float(np.mean(vals)), int(len(vals)), k=k) if len(vals) else 0.0
        x += float(pr.get(z, 0.0)) * mu_z
    return x


def h3b_lap_aptitude(hist, *, k: float = H3_SHRINK_K, s_col="_agari_pct") -> float:
    """H3b（frozen・1特徴）: strictly-prior な race内上り percentile 適性 s の縮約平均。履歴なし→0。

    hist は strictly_prior_runs 済の1頭ぶん（sed_race_percentile_ato3f 済 s_col）。正=同走より速い終い。
    """
    import numpy as np
    import pandas as pd
    if hist is None or len(hist) == 0 or s_col not in hist.columns:
        return 0.0
    s = pd.to_numeric(hist[s_col], errors="coerce").dropna()
    return shrink(float(np.mean(s)), int(len(s)), k=k) if len(s) else 0.0


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
