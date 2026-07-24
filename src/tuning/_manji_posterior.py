"""①.5b ベイズ事後分布ストア — (factor,bucket) のエッジを明示的 Normal-Normal 事後で較正。

現行 `_manji_calibration.calibrate_points` の √n/(√n+c) 収縮は経験ベイズそのもの。これを
**明示的な共役事後**に格上げする。

モデル（バケット b の単勝フラット回収 x = odds×[着1]）:
  事前   μ ~ N(μ0=1.0, τ0²=prior_var)      # 回収率=1（エッジ0）＝「初手は補正しない」中立事前
  尤度   x_i ~ N(μ, σ²)                     # σ² は学習窓の x のプール分散（既知として扱う）
  事後   post_mean = (μ0/τ0² + Σw_i·x_i/σ²) / (1/τ0² + Σw_i/σ²)
        post_var  = 1 / (1/τ0² + Σw_i/σ²)
  妙味度   myoumido(b) = 100 × post_mean          # 卍基準100（>100 買い妙味 / <100 過剰人気）
  加減点   point(b) = clip( λ·100·(post_mean − 1), −clip, +clip )  # 妙味度偏差（基準100→0点）

- 収縮定数は c ≈ σ²/τ0² に相当（n_eff→0 で事前=1へ、n_eff→∞ で標本平均へ）。小標本バケットは
  自動的にエッジ0へ寄る（＝卍「信頼度の低い因子は排除」）。unweighted n < min_n は不採用。
- **忘却割引（半減期）**: 重み w_i = 0.5^(age_days/half_life)。half_life=None で等重み（＝全過去
  依拠）、有限にすると近走の証拠が支配（＝直近数レース依拠）。同一機構で 2 系統を統一。
- **前進安全**: 本モジュールは渡された featured（＝学習窓）だけで事後を作る。時刻 t のレースの
  補正に使う事後は、必ず t 未満の証拠のみで構成する責任を呼び出し側（walk-forward / as-of 生成）
  が負う。build_posterior_store(as_of=...) はその境界を明示的に切る。

普遍性フィルタ（「明確・普遍的な傾向のみ」）とクロス残差化は _manji_calibration の実装を再利用する。
"""
from __future__ import annotations

import dataclasses

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import NA, FACTORS, factor_series
from src.tuning._manji_calibration import (
    _residualize_crosses,
    _time_slices,
    bucket_recovery,
)

# 近走の性質が強い因子（既定で有限半減期を与えたい候補）。呼び出し側が factor_half_life で上書き可。
RECENCY_FACTORS = ("recent3_form", "recent5_form", "recent3_recovery", "recent5_recovery")

# 卍氏「妙味度」の基準点。妙味度 = 100 × 補正回収率(post_mean)。100=中立、>100=過小評価(買い妙味)、
# <100=過剰人気。加減点(point) は基準100からの偏差 = 妙味度 − 100 = 100×(post_mean − 1) で表す
# （名鑑「妙味度が10違えば回収率も10%違う」と整合）。
MYOUMIDO_BASE = 100.0


@dataclasses.dataclass(frozen=True)
class PosteriorConfig:
    """Normal-Normal 事後較正のパラメータ。"""
    mu0: float = 1.0            # 中立事前（回収率1＝妙味度100＝エッジ0）
    prior_var: float = 0.25    # τ0²（事前分散。大きいほど動きやすい＝収縮弱）
    clip: float = 50.0         # 加減点(妙味度偏差)の絶対上限。±50=妙味度50〜150相当
    lam: float = 1.0           # 点のスケール（妙味度偏差＝100×(post_mean−1) に乗ずる）
    min_n: int = 30            # このunweighted件数未満のバケットは不採用（0点=妙味度100）
    sigma2_floor: float = 1e-6  # σ² の下限
    half_life_days: float | None = None  # 既定=割引なし（全過去等重み）。近走系は短めを渡す
    universality_slices: int = 3
    min_agree: float = 0.7
    # 回収率の測り方。"flat"=均等買い（単勝フラット、人気薄の大穴に大きく左右される）。
    # "implied"=卍流「均等払戻」＝賭け金をオッズに反比例（stake=1/odds）にした補正回収率。
    # 実質「実勝利数 / 市場implied勝利数」で高配当の分散に頑健。人×種牡馬の高カード因子向き。
    recovery_mode: str = "flat"


def _x_and_valid(featured: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """x = 単勝オッズ×[着1] と、決済可能（着順・オッズ既知）マスクを返す。"""
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    odds = pd.to_numeric(featured[ResultsCols.TANSHO_ODDS], errors="coerce")
    win = (rank == 1).astype(float)
    x = (odds * win).to_numpy()
    valid = rank.notna().to_numpy() & odds.notna().to_numpy() & np.isfinite(x)
    return x, valid


def global_sigma2(featured: pd.DataFrame, floor: float = 1e-6) -> float:
    """学習窓の x=odds×[着1] のプール分散（既知観測分散として使う）。"""
    x, valid = _x_and_valid(featured)
    xv = x[valid]
    if xv.size == 0:
        return floor
    return max(float(np.var(xv)), floor)


def factor_posterior(
    featured: pd.DataFrame,
    factor: str,
    cfg: PosteriorConfig,
    *,
    half_life_days: float | None = None,
    sigma2: float | None = None,
    prior_offsets: dict[str, float] | None = None,
) -> pd.DataFrame:
    """因子 factor のバケット別 事後統計を返す。

    Returns
    -------
    index=bucket, columns=[n, n_eff, post_mean, post_var, myoumido, point]。全バケット。
    myoumido=100×post_mean（卍基準100）、point=妙味度偏差=100×(post_mean−1)。
    （point は min_n 未満だと NaN。呼び出し側 calibrate は min_n/普遍性で採否を決める）
    """
    x, valid = _x_and_valid(featured)
    b = factor_series(featured, factor).astype(object).fillna(NA).to_numpy()
    dates = pd.to_datetime(featured["date"], errors="coerce").to_numpy() if "date" in featured.columns \
        else np.array([np.datetime64("NaT")] * len(featured))
    mask = valid & (b != NA)
    if not mask.any():
        return pd.DataFrame(columns=["n", "n_eff", "post_mean", "post_var", "myoumido", "point"])
    xv, bv, dv = x[mask], b[mask], dates[mask]

    if sigma2 is None:
        sigma2 = max(float(np.var(xv)), cfg.sigma2_floor)
    hl = half_life_days if half_life_days is not None else cfg.half_life_days
    if hl and np.isfinite(hl) and hl > 0 and not np.all(np.isnat(dv)):
        end = np.nanmax(dv)
        age_days = (end - dv) / np.timedelta64(1, "D")
        age_days = np.where(np.isfinite(age_days), age_days, 0.0)
        w = np.power(0.5, np.clip(age_days, 0.0, None) / hl)
    else:
        w = np.ones(len(xv))

    # 均等払戻（卍流補正回収率）: 賭け金をオッズに反比例（stake=1/odds）にして重み付け。
    # 加重平均 Σ(w·x)/Σw = Σ[着1] / Σ(1/odds) = 実勝利数 / 市場implied勝利数（大穴に頑健）。
    if cfg.recovery_mode == "implied":
        odds_m = pd.to_numeric(featured[ResultsCols.TANSHO_ODDS], errors="coerce").to_numpy()[mask]
        stake = np.where(np.isfinite(odds_m) & (odds_m > 0), 1.0 / odds_m, 0.0)
        w = w * stake

    work = pd.DataFrame({"b": bv, "x": xv, "w": w})
    inv_pv = 1.0 / cfg.prior_var
    out = {}
    for bucket, g in work.groupby("b"):
        n = int(len(g))
        n_eff = float(g["w"].sum())
        if n_eff <= 0:
            continue
        xbar = float((g["w"] * g["x"]).sum() / n_eff)
        prec = inv_pv + n_eff / sigma2
        # 事前平均 μ0_b: 卍の方向性ルール（妙味度オフセット）を基準100に足す。
        # offset[妙味度pt] → 回収率オフセット = offset/100。未定義バケットは中立(cfg.mu0)。
        mu0_b = cfg.mu0
        if prior_offsets:
            mu0_b += float(prior_offsets.get(bucket, 0.0)) / MYOUMIDO_BASE
        post_mean = (mu0_b * inv_pv + n_eff * xbar / sigma2) / prec
        post_var = 1.0 / prec
        myoumido = MYOUMIDO_BASE * post_mean  # 卍妙味度（基準100）
        # 加減点 = 妙味度偏差 = 100×(post_mean−1)。基準100（neutral=0点、妙味度100）。
        point = float(np.clip(cfg.lam * MYOUMIDO_BASE * (post_mean - 1.0), -cfg.clip, cfg.clip)) \
            if n >= cfg.min_n else np.nan
        out[bucket] = (n, n_eff, post_mean, post_var, myoumido, point)
    return pd.DataFrame.from_dict(
        out, orient="index",
        columns=["n", "n_eff", "post_mean", "post_var", "myoumido", "point"],
    )


def calibrate_points_bayes(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    cfg: PosteriorConfig | None = None,
    factor_half_life: dict[str, float] | None = None,
    factor_priors: dict[str, dict[str, float]] | None = None,
    residualize: bool = True,
) -> dict[str, dict[str, float]]:
    """学習窓 featured から points[factor][bucket] を Normal-Normal 事後で導出する。

    - σ² は窓全体でプール（バケット間で共有＝小標本を安定化）。
    - factor_half_life[factor] があればその因子だけ忘却割引（近走系向け）。
    - factor_priors[factor][bucket] があれば卍の方向性ルールを情報事前として使う
      （基準100からの妙味度オフセット。小標本は事前へ収縮、n増で実測が上書き）。
    - 普遍性フィルタ: 時系列 K 分割で符号一致が min_agree 未満のバケットは 0（不採用）。
    - residualize=True でクロス 'A*B' は交互作用残差に置換（加法二重計上の回避、既存実装を再利用）。

    渡す featured は**学習窓のみ**（未来非混入）であること。前進安全の保証は呼び出し側の責任。
    """
    cfg = cfg or PosteriorConfig()
    factor_names = factor_names or list(FACTORS)
    factor_half_life = factor_half_life or {}
    sigma2 = global_sigma2(featured, cfg.sigma2_floor)
    slices = _time_slices(featured, cfg.universality_slices) if cfg.universality_slices > 1 else []

    points: dict[str, dict[str, float]] = {}
    for f in factor_names:
        post = factor_posterior(
            featured, f, cfg, half_life_days=factor_half_life.get(f), sigma2=sigma2,
            prior_offsets=(factor_priors.get(f) if factor_priors else None),
        )
        if post.empty:
            continue
        # 普遍性: 各バケットの符号（回収−1）がサブ期間で min_agree 以上一致するか
        agree: dict[str, float] = {}
        if slices:
            for b in post.index:
                fs = np.sign(float(post.loc[b, "post_mean"]) - 1.0)
                if fs == 0.0:
                    agree[b] = 0.0
                    continue
                hits = tot = 0
                for sl in slices:
                    sr = bucket_recovery(sl, f)
                    if b in sr.index and sr.loc[b, "n"] >= max(5, cfg.min_n // 3):
                        tot += 1
                        if np.sign(sr.loc[b, "recovery"] - 1.0) == fs:
                            hits += 1
                agree[b] = (hits / tot) if tot else 0.0

        fmap: dict[str, float] = {}
        for b, row in post.iterrows():
            pt = row["point"]
            if not np.isfinite(pt) or pt == 0.0:
                continue
            if slices and agree.get(b, 0.0) < cfg.min_agree:
                continue
            fmap[b] = float(pt)
        if fmap:
            points[f] = fmap
    if residualize:
        points = _residualize_crosses(points)
    return points


def build_posterior_store(
    featured: pd.DataFrame,
    factor_names: list[str] | None = None,
    *,
    cfg: PosteriorConfig | None = None,
    factor_half_life: dict[str, float] | None = None,
    as_of: str | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """(factor,bucket) の事後統計を tidy 表で materialize する（検査・再利用用）。

    as_of を渡すと **その日付より前**の証拠だけで事後を作る（前進安全な境界を明示）。
    Returns: columns=[factor, bucket, n, n_eff, post_mean, post_var, point, half_life_days, as_of]
    """
    cfg = cfg or PosteriorConfig()
    factor_names = factor_names or list(FACTORS)
    factor_half_life = factor_half_life or {}

    data = featured
    if as_of is not None and "date" in featured.columns:
        cut = pd.to_datetime(as_of)
        data = featured[pd.to_datetime(featured["date"], errors="coerce") < cut]
    sigma2 = global_sigma2(data, cfg.sigma2_floor)

    rows = []
    for f in factor_names:
        hl = factor_half_life.get(f)
        post = factor_posterior(data, f, cfg, half_life_days=hl, sigma2=sigma2)
        for b, row in post.iterrows():
            rows.append({
                "factor": f, "bucket": b,
                "n": int(row["n"]), "n_eff": float(row["n_eff"]),
                "post_mean": float(row["post_mean"]), "post_var": float(row["post_var"]),
                "myoumido": float(row["myoumido"]),  # 卍妙味度（基準100）
                "point": float(row["point"]) if np.isfinite(row["point"]) else np.nan,
                "half_life_days": hl if hl is not None else np.nan,
                "as_of": pd.to_datetime(as_of) if as_of is not None else pd.NaT,
            })
    cols = ["factor", "bucket", "n", "n_eff", "post_mean", "post_var", "myoumido", "point",
            "half_life_days", "as_of"]
    return pd.DataFrame(rows, columns=cols)


def default_half_lives(base_days: float = 540.0) -> dict[str, float]:
    """近走系因子に既定の半減期を割る補助（呼び出し側が calibrate に渡す）。

    近走の妙味は時とともに市場に織り込まれ得るので、直近の証拠を重めにする。既定 ~1.5 年。
    """
    return {f: float(base_days) for f in RECENCY_FACTORS}


def save_posterior_store(store: pd.DataFrame, path=None) -> None:
    import os

    from src.constants._local_paths import LocalPaths
    path = path or LocalPaths.MANJI_POSTERIOR_STORE_PATH
    os.makedirs(os.path.dirname(path), exist_ok=True)
    store.to_pickle(path)


def load_posterior_store(path=None) -> pd.DataFrame | None:
    import os

    from src.constants._local_paths import LocalPaths
    path = path or LocalPaths.MANJI_POSTERIOR_STORE_PATH
    if not os.path.exists(path):
        return None
    return pd.read_pickle(path)


def main() -> None:
    import argparse

    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="卍①.5b ベイズ事後分布ストアを生成")
    ap.add_argument("--as-of", default=None, help="この日付(YYYY-MM-DD)より前の証拠のみで事後を作る")
    ap.add_argument("--prior-var", type=float, default=0.25)
    ap.add_argument("--min-n", type=int, default=30)
    ap.add_argument("--half-life-days", type=float, default=None,
                    help="近走系因子(recent*)に与える忘却半減期（既定=割引なし）")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    from src.constants._local_paths import LocalPaths
    import os
    if os.path.exists(LocalPaths.MANJI_POSTERIOR_STORE_PATH) and not args.force:
        print(f"既に存在します: {LocalPaths.MANJI_POSTERIOR_STORE_PATH}（再生成は --force）")
        return

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（先に rebuild-featured を実行）")
        return
    cfg = PosteriorConfig(prior_var=args.prior_var, min_n=args.min_n)
    fhl = default_half_lives(args.half_life_days) if args.half_life_days else None
    store = build_posterior_store(featured, cfg=cfg, factor_half_life=fhl, as_of=args.as_of)
    save_posterior_store(store)
    n_adopt = int(store["point"].notna().sum())
    print(f"\n生成完了: {LocalPaths.MANJI_POSTERIOR_STORE_PATH}"
          f"（{len(store):,} バケット / 採用 {n_adopt}）")
    print("次段（Step 3）: シナリオ compile — 本ストア×factor_table の線形結合で補正列を作る。")


if __name__ == "__main__":
    main()
