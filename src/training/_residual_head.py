"""市場アンカー残差ヘッド r_θ の学習（Step1 の学習レイヤ・JRDB アブレーションの土台）。

canonical 形 P_i = softmax(log q_i + r_i) の r_i = f_θ(x_i) を LightGBM で学習する:
    - 二値（勝ち=1）logloss + **init_score = log q**（市場アンカー）。市場が既に説明する分は
      オフセットに載っているため、木は「市場からのズレ」だけを学習する（Residual Modeling）。
    - 予測は raw_score（init_score を含まないモデル出力）= r_i。P は softmax(log q + s·r)。
    - **s（合流スケール）は学習内バリデーションの listwise NLL 最小で選ぶ**。証拠が無ければ
      s→0 で市場に厳密退化する（「市場から離れるには証拠を要求する」の実装。λ的な
      事後調整ではなく、fit 内・過去データのみで決まるため rolling-origin と整合）。

特徴量: featured の数値列から目的変数・ID・事後情報を除いたもの。**単勝オッズ・人気は
特徴量として許可**（市場アンカー設計では「市場+特徴量→市場残差」が正 — FL バイアスの
学習は正当。アンカー log q と重複しても木は差分だけ拾う）。JRDB アブレーションは
`extra_cols`/`drop_prefixes` で列群を段階投入して同一ハーネスで比較する。

レイヤ: training。pandas/LightGBM。I/O なし（CLI は train_residual.py）。
"""
from __future__ import annotations

from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

# 目的変数・ID・事後情報（決して特徴量にしない）
_ALWAYS_DROP = {
    ResultsCols.RANK, "rank", "rank_win", "date", "horse_id", "race_id", "通過",
}


def residual_feature_cols(
    df: pd.DataFrame,
    *,
    drop_prefixes: Sequence[str] = (),
    extra_drop: Sequence[str] = (),
) -> list[str]:
    """残差ヘッドの特徴量列（数値のみ・リーク列除外）。

    drop_prefixes でカテゴリ群を丸ごと外せる（アブレーション用。例: ("jrdb_",) を外すと
    JRDB なしベースライン）。単勝/人気は既定で**含む**（docstring 参照）。
    """
    drop = _ALWAYS_DROP | set(extra_drop)
    cols = []
    for c in df.columns:
        cs = str(c)
        if c in drop or any(cs.startswith(p) for p in drop_prefixes):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    return cols


def _market_log_q(df: pd.DataFrame, rid: pd.Series) -> pd.Series:
    """行ごとの log q（レース内正規化した市場implied勝率の対数）。オッズ不正は NaN。"""
    odds = pd.to_numeric(df[ResultsCols.TANSHO_ODDS], errors="coerce")
    inv = 1.0 / odds.where(odds > 1.0)
    z = inv.groupby(rid).transform("sum")
    q = inv / z
    return np.log(q)


def _listwise_nll(log_q: np.ndarray, f: np.ndarray, win: np.ndarray,
                  rid: np.ndarray, scale: float) -> np.ndarray:
    """レース内 softmax(log q + scale·f) の勝ち馬 NLL を**レース別配列**で返す（scale 選択用）。"""
    s = log_q + scale * f
    d = pd.DataFrame({"rid": rid, "s": s, "w": win})
    m = d.groupby("rid")["s"].transform("max")
    e = np.exp(d["s"] - m)
    z = e.groupby(d["rid"]).transform("sum")
    p = (e / z).to_numpy()
    pw = p[d["w"].to_numpy().astype(bool)]
    return -np.log(np.maximum(pw, 1e-300))


def fit_residual_head(
    df: pd.DataFrame,
    feature_cols: Sequence[str],
    *,
    valid_fraction: float = 0.15,
    num_boost_round: int = 600,
    params: Mapping | None = None,
    scale_grid: Sequence[float] = tuple(np.arange(0.0, 1.51, 0.05)),
    se_guard: float = 0.25,
):
    """残差ヘッドを学習し (booster, scale, 診断dict) を返す。**rolling の fit_fn 内で呼ぶこと**。

    df は per-horse 行（index=race_id・着順/単勝あり）。バリデーションは race_id 昇順の
    **末尾レース**（時系列末尾・レース単位で分ける）。scale はバリデーション listwise NLL
    最小で選ぶ（s=0 が最良なら「特徴量に市場超の情報なし」＝市場へ退化）。
    """
    import lightgbm as lgb

    rid = pd.Series(df.index.astype(str), index=df.index)
    y = (pd.to_numeric(df[ResultsCols.RANK], errors="coerce") == 1).astype(int)
    log_q = _market_log_q(df, rid)
    ok = log_q.notna() & y.notna()
    d = df[ok]
    rid_ok, y_ok, lq = rid[ok], y[ok], log_q[ok]

    races = np.array(sorted(rid_ok.unique()))
    n_valid = max(1, int(len(races) * valid_fraction))
    valid_races = set(races[-n_valid:])
    is_valid = rid_ok.isin(valid_races).to_numpy()

    x = d[list(feature_cols)].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float64)
    p = {"objective": "binary", "metric": "binary_logloss", "learning_rate": 0.05,
         "num_leaves": 31, "min_data_in_leaf": 100, "feature_fraction": 0.8,
         "bagging_fraction": 0.8, "bagging_freq": 1, "lambda_l2": 1.0,
         "verbosity": -1, **(params or {})}
    dtr = lgb.Dataset(x[~is_valid], label=y_ok[~is_valid].to_numpy(),
                      init_score=lq[~is_valid].to_numpy())
    dva = lgb.Dataset(x[is_valid], label=y_ok[is_valid].to_numpy(),
                      init_score=lq[is_valid].to_numpy(), reference=dtr)
    booster = lgb.train(p, dtr, num_boost_round=num_boost_round, valid_sets=[dva],
                        callbacks=[lgb.early_stopping(50, verbose=False)])

    # 合流スケール s: バリデーション listwise NLL 最小 ＋ **se_guard×標準誤差の軽いガード**。
    # 純粋最小化だと無信号でもノイズで s>0 を選びがち（合成世界で s=0.5 を観測）。ただし
    # 1SE は保守的すぎて真の弱信号も殺す（drift 対照で改善0.022<1SE を棄却）。有意性の
    # 本丸は OOS の compare_models（Bootstrap CI＋LRT）なので、ここは egregious な過学習だけ
    # 止める軽ガード（既定 0.25SE）に留める。se_guard=0 で純粋最小化、大で保守化。
    f_va = booster.predict(x[is_valid], raw_score=True,
                           num_iteration=booster.best_iteration)
    lq_va = lq[is_valid].to_numpy()
    win_va = y_ok[is_valid].to_numpy()
    rid_va = rid_ok[is_valid].to_numpy()
    per_race = {s: _listwise_nll(lq_va, f_va, win_va, rid_va, s) for s in scale_grid}
    means = {s: float(v.mean()) for s, v in per_race.items()}
    s_best = min(means, key=means.get)
    nll0 = means.get(0.0, float("nan"))
    scale = float(s_best)
    if s_best != 0.0 and 0.0 in per_race:
        diff = per_race[s_best] - per_race[0.0]
        se = float(diff.std(ddof=1) / np.sqrt(len(diff))) if len(diff) > 1 else float("inf")
        if float(diff.mean()) > -se_guard * se:  # 改善が se_guard×SE に満たない → 市場へ退化
            scale = 0.0
    diag = {"scale": scale, "nll_market": nll0,
            "nll_best": means[s_best], "nll_used": means.get(scale, nll0),
            "best_iteration": int(booster.best_iteration or 0),
            "n_train_races": int(len(races) - n_valid), "n_valid_races": n_valid}
    return booster, scale, diag


def predict_residual(booster, df: pd.DataFrame, feature_cols: Sequence[str],
                     scale: float) -> pd.Series:
    """r_i = scale · f_θ(x_i)（raw_score・init_score 非含）。index は df のまま。"""
    x = df[list(feature_cols)].apply(pd.to_numeric, errors="coerce").to_numpy(dtype=np.float64)
    f = booster.predict(x, raw_score=True, num_iteration=getattr(booster, "best_iteration", None))
    return pd.Series(scale * f, index=df.index)
