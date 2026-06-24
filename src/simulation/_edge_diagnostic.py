"""パリミュチュエルのエッジ診断 — 自分の勝率推定 r̂ と「実現最終市場」p_final の比較。

JRA はパリミュチュエル（払戻は確定オッズ基準）なので、馬券の優位は
``EV_i = r̂_i · O_{i,final} − 1`` / ``Edge_i = r̂_i − p̂_{i,final}`` で決まる。
本モジュールは**過去レース**（最終オッズが実現値として手に入る）に対し、

- r̂  : 自分の勝率推定（Win ヘッドの予測をレース内で Σ=1 に正規化）
- p_mkt: 実現最終単勝オッズ由来の市場勝率（控除抜き＝レース内で Σ=1）
- Edge = r̂ − p_mkt / EV = r̂·O_final − 1
- 人気帯別の較正（市場/モデルの平均確率 vs 実勝率）とマーケット・エコー（r̂ と p_mkt の相関）
- 勝ち馬 logloss（モデル vs 市場）= 「市場を超える情報があるか」の単一指標

を算出する。**力学モデルは不要**（実現最終オッズを使う）。これにより「そもそも対市場
エッジが存在するか」を backtest で先に判定できる（ライブ予測の前段の健全性チェック）。

Streamlit/モデルに依存しない純粋関数で構成（run_edge_diagnostic だけ model を受け取る）。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB

_EPS = 1e-12


def _within_race_normalize(s: pd.Series) -> pd.Series:
    """race_id（index level0）ごとに非負値を Σ=1 へ正規化する。総和0は NaN。"""
    return s.groupby(level=0, group_keys=False).transform(
        lambda x: x / x.sum() if x.sum() > 0 else np.nan
    )


def market_implied_prob(odds: pd.Series) -> pd.Series:
    """実現最終オッズ（単勝）→ 控除抜き市場勝率（レース内 Σ=1）。

    非正・NaN オッズは寄与させない。p_mkt_i = (1/O_i) / Σ_j (1/O_j)。
    """
    inv = 1.0 / odds.where(odds > 0)
    return _within_race_normalize(inv)


def build_edge_frame(score_table: pd.DataFrame, won) -> pd.DataFrame:
    """score_table（[馬番, prob, current_odds]・index=race_id）と勝敗から馬単位の診断表を作る。

    Parameters
    ----------
    score_table : ExpectedValueScorePolicy.calc(win_model, X) の戻り値
    won : score_table と同順の 0/1（1着=1）。Series/array いずれも可

    Returns
    -------
    columns: umaban, r_hat(レース内正規化勝率), odds, p_mkt, edge, ev, won, pop_rank
    """
    df = pd.DataFrame(index=score_table.index)
    df["umaban"] = score_table[ResultsCols.UMABAN].to_numpy()
    df["odds"] = pd.to_numeric(score_table[CURRENT_ODDS], errors="coerce").to_numpy()
    df["won"] = np.asarray(won, dtype=float)
    df["r_hat"] = _within_race_normalize(
        pd.Series(pd.to_numeric(score_table[PROB], errors="coerce").to_numpy(), index=df.index)
    )
    df["p_mkt"] = market_implied_prob(pd.Series(df["odds"].to_numpy(), index=df.index))
    df["edge"] = df["r_hat"] - df["p_mkt"]
    df["ev"] = df["r_hat"] * df["odds"] - 1.0
    # 人気順位（市場勝率が高い=1）。同率は最小順位。
    df["pop_rank"] = df.groupby(level=0)["p_mkt"].rank(ascending=False, method="min")
    return df


def _win_logloss(prob: pd.Series, won: pd.Series) -> float:
    """勝ち馬 logloss = 勝った馬の予測勝率の −log の平均（小さいほど良い）。

    1着行（won==1）のみ評価。レース内 Σ=1 正規化済み prob を渡す前提。
    """
    mask = won.to_numpy() == 1
    if not mask.any():
        return float("nan")
    p = np.clip(prob.to_numpy()[mask], _EPS, 1.0)
    return float(-np.log(p).mean())


def calibration_by_band(edge_df: pd.DataFrame, n_bands: int = 10) -> pd.DataFrame:
    """市場勝率 p_mkt の分位で帯分けし、帯別の較正とエッジを集計する。

    各帯で: 件数 / 平均 p_mkt(市場) / 平均 r_hat(モデル) / 実勝率(won平均) / 平均edge / 平均ev。
    「平均p_mkt vs 実勝率」で市場の人気-穴バイアス、「平均r_hat vs 実勝率」でモデル較正、
    「平均r_hat ≈ 平均p_mkt」ならマーケット・エコー（モデルが市場の写し）を読む。
    """
    df = edge_df.dropna(subset=["p_mkt", "r_hat"]).copy()
    if df.empty:
        return pd.DataFrame()
    # 重複境界に強い rank ベースの分位ビニング
    ranks = df["p_mkt"].rank(method="first")
    bands = np.ceil(ranks / len(df) * n_bands).clip(1, n_bands).astype(int)
    df["_band"] = bands.to_numpy()
    g = df.groupby("_band")
    out = pd.DataFrame({
        "n": g.size(),
        "p_mkt_mean": g["p_mkt"].mean(),
        "r_hat_mean": g["r_hat"].mean(),
        "win_rate": g["won"].mean(),
        "edge_mean": g["edge"].mean(),
        "ev_mean": g["ev"].mean(),
    })
    out.index.name = "band"
    return out.reset_index()


def echo_correlation(edge_df: pd.DataFrame) -> float:
    """r_hat と p_mkt の Pearson 相関（全馬）。1 に近いほど「モデル=市場の写し」。"""
    df = edge_df.dropna(subset=["r_hat", "p_mkt"])
    if len(df) < 2 or df["r_hat"].std() == 0 or df["p_mkt"].std() == 0:
        return float("nan")
    return float(np.corrcoef(df["r_hat"], df["p_mkt"])[0, 1])


def diagnostic_summary(edge_df: pd.DataFrame) -> dict:
    """総括: 勝ち馬 logloss（モデル/市場）・エコー相関・規模。

    ``model_beats_market`` = モデル logloss < 市場 logloss（市場を超える情報の有無）。
    """
    df = edge_df.dropna(subset=["r_hat", "p_mkt"])
    n_races = df.index.nunique()
    model_ll = _win_logloss(df["r_hat"], df["won"])
    market_ll = _win_logloss(df["p_mkt"], df["won"])
    return {
        "n_races": int(n_races),
        "n_horses": int(len(df)),
        "echo_corr": echo_correlation(df),
        "model_win_logloss": model_ll,
        "market_win_logloss": market_ll,
        "model_beats_market": bool(model_ll < market_ll)
        if not (np.isnan(model_ll) or np.isnan(market_ll)) else False,
    }


def _actual_win(X: pd.DataFrame) -> pd.Series:
    """featured から 1着フラグ（0/1）を取り出す。rank_win 優先、無ければ 着順==1。"""
    if "rank_win" in X.columns:
        return (pd.to_numeric(X["rank_win"], errors="coerce") == 1).astype(float)
    if ResultsCols.RANK in X.columns:
        return (pd.to_numeric(X[ResultsCols.RANK], errors="coerce") == 1).astype(float)
    raise KeyError("勝敗列が featured にありません（rank_win / 着順 のいずれかが必要）")


def run_edge_diagnostic(model, X: pd.DataFrame, n_bands: int = 10) -> dict:
    """Win モデルと featured（実現最終単勝込み）から Edge/EV 診断を実行する。

    Returns: {"edge_df", "calibration", "summary"}。
    model は勝率モデル（Win ヘッド推奨。Place でも動くが r̂=top3 確率になる）。
    """
    from src.policies._score_policy import ExpectedValueScorePolicy

    table = ExpectedValueScorePolicy.calc(model, X)
    won = _actual_win(X)
    edge_df = build_edge_frame(table, won.to_numpy())
    return {
        "edge_df": edge_df,
        "calibration": calibration_by_band(edge_df, n_bands=n_bands),
        "summary": diagnostic_summary(edge_df),
    }


def format_edge_report(result: dict) -> str:
    """run_edge_diagnostic の結果を人が読める表に整形する。"""
    s = result["summary"]
    calib = result["calibration"]
    lines = ["=== Edge/EV 診断（自分の勝率 r̂ vs 実現最終市場 p_mkt）==="]
    lines.append(
        f"レース数={s['n_races']:,} 馬数={s['n_horses']:,}  "
        f"エコー相関(r̂,p_mkt)={s['echo_corr']:.3f}"
    )
    lines.append(
        f"勝ち馬logloss: モデル={s['model_win_logloss']:.4f} / 市場={s['market_win_logloss']:.4f} "
        f"→ {'モデルが市場に勝つ✅' if s['model_beats_market'] else '市場に勝てず（エッジ薄）⚠️'}"
    )
    if not calib.empty:
        lines.append("\n人気帯別 較正（band 1=人気薄… n=本命）:")
        lines.append(f"{'band':>4}{'n':>7}{'p_mkt':>9}{'r_hat':>9}{'実勝率':>9}{'edge':>9}{'EV':>9}")
        for _, r in calib.iterrows():
            lines.append(
                f"{int(r['band']):>4}{int(r['n']):>7}{r['p_mkt_mean']:>9.3f}"
                f"{r['r_hat_mean']:>9.3f}{r['win_rate']:>9.3f}{r['edge_mean']:>+9.3f}{r['ev_mean']:>+9.2f}"
            )
    return "\n".join(lines)
