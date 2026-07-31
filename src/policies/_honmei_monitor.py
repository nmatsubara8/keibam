"""複勝本命（BetPolicyFukushoHonmei）運用点のモニタ（④ 運用監視）。

固定推奨 min_score（`scripts/loss_min_operating_point.py` が出す運用点）で運用するとき、
「採用率・本命 score 分布・実現回収率」が期間で逸脱していないかを検知するための純ロジック。
市場ドリフト監視（odds_watch 等）とは別軸＝**ポリシー運用点そのもの**の監視。

使い方:
    ref = monitor_honmei(score_table_ref, min_score=0.42)   # 推奨運用点の期待プロファイル
    cur = monitor_honmei(score_table_now, min_score=0.42, realized_returns=returns_now)
    alerts = detect_drift(ref, cur)                          # 逸脱を列挙（空なら正常）
"""
from __future__ import annotations

import pandas as pd


def monitor_honmei(
    score_table: pd.DataFrame,
    *,
    min_score: float,
    top_n: int = 1,  # noqa: ARG001 — 本命(top1)の score を監視。将来 top_n>1 拡張の受け口
    realized_returns=None,
) -> dict:
    """複勝本命の運用点プロファイルを返す。

    score_table: index=race_id、列 "score"（place=top3 確率）。judge と同じ入力。
    - adoption_rate: min_score を満たし賭けたレース割合（見送りの裏返し）。
    - honmei_score_{mean,p10,p50,p90}: 各レース最高 score（=本命）の分布。
    - realized_returns（任意・賭けたレースの払戻 円/100円・外れは0）: hit_rate / return_rate。
    """
    if score_table is None or score_table.empty or "score" not in score_table.columns:
        return {"n_races": 0, "n_bet_races": 0, "adoption_rate": 0.0, "below_min_rate": 0.0}
    race_max = score_table.groupby(level=0)["score"].max()
    n_races = int(len(race_max))
    bet = race_max[race_max >= min_score]
    n_bet = int(len(bet))
    out: dict = {
        "min_score": float(min_score),
        "n_races": n_races,
        "n_bet_races": n_bet,
        "adoption_rate": (n_bet / n_races) if n_races else 0.0,
        "below_min_rate": ((n_races - n_bet) / n_races) if n_races else 0.0,
        "honmei_score_mean": float(bet.mean()) if n_bet else None,
        "honmei_score_p10": float(bet.quantile(0.10)) if n_bet else None,
        "honmei_score_p50": float(bet.quantile(0.50)) if n_bet else None,
        "honmei_score_p90": float(bet.quantile(0.90)) if n_bet else None,
    }
    if realized_returns is not None:
        rr = [float(x) for x in realized_returns]
        if rr:
            out["hit_rate"] = sum(1 for x in rr if x > 0) / len(rr)
            out["return_rate"] = sum(rr) / (100.0 * len(rr))  # 円/100円ベット
    return out


def detect_drift(
    reference: dict,
    current: dict,
    *,
    adoption_tol: float = 0.10,
    score_tol: float = 0.03,
    roi_tol: float = 0.05,
) -> list[str]:
    """reference（推奨運用点の期待）と current を比較し、許容超の逸脱を列挙する（空=正常）。"""
    alerts: list[str] = []

    def _cmp(key: str, tol: float, label: str) -> None:
        a, b = reference.get(key), current.get(key)
        if a is not None and b is not None and abs(b - a) > tol:
            alerts.append(f"{label}: {a:.3f} → {b:.3f}（Δ{b - a:+.3f} / 許容±{tol}）")

    _cmp("adoption_rate", adoption_tol, "採用率")
    _cmp("honmei_score_mean", score_tol, "本命score平均")
    _cmp("return_rate", roi_tol, "回収率")
    return alerts
