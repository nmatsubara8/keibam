"""市場併走型 予測器の品質スコアボード（OOS）— 市場 vs モデル vs 合成(companion)。

新目的（市場効率を受容後）: 市場を超えるのでなく「市場と同等以上に正確で、よく較正された
確率予測器」を作る。北極星は **勝ち馬 logloss（市場≈1.96 を下回るほど良い）** と **較正(ECE)**。

各 OOS 年について、馬ごとの:
  - r_hat  : モデルのレース内正規化勝率
  - p_mkt  : 市場 implied 勝率（確定オッズ由来・Σ=1）
  - p_blend: 対数線形プール合成（models/blend_weights.json の α,β。companion 予測器の本体）
を作り、3者の「勝ち馬 logloss / Brier / ECE / AUC」を並べる。Blend(companion) を最小化するのが目標。

実行:
  python predict_quality.py --years 2026            # 最新モデル・2026 OOS
  python predict_quality.py --years 2025 2026
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _ece(prob, won, n_bins=10):
    """Expected Calibration Error（予測確率を等頻度ビンに分け |平均予測−実勝率| を加重平均）。"""
    import numpy as np
    import pandas as pd

    s = pd.DataFrame({"p": np.asarray(prob, float), "y": np.asarray(won, float)}).dropna()
    if s.empty:
        return float("nan")
    s["bin"] = pd.qcut(s["p"].rank(method="first"), min(n_bins, len(s)), labels=False)
    err = 0.0
    for _, g in s.groupby("bin"):
        err += len(g) / len(s) * abs(g["p"].mean() - g["y"].mean())
    return float(err)


def _blend_series(edge_df, alpha, beta):
    """edge_df の r_hat(p_fund)/p_mkt(p_public) を combine_logpool でレース内合成し、
    edge_df の行順に揃えた Series（index=race_id・位置整合）で返す。"""
    import pandas as pd

    from src.policies._blend import combine_logpool

    out = {}  # (race_id, 馬番) -> 合成勝率
    for rid, g in edge_df.groupby(level=0):
        p_fund = {int(u): float(p) for u, p in zip(g["umaban"], g["r_hat"], strict=False) if p == p}
        p_pub = {int(u): float(p) for u, p in zip(g["umaban"], g["p_mkt"], strict=False) if p == p}
        for u, p in combine_logpool(p_fund, p_pub, alpha, beta).items():
            out[(rid, int(u))] = p
    # edge_df の行順（index と 馬番）に沿って位置整合で取り出す。
    vals = [out.get((rid, int(u)), float("nan"))
            for rid, u in zip(edge_df.index, edge_df["umaban"], strict=False)]
    return pd.Series(vals, index=edge_df.index)


def main():
    ap = argparse.ArgumentParser(description="市場併走型 予測器の品質スコアボード（OOS）")
    ap.add_argument("--years", type=int, nargs="+", required=True, metavar="YYYY",
                    help="評価する OOS 年（学習に使っていない年。例: 2026）")
    ap.add_argument("--version", default=None, help="モデルのバージョン名（既定は最新）")
    args = ap.parse_args()

    import numpy as np
    import pandas as pd

    from app._data_loader import load_latest_model, load_model_by_version
    from app._model_eval import load_featured_data
    from src.simulation._edge_diagnostic import build_edge_frame
    from src.simulation._edge_diagnostic import _win_logloss
    from src.policies._blend import load_blend_weights
    from src.policies._score_policy import ExpectedValueScorePolicy

    model = load_model_by_version(args.version) if args.version else load_latest_model()
    if model is None:
        print("モデルが見つかりません")
        return
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return

    yset = {str(y) for y in args.years}
    rid = featured.index.astype(str)
    featured = featured[rid.str[:4].isin(yset)]
    if featured.empty:
        print(f"対象年 {sorted(yset)} のレースがありません")
        return
    if "着順" not in featured.columns:
        print("着順 列がありません")
        return
    won = (pd.to_numeric(featured["着順"], errors="coerce") == 1).astype(float)

    score_table = ExpectedValueScorePolicy.calc(model.effective_model, featured)
    edge = build_edge_frame(score_table, won.to_numpy())

    bw = load_blend_weights("models/blend_weights.json")
    alpha, beta = (bw.alpha, bw.beta) if bw is not None else (0.0, 1.0)
    edge["p_blend"] = _blend_series(edge, alpha, beta).to_numpy()

    print("=" * 70)
    print(f"予測品質スコアボード（OOS 年={sorted(yset)} / レース={edge.index.nunique()} / "
          f"馬={len(edge)} / blend α={alpha:.3f} β={beta:.3f}）")
    print(f"  {'予測器':<22}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}{'AUC':>8}")
    print("-" * 70)
    for name, col in [("市場 p_mkt", "p_mkt"), ("モデル r_hat", "r_hat"),
                      ("合成 companion", "p_blend")]:
        p = edge[col]
        ll = _win_logloss(p, edge["won"])
        brier = float(np.nanmean((p.to_numpy() - edge["won"].to_numpy()) ** 2))
        ece = _ece(p, edge["won"])
        try:
            from sklearn.metrics import roc_auc_score
            auc = roc_auc_score(edge["won"], p)
        except Exception:  # noqa: BLE001
            auc = float("nan")
        print(f"  {name:<22}{ll:>11.4f}{brier:>10.5f}{ece:>9.4f}{auc:>8.4f}")
    print("-" * 70)
    print("  ※ 勝logloss が小さいほど良い（市場が基準≈1.96）。companion が市場以下なら、")
    print("     市場より正確な確率予測＝新目的の達成。ECE 小=較正良好。")
    print("=" * 70)


if __name__ == "__main__":
    main()
