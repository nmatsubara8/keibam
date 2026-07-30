"""意思決定システムの厳密バックテスト（ユーザ仕様 §1-6 の忠実実装・realizable/OOS）。

「予測」ではなく「確率×金額の意思決定」を評価する。エッジ源泉が無い（市場半強効率・本セッションで
5経路 null 確定）前提でも、システムが log 成長・破産回避・損失最小化として正しく動くかを、
**ユーザ自身の合否基準（§10: OOS log growth>0 / ROI<0.95=失敗）** に対して測る。

パイプライン（§1-3）:
  校正済 p̂（Win ヘッド, isotonic 済）
  → KLブレンド  p*_i ∝ p̂_i^(1-λ) · q_i^λ     （q=TYB直前implied, 市場を制約として使う）
  → 不確実性    Var(p_i)=p*_i(1-p*_i)/(α0+1),  α0=Σ c·p*
  → フィルタ    edge=p*·o_tyb−1 > τ_edge, Var<τ_var, o_tyb∈[o_min,o_max]
  → サイズ      f_i = κ·max(0, (p*·o−1)/(o−1)),  0≤f≤f_max,  Σf≤F_race
  → 精算        realizable: 選定/サイズは TYB(o_tyb)、**払戻は最終単勝(o_final)**（pari-mutuel）

評価（§5）: 年ごと（OOS）に ROI(flat) / log成長 / Sharpe / MaxDD / hit / bet頻度。

使い方:
  python scripts/decision_system_backtest.py --version baseline_jrdb_seirei --jra-only \
      --db data/keibam.db --lam 0.2 --c 50 --kappa 0.25 --tau-edge 0.05
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def kl_blend(p_hat: np.ndarray, q: np.ndarray, lam: float) -> np.ndarray:
    """レース内 KLブレンド p* ∝ p̂^(1-λ)·q^λ（対数プーリング）。Σ=1 に正規化。"""
    eps = 1e-12
    w = np.power(np.maximum(p_hat, eps), 1.0 - lam) * np.power(np.maximum(q, eps), lam)
    s = w.sum()
    return w / s if s > 0 else np.full_like(w, 1.0 / len(w))


def dirichlet_var(p_star: np.ndarray, c: float) -> np.ndarray:
    """α=c·p*, α0=Σα=c → Var(p_i)=p*(1-p*)/(c+1)。"""
    return p_star * (1.0 - p_star) / (c + 1.0)


def kelly_fraction(p: np.ndarray, o: np.ndarray, kappa: float, f_max: float) -> np.ndarray:
    """フラクショナル Kelly f=κ·max(0,(p·o−1)/(o−1))、[0,f_max] にクリップ。"""
    b = o - 1.0
    f_star = np.where(b > 0, (p * o - 1.0) / b, 0.0)
    return np.clip(kappa * np.maximum(0.0, f_star), 0.0, f_max)


def select_and_size(p_star, o_tyb, var, params) -> np.ndarray:
    """フィルタ（edge/var/odds帯）→ Kelly サイズ → レース予算 F_race 制約。f ベクトルを返す。"""
    edge = p_star * o_tyb - 1.0
    keep = (edge > params["tau_edge"]) & (o_tyb >= params["o_min"]) & (o_tyb <= params["o_max"])
    if params["tau_var"] is not None:
        keep &= var <= params["tau_var"]
    f = np.where(keep, kelly_fraction(p_star, o_tyb, params["kappa"], params["f_max"]), 0.0)
    total = f.sum()
    if total > params["f_race"]:
        f = f * (params["f_race"] / total)      # レース内合計を F_race に按分
    return f


def settle_race(f: np.ndarray, o_final: np.ndarray, won: np.ndarray) -> tuple[float, float, int]:
    """realized: レース資金成長率 g=Σ f_i(o_final_i·won_i −1)、単位stake収支、賭け数。"""
    ret = f * (o_final * won - 1.0)
    g = float(ret.sum())                          # 資金比リターン（log(1+g) 用）
    staked = float(f.sum())
    payoff = float((f * o_final * won).sum())
    return g, staked, payoff, int((f > 0).sum())


def _metrics(g_list, staked, payoff, wealth_path):
    roi = payoff / staked if staked > 0 else float("nan")
    g = np.asarray(g_list, dtype=float)
    log_growth = float(np.mean(np.log1p(g))) if len(g) else float("nan")
    sharpe = float(g.mean() / g.std()) if len(g) > 1 and g.std() > 0 else float("nan")
    w = np.asarray(wealth_path, dtype=float)
    peak = np.maximum.accumulate(w)
    max_dd = float(np.max((peak - w) / peak)) if len(w) else float("nan")
    return roi, log_growth, sharpe, max_dd


def _load_tyb(engine) -> dict:
    from sqlalchemy import text
    df = pd.read_sql(text("SELECT race_id, umaban, tansho_odds FROM raw_jrdb_tyb"), engine)
    out: dict = {}
    for r in df.itertuples():
        if pd.isna(r.umaban) or r.tansho_odds is None:
            continue
        try:
            o = float(r.tansho_odds)
        except (TypeError, ValueError):
            continue
        if o > 0:
            out[(str(r.race_id).split(".")[0], int(r.umaban))] = o
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="意思決定システム 厳密バックテスト")
    ap.add_argument("--version", default="baseline_jrdb_seirei")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--test-size", type=float, default=0.2)
    ap.add_argument("--lam", type=float, default=0.2)
    ap.add_argument("--c", type=float, default=50.0)
    ap.add_argument("--kappa", type=float, default=0.25)
    ap.add_argument("--tau-edge", type=float, default=0.05)
    ap.add_argument("--var-cut-quantile", type=float, default=0.5, help="Var 上位カット分位（§τ_var）")
    ap.add_argument("--o-min", type=float, default=1.0)
    ap.add_argument("--o-max", type=float, default=100.0)
    ap.add_argument("--f-max", type=float, default=0.05)
    ap.add_argument("--f-race", type=float, default=0.15)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from app._data_loader import load_model_from_path, load_win_head_for
    from app._model_eval import _split_by_date
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.policies._score_policy import CURRENT_ODDS, PROB, ExpectedValueScorePolicy
    from src.storage._db import get_engine

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    eff = getattr(win_ai or place_ai, "effective_model", win_ai or place_ai)
    print(f"[dsys] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし'}）")

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    _, test = _split_by_date(featured, args.test_size)
    table = ExpectedValueScorePolicy.calc(eff, test)
    won = (pd.to_numeric(test[ResultsCols.RANK], errors="coerce").to_numpy() == 1).astype(float)
    df = pd.DataFrame({
        "rid": table.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(table[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "p_hat": np.asarray(table[PROB], dtype=float),
        "o_final": np.asarray(table[CURRENT_ODDS], dtype=float),
        "won": won,
    }).dropna(subset=["uma"])
    df["uma"] = df["uma"].astype(int)

    tyb_raw = _load_tyb(get_engine(args.db))
    ratios = [tyb_raw[(r, u)] / of for r, u, of in zip(df["rid"], df["uma"], df["o_final"],
              strict=False) if (r, u) in tyb_raw and of > 0]
    scale = 0.1 if (ratios and float(np.median(ratios)) > 3.0) else 1.0
    df["o_tyb"] = [tyb_raw.get((r, u), np.nan) * scale for r, u in zip(df["rid"], df["uma"], strict=False)]
    df = df[(df["o_tyb"] > 0) & (df["o_final"] > 0)].copy()
    df["year"] = df["rid"].str[:4]
    print(f"[dsys] holdout×TYB {len(df):,} 頭 / {df['rid'].nunique():,} レース（TYBスケール×{scale}）")
    print(f"[dsys] params: λ={args.lam} c={args.c} κ={args.kappa} τ_edge={args.tau_edge} "
          f"f_max={args.f_max} F_race={args.f_race}\n")

    # τ_var: 全体の Var 分布の分位で「上位カット」閾値を決める（§τ_var=上位50%カット）
    all_var = []
    for _, g in df.groupby("rid"):
        ps = kl_blend(g["p_hat"].to_numpy(), (1.0 / g["o_tyb"]).to_numpy()
                      / (1.0 / g["o_tyb"]).sum(), args.lam)
        all_var.append(dirichlet_var(ps, args.c))
    tau_var = float(np.quantile(np.concatenate(all_var), args.var_cut_quantile)) if all_var else None
    params = {"tau_edge": args.tau_edge, "tau_var": tau_var, "o_min": args.o_min,
              "o_max": args.o_max, "kappa": args.kappa, "f_max": args.f_max, "f_race": args.f_race}

    print(f"  {'年':<8}{'ROI':>9}{'log成長':>11}{'Sharpe':>9}{'MaxDD':>9}{'bet数':>9}{'賭ﾚｰｽ':>8}")
    overall = {"g": [], "staked": 0.0, "payoff": 0.0}
    for yr in sorted(df["year"].unique()):
        sub = df[df["year"] == yr]
        g_list, staked, payoff = [], 0.0, 0.0
        wealth, wpath, nbet, nrace_bet = 1.0, [1.0], 0, 0
        for _, g in sub.groupby("rid"):
            q = (1.0 / g["o_tyb"]).to_numpy()
            q = q / q.sum()
            p_star = kl_blend(g["p_hat"].to_numpy(), q, args.lam)
            var = dirichlet_var(p_star, args.c)
            f = select_and_size(p_star, g["o_tyb"].to_numpy(), var, params)
            gr, st, pay, nb = settle_race(f, g["o_final"].to_numpy(), g["won"].to_numpy())
            if st > 0:
                g_list.append(gr)
                staked += st
                payoff += pay
                nbet += nb
                nrace_bet += 1
                wealth *= (1.0 + gr)
                wpath.append(wealth)
        roi, lg, sh, dd = _metrics(g_list, staked, payoff, wpath)
        overall["g"] += g_list
        overall["staked"] += staked
        overall["payoff"] += payoff
        print(f"  {yr:<8}{roi:>9.4f}{lg:>+11.5f}{sh:>9.3f}{dd:>9.3f}{nbet:>9}{nrace_bet:>8}")
    o_roi = overall["payoff"] / overall["staked"] if overall["staked"] > 0 else float("nan")
    o_g = np.asarray(overall["g"])
    o_lg = float(np.mean(np.log1p(o_g))) if len(o_g) else float("nan")
    print(f"\n[dsys] 全体 ROI={o_roi:.4f} / 平均log成長={o_lg:+.5f}")
    print("[dsys] §10 判定: ", end="")
    ok = (o_lg > 0) and (o_roi >= 0.95)
    print("成功（log成長>0 かつ ROI≥0.95）" if ok
          else "失敗（ROI<0.95 or log成長≤0）— エッジ源 (p_ML−q)>0 が無いため（理論予測どおり）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
