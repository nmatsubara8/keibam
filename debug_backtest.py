"""券種別バックテストの指標が内部矛盾（的中率↑かつ回収率↑）を起こす原因の診断。

optimize_bet_types が選ぶ degenerate なパラメータ（温度1.6 等）と、検証済み近傍
（EV1.1/温度1.0）を同じ held-out 区間で 1 点ずつバックテストし、レース単位の
bet_amount / return_amount を突き合わせて「多頭ベットなのか」「払戻集計が過大なのか」を
切り分ける。漏洩ではなく集計バグかを確定するための使い捨てスクリプト。

実行:
    python debug_backtest.py                 # tansho・既定2点
    python debug_backtest.py --bet-type fukusho
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _load(models_dir="models", test_frac=0.2):
    from app._data_loader import load_latest_model
    from app._model_compare import recent_race_slice
    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.preprocessing._return_processor import ReturnProcessor

    ai = load_latest_model(models_dir)
    featured_all = load_featured_data()
    rp = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    featured = recent_race_slice(featured_all, test_frac)
    return ai, featured, rp


def _probe(ai, featured, rp, bet_type, ev_threshold, temperature, takeout=0.2):
    from app._bet_type_optimizer import backtest_bet_type
    from src.policies._bet_type_params import BetTypeParams

    params = BetTypeParams(ev_threshold=ev_threshold, temperature=temperature, prob_scale=1.0)
    summary, per_race = backtest_bet_type(ai, featured, rp, bet_type, params, takeout=takeout)
    print("=" * 72)
    print(f"[{bet_type}] EV閾値={ev_threshold} 温度={temperature}")
    if not summary or per_race is None or len(per_race) == 0:
        print("  賭け不成立（summary 空）")
        return
    keys = ["n_bets", "n_races", "n_hits", "total_bet_amount", "return_rate", "hit_rate",
            "sharpe_ratio", "profit"]
    print("  summary:", {k: round(summary[k], 4) if isinstance(summary.get(k), float) else summary.get(k)
                         for k in keys if k in summary})
    # レース単位の整合性チェック
    n_races = len(per_race)
    avg_bets = per_race["n_bets"].mean()
    total_bet = per_race["bet_amount"].sum()
    total_ret = per_race["return_amount"].sum()
    print(f"  レース数={n_races}  平均ベット数/レース={avg_bets:.2f}  "
          f"total_bet={total_bet:.1f}  total_return={total_ret:.1f}  "
          f"手計算回収率={total_ret / total_bet if total_bet else float('nan'):.4f}")
    # 1レースで return_amount >> bet_amount のレース（払戻過大＝集計バグの兆候）
    pr = per_race.copy()
    pr["ratio"] = pr["return_amount"] / pr["bet_amount"].replace(0, float("nan"))
    top = pr.sort_values("ratio", ascending=False).head(5)
    print("  払戻/賭金比が高い上位レース（bet_amount, return_amount, ratio）:")
    for rid, row in top.iterrows():
        print(f"    {rid}: bet={row['bet_amount']:.0f} return={row['return_amount']:.1f} "
              f"ratio={row['ratio']:.1f} n_bets={row['n_bets']:.0f}")


def _detail(ai, featured, rp, bet_type, ev, temp, n=12, takeout=0.2):
    """賭けた馬と実際の勝ち馬を突き合わせて漏洩か払戻バグかを確定する。"""
    from app._bet_type_optimizer import backtest_bet_type  # noqa: F401 (副作用 import 安定化)
    from src.constants._results_cols import ResultsCols
    from src.policies._bet_policy import ExpectedValueBetPolicy
    from src.policies._bet_type_params import BetTypeParams
    from src.policies._odds_provider import HistoricalOddsProvider
    from src.policies._score_policy import CURRENT_ODDS, PROB, ExpectedValueScorePolicy

    score_table = ai.calc_score(featured, ExpectedValueScorePolicy)
    op = HistoricalOddsProvider.from_score_table(score_table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=takeout)
    params = BetTypeParams(ev_threshold=ev, temperature=temp, prob_scale=1.0)
    policy = ExpectedValueBetPolicy(op, {bet_type: ev}, bet_types=[bet_type], bet_type_params={bet_type: params})
    actions = policy.judge(score_table)
    win_tbl = rp.preprocessed_data.get(bet_type)
    if win_tbl is not None:
        win_tbl = win_tbl.copy()
        win_tbl.index = win_tbl.index.astype(str)  # index 型を str に正規化（int/str 混在対策）
    win_idx = set(win_tbl.index) if win_tbl is not None else set()

    print("=" * 72)
    print(f"詳細 [{bet_type}] EV={ev} 温度={temp}: 賭け馬 vs 勝ち馬（先頭{n}レース）")
    print(f"  {'race_id':>14} {'賭け馬番':>8} {'勝率':>7} {'オッズ':>8} | {'勝ち馬番':>8} {'払戻/100':>9} {'一致':>5}")
    shown = 0
    for rid, bets in actions.items():
        umas = bets.get(bet_type) or []
        if not umas:
            continue
        rid_s = str(rid)
        try:
            race_rows = score_table.loc[[rid]]
            pm = {int(r[ResultsCols.UMABAN]): (float(r[PROB]), float(r[CURRENT_ODDS])) for _, r in race_rows.iterrows()}
        except Exception:  # noqa: BLE001
            pm = {}
        won, payout = "?", "?"
        if rid_s in win_idx and "win_0" in win_tbl.columns:
            wr = win_tbl.loc[rid_s]
            if hasattr(wr, "to_frame"):  # Series（単一行）
                won, payout = wr.get("win_0", "?"), wr.get("return_0", "?")
            else:  # DataFrame（重複 index）→ 先頭行
                won, payout = wr["win_0"].iloc[0], wr["return_0"].iloc[0]
        for u in umas:
            pr, od = pm.get(int(u), (float("nan"), float("nan")))
            match = "○" if str(won) == str(u) else ""
            print(f"  {rid_s:>14} {u:>8} {pr:>7.3f} {od:>8.1f} | {str(won):>8} {str(payout):>9} {match:>5}")
        shown += 1
        if shown >= n:
            break


def main():
    ap = argparse.ArgumentParser(description="券種別バックテスト指標の診断")
    ap.add_argument("--bet-type", default="tansho")
    ap.add_argument("--test-frac", type=float, default=0.2)
    ap.add_argument("--detail", type=int, default=0, help="温度1.6点の賭け馬vs勝ち馬を N レース表示")
    args = ap.parse_args()

    ai, featured, rp = _load(test_frac=args.test_frac)
    print(f"held-out 区間: {featured.index.nunique()} レース / {len(featured)} 行")
    # 検証済み近傍（健全値が出るはず）と、optimizer が選んだ degenerate 点。
    _probe(ai, featured, rp, args.bet_type, 1.1, 1.0)
    _probe(ai, featured, rp, args.bet_type, 1.5, 1.6)
    if args.detail > 0:
        _detail(ai, featured, rp, args.bet_type, 1.5, 1.6, n=args.detail)


if __name__ == "__main__":
    main()
