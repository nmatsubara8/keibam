"""B群(市場×モデル不一致)の説明的分析＝「市場との差はどこから来るか」（ROIでなく説明可能性）。

研究テーマ転換後の主分析。完全OOSで B群の市場超えエッジは否定された（符号が年で反転）が、不一致集合
自体は年をまたいで安定に出現する。その**発生機構を説明・分類**するのが本スクリプト。

入力: sim_ticket_strategy_roi.py --dump-disagreement で累積した disagreement.csv
  （列: race_id/model_version/year/track/market_fav/market_fav_odds/lgbm_top/lgbm_top_odds/
   market_rank_of_lgbm/lgbm_prob_cal/market_impl_lgbm/prob_diff/odds_diff/winner/
   lgbm_win_payout/market_win_payout/lgbm_hit/market_hit）。

出力（すべて記述統計・採否判定ではない）:
  ① 誰が勝ったか（モデル本命/市場本命/どちらでもない）を年別に。
  ② CSV自己完結の切り口別（市場人気順位/オッズ差/確率差/モデル本命オッズ帯）の
     モデル勝率・市場勝率・単ROI・件数。
  ③ メタ判断の種: 「モデルと市場が割れて片方が勝ったレース」で、どの特徴が『モデルの勝ち』を
     予測するか＝**単変量 AUC を年別**に（sklearn非依存）。両年で AUC>0.55 なら『信じる条件』候補。
  （--featured 指定時のみ）芝ダ/距離/クラス/頭数 別の内訳（列が featured にあれば）。

使い方:
  python scripts/analyze_disagreement.py --csv data/disagreement.csv
  python scripts/analyze_disagreement.py --csv data/disagreement.csv --featured  # 芝ダ等を join
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _roi(payouts):
    """払戻(円/100円賭け)列 → 単勝ROI（払戻合計/(件数×100)）。"""
    n = len(payouts)
    return (sum(payouts) / (n * 100.0)) if n else 0.0


def _summary(df):
    print(f"[サマリ] 不一致 {len(df):,}レース / model_version={sorted(df['model_version'].unique())}")
    for y, g in df.groupby("year"):
        mw = g["lgbm_hit"].mean()
        kw = g["market_hit"].mean()
        neither = 1.0 - mw - kw
        print(f"  {y}: n={len(g):,}  モデル本命勝率={mw:.1%}  市場本命勝率={kw:.1%}  "
              f"どちらでもない={neither:.1%}  モデル単ROI={_roi(g['lgbm_win_payout']):.1%}  "
              f"市場単ROI={_roi(g['market_win_payout']):.1%}")
    print("  → モデル勝率>市場勝率 かつ 年で安定なら『不一致でモデルが正しい』傾向。ROIは控除で別問題。")


def _breakdown(df, col, bins=None, labels=None, title=None):
    import pandas as pd
    print(f"\n[{title or col}別]  {'区分':<14}{'件数':>7}{'モ勝':>7}{'市勝':>7}{'モROI':>8}{'市ROI':>8}")
    if bins is not None:
        key = pd.cut(pd.to_numeric(df[col], errors="coerce"), bins=bins, labels=labels)
    else:
        key = df[col].astype(str)
    for k, g in df.groupby(key, observed=True):
        if not len(g):
            continue
        print(f"  {str(k):<14}{len(g):>7,}{g['lgbm_hit'].mean():>7.1%}{g['market_hit'].mean():>7.1%}"
              f"{_roi(g['lgbm_win_payout']):>8.1%}{_roi(g['market_win_payout']):>8.1%}")


def _meta_auc(df):
    """③ メタ判断の種: 割れて片方が勝ったレースで『モデルの勝ち』を各特徴が予測するか（年別単変量AUC）。"""
    from src.simulation._bet_eval import _auc
    sub = df[(df["lgbm_hit"] == 1) | (df["market_hit"] == 1)].copy()   # どちらかの本命が勝ったレース
    print(f"\n[③メタ判断の種] モデルor市場の本命が勝った {len(sub):,}レースで、"
          "『モデルの勝ち(=lgbm_hit)』を予測する単変量AUC（年別・0.5=無情報）")
    feats = ["prob_diff", "odds_diff", "market_rank_of_lgbm", "lgbm_top_odds", "lgbm_prob_cal"]
    years = sorted(sub["year"].unique())
    print(f"  {'特徴':<20}" + "".join(f"{y:>10}" for y in years))
    import pandas as pd
    for f in feats:
        cells = []
        for y in years:
            g = sub[sub["year"] == y]
            sc = list(zip(pd.to_numeric(g[f], errors="coerce").fillna(0.0),
                          g["lgbm_hit"].astype(int), strict=False))
            a = _auc(sc)
            cells.append(f"{a:.3f}" if a is not None else "  -  ")
        print(f"  {f:<20}" + "".join(f"{c:>10}" for c in cells))
    print("  → 両年で AUC が安定して >0.55 の特徴があれば『その条件のときモデルを信じる』メタルール候補。"
          "全て≒0.5 なら、不一致からモデル/市場どちらが勝つかは事前に判別できない（現状の見込み）。")


def _featured_join(df, featured_path):
    """(任意) featured から race 単位の 芝ダ/距離/クラス/頭数 を join して内訳を出す（列があれば）。"""
    import pandas as pd

    from app._model_eval import load_featured_data
    f = load_featured_data(featured_path) if featured_path else load_featured_data()
    if f is None or f.empty:
        print("\n[featured] 読み込めず（芝ダ/距離等の内訳はスキップ）", file=sys.stderr)
        return
    cand = {c: c for c in f.columns if any(k in str(c) for k in
            ("コース", "course", "距離", "dist", "race_class", "頭数", "n_horses", "going", "芝"))}
    if not cand:
        print("\n[featured] 芝ダ/距離/クラス相当の列が見つからず内訳スキップ", file=sys.stderr)
        return
    race = f.groupby(level=0).first()
    race.index = race.index.astype(str)
    j = df.join(race[list(cand)], on="race_id")
    print(f"\n[featured 内訳] join 列: {list(cand)[:8]}")
    for c in list(cand)[:4]:
        try:
            _breakdown(j.dropna(subset=[c]), c, title=f"featured:{c}")
        except Exception as e:  # noqa: BLE001
            print(f"  {c}: 集計失敗 {e}", file=sys.stderr)


def main() -> int:
    import pandas as pd

    ap = argparse.ArgumentParser(description="B群(市場×モデル不一致)の説明的分析")
    ap.add_argument("--csv", default="data/disagreement.csv")
    ap.add_argument("--featured", nargs="?", const="", default=None,
                    help="featured を join して芝ダ/距離/クラス別も出す（パス省略で既定 featured）")
    args = ap.parse_args()

    if not Path(args.csv).exists():
        print(f"CSV がありません: {args.csv}（先に sim_ticket_strategy_roi --dump-disagreement で生成）",
              file=sys.stderr)
        return 1
    df = pd.read_csv(args.csv, dtype={"race_id": str, "year": str, "model_version": str})
    for c in ("lgbm_hit", "market_hit", "lgbm_win_payout", "market_win_payout"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    print(f"=== 不一致分析（説明可能性・ROIでなく発生機構） {args.csv} ===")
    _summary(df)
    _breakdown(df, "market_rank_of_lgbm", title="市場人気順位(モデル本命の)")
    _breakdown(df, "odds_diff", bins=[-999, -5, -2, 0, 2, 5, 999],
               labels=["≤-5", "-5..-2", "-2..0", "0..2", "2..5", ">5"], title="オッズ差(モ-市)")
    _breakdown(df, "prob_diff", bins=[-1, 0, 0.05, 0.1, 0.2, 1],
               labels=["<0", "0-.05", ".05-.1", ".1-.2", ">.2"], title="確率差(モ-市implied)")
    _breakdown(df, "lgbm_top_odds", bins=[0, 3, 5, 10, 20, 999],
               labels=["≤3", "3-5", "5-10", "10-20", ">20"], title="モデル本命オッズ帯")
    _meta_auc(df)
    if args.featured is not None:
        _featured_join(df, args.featured or None)
    print("\n※ これは記述統計。ここで見つけた条件で買い目を作ると多重探索。条件は事前登録し完全OOSで検証すること。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
