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
    s = pd.to_numeric(df[col], errors="coerce")
    if bins is not None:
        key = pd.cut(s, bins=bins, labels=labels)
    elif s.notna().sum() >= len(df) * 0.5 and s.nunique() > 15:
        # 連続値(距離・馬場長 等)は分位で ~6 区分に丸める（生 groupby で1行/値・空表になるのを防ぐ）。
        key = pd.qcut(s, q=min(6, s.nunique()), duplicates="drop")
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
    """(任意) featured の race 単位 数値特徴(頭数/距離/コース/クラス等)を join。返す:(df, 数値列名)。"""
    import pandas as pd

    from app._model_eval import load_featured_data
    f = load_featured_data(featured_path) if featured_path else load_featured_data()
    if f is None or f.empty:
        print("\n[featured] 読み込めず（race-context の効果量/内訳はスキップ）", file=sys.stderr)
        return df, []
    cand = [c for c in f.columns if any(k in str(c) for k in
            ("コース", "course", "距離", "dist", "race_class", "頭数", "n_horses", "going", "芝",
             "class_level"))]
    if not cand:
        return df, []
    race = f.groupby(level=0).first()
    race.index = race.index.astype(str)
    j = df.join(race[cand], on="race_id")
    num_cols = [c for c in cand if pd.to_numeric(j[c], errors="coerce").notna().sum() >= len(j) * 0.5]
    print(f"\n[featured join] race-context 数値列 {len(num_cols)}: {num_cols[:8]}")
    return j, num_cols


def _cohens_d(a, b):
    """Cohen's d（標準化平均差）。|d|<0.2小 / 0.5中 / 0.8大。"""
    import numpy as np
    a = np.asarray(a, float); a = a[np.isfinite(a)]
    b = np.asarray(b, float); b = b[np.isfinite(b)]
    if len(a) < 2 or len(b) < 2:
        return None
    sp = (((len(a) - 1) * a.std(ddof=1) ** 2 + (len(b) - 1) * b.std(ddof=1) ** 2)
          / (len(a) + len(b) - 2)) ** 0.5
    return float((a.mean() - b.mean()) / sp) if sp > 0 else 0.0


def _cliffs_delta(a, b):
    """Cliff's delta（順位ベース・非正規頑健）= 2·AUC−1。|δ|<0.147小 / 0.33中 / 0.474大。"""
    from src.simulation._bet_eval import _auc
    scores = [(float(x), 1) for x in a if x == x] + [(float(x), 0) for x in b if x == x]
    auc = _auc(scores)
    return (2 * auc - 1) if auc is not None else None


def _mag(delta):
    """Cliff's delta の大きさラベル。"""
    if delta is None:
        return "-"
    ad = abs(delta)
    return "大" if ad >= 0.474 else "中" if ad >= 0.33 else "小" if ad >= 0.147 else "無視"


def _effect_sizes(df, feats):
    """B群内『モデルが勝ったレース vs 市場が勝ったレース』の二群を各特徴の効果量で比較（記述）。

    片方の本命だけが勝ったレース(lgbm_hit XOR market_hit)を対象に、A=モデル勝ち/B=市場勝ち。
    Cohen's d と Cliff's delta を出し、|δ| 降順で『両群を最も分ける特徴』を示す。全て小なら
    『現特徴ではモデルが勝つ場面を事前識別できない』(AUC≈0.5 を別角度で裏付け)。大きい特徴があれば
    事前登録して完全OOSで検証すべき仮説。※有意性検定はしない（記述＝多重探索を避ける）。
    """
    import pandas as pd
    sub = df[(pd.to_numeric(df["lgbm_hit"], errors="coerce") == 1)
             ^ (pd.to_numeric(df["market_hit"], errors="coerce") == 1)].copy()
    A = sub[sub["lgbm_hit"] == 1]
    B = sub[sub["market_hit"] == 1]
    print(f"\n[効果量: モデル勝ち {len(A):,} vs 市場勝ち {len(B):,}]（片方の本命が勝った{len(sub):,}レース）")
    print(f"  {'特徴':<22}{'Cohen d':>9}{'Cliff δ':>9}{'大きさ':>7}  (Aμ/Bμ)")
    rows = []
    for f in feats:
        a = pd.to_numeric(A[f], errors="coerce")
        b = pd.to_numeric(B[f], errors="coerce")
        d = _cohens_d(a, b)
        cd = _cliffs_delta(a.dropna().tolist(), b.dropna().tolist())
        rows.append((f, d, cd, a.mean(), b.mean()))
    for f, d, cd, am, bm in sorted(rows, key=lambda r: -(abs(r[2]) if r[2] is not None else -1)):
        ds = f"{d:+.3f}" if d is not None else "  -  "
        cds = f"{cd:+.3f}" if cd is not None else "  -  "
        print(f"  {f:<22}{ds:>9}{cds:>9}{_mag(cd):>7}  ({am:.2f}/{bm:.2f})")
    big = [r for r in rows if r[2] is not None and abs(r[2]) >= 0.33]
    if big:
        print(f"  → |Cliff δ|≥0.33(中以上)の特徴あり: {[r[0] for r in big]}。"
              "これは事前登録して完全OOSで検証すべき仮説（今ここで買い目化しない）。")
    else:
        print("  → 全特徴 |Cliff δ|<0.33（小/無視）。現特徴ではモデルが市場に勝つ場面を事前識別できない"
              "＝③のAUC≒0.5 を別角度で裏付け（不一致は現情報で分離不能）。")


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
    print("※ 主要指標は 勝率差（どちらの本命が勝ったか）。ROIはオッズ由来の分散が大きく年で不安定。")
    csv_feats = ["prob_diff", "odds_diff", "market_rank_of_lgbm", "lgbm_top_odds", "lgbm_prob_cal"]
    race_cols = []
    if args.featured is not None:
        df, race_cols = _featured_join(df, args.featured or None)
    _summary(df)
    _breakdown(df, "market_rank_of_lgbm", title="市場人気順位(モデル本命の)")
    _breakdown(df, "odds_diff", bins=[-999, -5, -2, 0, 2, 5, 999],
               labels=["≤-5", "-5..-2", "-2..0", "0..2", "2..5", ">5"], title="オッズ差(モ-市)")
    _breakdown(df, "lgbm_top_odds", bins=[0, 3, 5, 10, 20, 999],
               labels=["≤3", "3-5", "5-10", "10-20", ">20"], title="モデル本命オッズ帯")
    for c in race_cols[:4]:
        _breakdown(df, c, title=f"featured:{c}")
    feats = csv_feats + race_cols
    _meta_auc(df)                    # ③ 予測可能性（単変量AUC・年別）
    _effect_sizes(df, feats)         # 二群(モデル勝ち vs 市場勝ち)の効果量（記述・分離度）
    print("\n※ これは記述統計。ここで見つけた条件で買い目を作ると多重探索。条件は事前登録し完全OOSで検証すること。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
