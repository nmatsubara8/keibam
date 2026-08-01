"""B群(市場×モデル不一致)の説明的分析＝「市場との差はどこから来るか」（ROIでなく説明可能性）。

研究テーマ転換後の主分析。完全OOSで B群の市場超えエッジは否定された（符号が年で反転）が、不一致集合
自体は年をまたいで安定に出現する。その**発生機構を説明・分類**するのが本スクリプト。

入力: sim_ticket_strategy_roi.py --dump-disagreement で累積した disagreement.csv
  （列: race_id/model_version/year/track/market_fav/market_fav_odds/lgbm_top/lgbm_top_odds/
   market_rank_of_lgbm/lgbm_prob_cal/market_impl_lgbm/prob_diff/odds_diff/winner/
   lgbm_win_payout/market_win_payout/lgbm_hit/market_hit）。

出力（記述統計中心。ただし勝率差のみ常設 paired 検定）:
  ① 誰が勝ったか（モデル本命/市場本命/どちらでもない）を年別に。
  ①' 常設検定: 片方の本命が勝った決着レースでのモデル勝率の Wilson 95%CI（年別・通算。0.5基準）。
  ② CSV自己完結の切り口別（市場人気順位/オッズ差/確率差/モデル本命オッズ帯）の
     モデル勝率・市場勝率・単ROI・件数。
  ③ メタ判断の種: 「モデルと市場が割れて片方が勝ったレース」で、どの特徴が『モデルの勝ち』を
     予測するか＝**単変量 AUC を年別**に（sklearn非依存）。両年で AUC>0.55 なら『信じる条件』候補。
  （--featured 指定時のみ）**二頭差分**: LGBM本命と市場本命を (race_id, 馬番) で各々引き当て、
     馬固有特徴の `lgbm − market` 差分列 d_* を作る（旧 groupby.first() が無関係な先頭馬の値を
     付けていたバグの修正）。距離/クラス/頭数 等レース内一定の列は文脈として実値で内訳表示。
     クラスは疎な one-hot でなく ordinal `race_class_level` を採用（“race_class 全ゼロ”の是正）。

使い方:
  python scripts/analyze_disagreement.py --csv data/disagreement.csv
  python scripts/analyze_disagreement.py --csv data/disagreement.csv --featured  # 二頭差分＋文脈内訳
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


def _wilson(k, n, z=1.96):
    """二項割合の Wilson 95%CI（正規近似より小標本で頑健）。返す (p, lo, hi)。"""
    if n <= 0:
        return 0.0, 0.0, 0.0
    p = k / n
    denom = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denom
    half = z * ((p * (1 - p) / n + z * z / (4 * n * n)) ** 0.5) / denom
    return p, max(0.0, center - half), min(1.0, center + half)


def _winrate_test(df):
    """[常設 paired 検定] 片方の本命が勝った決着レースで『モデル勝率』の Wilson 95%CI を出す。

    不一致レースでは勝ち馬は高々一方の本命（同着以外は排他）。決着レース n のうち
    モデル本命の勝ち k を割合とし、95%CI が 0.5 を跨ぐか（＝市場より有意に上か）を年別・通算で判定。
    ここは記述でなく検定（買い目化ではないので多重探索にならない・単一の事前指標）。
    """
    import pandas as pd
    lh = pd.to_numeric(df["lgbm_hit"], errors="coerce").fillna(0).astype(int)
    mh = pd.to_numeric(df["market_hit"], errors="coerce").fillna(0).astype(int)
    dec = df[(lh == 1) ^ (mh == 1)].copy()   # 片方の本命だけが勝った決着レース
    print(f"\n[常設検定] 片方の本命が勝った {len(dec):,}決着レースでのモデル勝率（Wilson 95%CI・0.5基準）")
    print(f"  {'年':<8}{'決着n':>7}{'モ勝k':>7}{'モ勝率':>8}{'95%CI':>18}{'判定':>10}")

    def _row(label, g):
        n = len(g)
        k = int(pd.to_numeric(g["lgbm_hit"], errors="coerce").fillna(0).sum())
        p, lo, hi = _wilson(k, n)
        verdict = "市場超↑" if lo > 0.5 else "市場未満↓" if hi < 0.5 else "判別不能"
        print(f"  {label:<8}{n:>7,}{k:>7,}{p:>8.1%}[{lo:>5.1%},{hi:>5.1%}]{verdict:>10}")

    for y, g in dec.groupby("year"):
        _row(str(y), g)
    _row("通算", dec)
    print("  → 通算CIが0.5超なら『不一致時にモデル本命の方が勝ちやすい』を年跨ぎで支持（ROIは控除で別問題）。"
          "各年CIが0を跨ぐなら年単体では確言できない。")


def _meta_auc(df, extra_feats=()):
    """③ メタ判断の種: 割れて片方が勝ったレースで『モデルの勝ち』を各特徴が予測するか（年別単変量AUC）。"""
    from src.simulation._bet_eval import _auc
    sub = df[(df["lgbm_hit"] == 1) | (df["market_hit"] == 1)].copy()   # どちらかの本命が勝ったレース
    print(f"\n[③メタ判断の種] モデルor市場の本命が勝った {len(sub):,}レースで、"
          "『モデルの勝ち(=lgbm_hit)』を予測する単変量AUC（年別・0.5=無情報）")
    feats = ["prob_diff", "odds_diff", "market_rank_of_lgbm", "lgbm_top_odds", "lgbm_prob_cal",
             *extra_feats]
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


# 順序値クラス（格の大小 1..9）。疎な one-hot ダミー race_class__<cat> は個別にほぼ全ゼロで
# 無情報のため除外し、この連続軸を「クラス」文脈として使う（“race_class 全ゼロ”問題の原因＝
# ダミー列を掴んでいたこと。ordinal を明示採用する）。
_CTX_KEEP = {"race_class_level"}
# レース内でほぼ全ゼロになる疎な one-hot 群（個別ダミーは分析に使わない）。
_SPARSE_ONEHOT = ("race_class_", "race_type_", "ground_state", "weather_", "性_",
                  "around_", "place_", "コース_", "course_")
_DIFF_DROP = {"rank", "着順", "date", "horse_id"}


def _horse_feature_cols(feat, uma_col):
    """featured から二頭差分に使う数値/真偽の特徴列を選ぶ（疎 one-hot は除外、ordinal は残す）。"""
    import numpy as np
    out = []
    for c in feat.select_dtypes(include=[np.number, "bool"]).columns:
        cs = str(c)
        if c == uma_col or cs in _DIFF_DROP:
            continue
        if cs in _CTX_KEEP:
            out.append(c)
            continue
        if any(b in cs for b in _SPARSE_ONEHOT):
            continue
        out.append(c)
    return out


def _two_horse_diff(df, feat, uma_col, *, tol=1e-9):
    """二頭(LGBM本命/市場本命)を (race_id, 馬番) で引き当て、race文脈列と二頭差分列を作る。

    馬固有特徴(騎手/厩舎/生産者勝率・斤量・馬体重・年齢等)は本命ごとに値が違うので、
    `(race_id, LGBM馬番)` と `(race_id, 市場馬番)` の二系統で引いて `lgbm − market` を差分列
    `d_<feat>` にする（＝旧 groupby.first() が“先頭馬”の無関係値を付けていたバグの修正）。
    レース内で一定の列(距離/クラス/頭数 等)は差分が0になるので文脈列として実値を残す。

    返す: (df_joined, race_cols, diff_cols)。
    """
    import numpy as np
    import pandas as pd

    feat = feat.copy()
    feat.index = feat.index.astype(str)
    if uma_col not in feat.columns:
        print(f"  [featured] 馬番列 {uma_col!r} なし → 二頭差分はスキップ", file=sys.stderr)
        return df, [], []
    cols = _horse_feature_cols(feat, uma_col)
    if not cols:
        return df, [], []

    def _key(rid_str, uma):
        u = pd.to_numeric(uma, errors="coerce").astype("Int64").astype(str)
        return np.asarray(rid_str, dtype=object) + "#" + np.asarray(u, dtype=object)

    lut = feat[cols].astype(float, errors="ignore")
    lut.index = _key(feat.index.astype(str), feat[uma_col])
    lut = lut[~lut.index.duplicated(keep="first")]

    rid = df["race_id"].astype(str)
    L = lut.reindex(_key(rid, df["lgbm_top"])); L.index = df.index
    M = lut.reindex(_key(rid, df["market_fav"])); M.index = df.index
    matched = L.notna().any(axis=1) & M.notna().any(axis=1)
    n_both = int(matched.sum())
    diff = L.astype(float) - M.astype(float)
    nz = (diff[matched].abs() > tol).mean() if n_both else pd.Series(0.0, index=cols)

    out = df.copy()
    race_cols, diff_cols = [], []
    for c in cols:
        frac = float(nz.get(c, 0.0))
        if frac < 1e-4:                    # レース内一定＝文脈（距離/クラス/頭数 等）
            out[c] = L[c]                  # 実値（どちらの本命でも同じ）
            race_cols.append(c)
        else:                              # 馬ごとに違う＝二頭差分（本命の情報差）
            out[f"d_{c}"] = diff[c]
            diff_cols.append(f"d_{c}")
    dead = [c for c in race_cols if out[c].nunique(dropna=True) <= 1]
    if dead:
        print(f"  [警告] 文脈列が全レースで単一値＝特徴が取得できていない疑い: {dead}", file=sys.stderr)
    print(f"  [二頭差分] {n_both:,}/{len(df):,} レースで両本命を featured に照合 → "
          f"文脈列 {len(race_cols)} / 差分列 {len(diff_cols)}")
    return out, race_cols, diff_cols


def _featured_join(df, featured_path):
    """featured を二頭(LGBM/市場)で結合。返す:(df, race_cols, diff_cols)。"""
    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols
    f = load_featured_data(featured_path) if featured_path else load_featured_data()
    if f is None or f.empty:
        print("\n[featured] 読み込めず（race-context/二頭差分はスキップ）", file=sys.stderr)
        return df, [], []
    return _two_horse_diff(df, f, ResultsCols.UMABAN)


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
    race_cols, diff_cols = [], []
    if args.featured is not None:
        df, race_cols, diff_cols = _featured_join(df, args.featured or None)
    _summary(df)
    _winrate_test(df)                # 常設 paired 検定（Wilson CI・勝率差）
    _breakdown(df, "market_rank_of_lgbm", title="市場人気順位(モデル本命の)")
    _breakdown(df, "odds_diff", bins=[-999, -5, -2, 0, 2, 5, 999],
               labels=["≤-5", "-5..-2", "-2..0", "0..2", "2..5", ">5"], title="オッズ差(モ-市)")
    _breakdown(df, "lgbm_top_odds", bins=[0, 3, 5, 10, 20, 999],
               labels=["≤3", "3-5", "5-10", "10-20", ">20"], title="モデル本命オッズ帯")
    for c in race_cols[:4]:
        _breakdown(df, c, title=f"featured:{c}")
    _meta_auc(df, extra_feats=diff_cols)      # ③ 予測可能性（単変量AUC・年別／二頭差分含む）
    # 効果量は CSV由来スカラ＋馬単位の二頭差分列(d_*)で。race文脈列は差分0のため除外。
    _effect_sizes(df, csv_feats + diff_cols)  # 二群(モデル勝ち vs 市場勝ち)の効果量（記述・分離度）
    print("\n※ これは記述統計。ここで見つけた条件で買い目を作ると多重探索。条件は事前登録し完全OOSで検証すること。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
