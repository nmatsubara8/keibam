"""JRDB 特徴の edge 源泉監査＝I(X;勝敗) と I(X;勝敗|市場) を全特徴で出し、市場に無い情報を測る。

Residual Learning の運用: 特徴が勝敗に持つ情報のうち、市場(単勝オッズの含意勝率)を条件づけた後に
残る分(CMI)だけが edge の源泉。CMI 降順に並べ、市場と重複するだけの特徴(高 MI・低 edge_ratio)と、
市場に無い情報を持つ特徴(高 CMI)を区別する。特徴が「在るか」でなく「情報を持つか」を見る監査。

  Y = 勝ち(着順==1) / M = 市場含意勝率(1/単勝, レース内正規化の分位) / X = 各数値特徴
  I(X;Y)   生情報、  I(X;Y|M) 市場条件後に残る情報、 edge_ratio=CMI/MI(1に近い＝市場直交)

使い方:
  python scripts/audit_feature_information.py
  python scripts/audit_feature_information.py --top 40 --sample 200000
※ plug-in 推定は小標本で上方バイアス。件数の多い特徴の相対比較として読む（絶対値の過信は避ける）。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# 目的変数・結果由来・ID・オッズは説明変数から除外（リーク/自明）。
_EXCLUDE_SUBSTR = ("着順", "rank_win", "rank_place", "着差", "オッズ", "odds", "impl",
                   "payout", "return", "haitou", "horse_id", "race_id", "date")
_EXCLUDE_EXACT = {"着順", "馬番", "単勝", "rank", "date", "horse_id"}


def _feature_cols(df):
    import numpy as np
    out = []
    for c in df.select_dtypes(include=[np.number, "bool"]).columns:
        cs = str(c)
        if c in _EXCLUDE_EXACT or any(s in cs for s in _EXCLUDE_SUBSTR):
            continue
        out.append(c)
    return out


def main() -> int:
    import numpy as np
    import pandas as pd

    from app._model_eval import load_featured_data
    from src.constants._results_cols import ResultsCols
    from src.simulation._information import edge_decomposition

    ap = argparse.ArgumentParser(description="特徴の edge 源泉監査（条件付き相互情報量）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--top", type=int, default=30)
    ap.add_argument("--sample", type=int, default=0, help=">0 で行サンプリング（高速化）")
    ap.add_argument("--x-bins", type=int, default=5)
    ap.add_argument("--m-bins", type=int, default=5)
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行してください）", file=sys.stderr)
        return 2
    rank = pd.to_numeric(feat.get(ResultsCols.RANK), errors="coerce")
    odds = pd.to_numeric(feat.get(ResultsCols.TANSHO_ODDS), errors="coerce")
    if rank is None or odds is None or rank.isna().all() or odds.isna().all():
        print("着順/単勝 列が featured にありません（Y/市場 を作れない）", file=sys.stderr)
        return 3
    y = (rank == 1).astype(float).to_numpy()
    m = (1.0 / odds.where(odds > 0)).to_numpy()          # 市場含意（未正規化でも分位ビンで可）

    df = feat
    if args.sample and len(df) > args.sample:
        df = df.sample(args.sample, random_state=0)
        y = (pd.to_numeric(df[ResultsCols.RANK], errors="coerce") == 1).astype(float).to_numpy()
        o = pd.to_numeric(df[ResultsCols.TANSHO_ODDS], errors="coerce")
        m = (1.0 / o.where(o > 0)).to_numpy()

    cols = _feature_cols(df)
    print(f"=== 特徴 edge 源泉監査  n={len(df):,} / 特徴 {len(cols)} / 勝率(base)={y.mean():.3f} ===")
    print("  I(X;Y)=生情報  CMI=I(X;Y|市場)=市場後に残る情報  edge_ratio=CMI/MI（1=市場直交）  bit単位")
    rows = []
    for c in cols:
        x = pd.to_numeric(df[c], errors="coerce").to_numpy()
        if np.isfinite(x).sum() < 100:
            continue
        d = edge_decomposition(x, y, m, x_bins=args.x_bins, m_bins=args.m_bins)
        rows.append((c, d["mi"], d["cmi"], d["redundant"], d["edge_ratio"]))

    rows.sort(key=lambda r: -r[2])            # CMI 降順＝edge 源泉順
    print(f"\n[edge 源泉 上位{args.top}]（CMI 降順＝市場に無い情報が多い順）")
    print(f"  {'特徴':<28}{'I(X;Y)':>9}{'CMI':>9}{'冗長':>9}{'edge率':>8}")
    for c, mi, cmi, red, er in rows[:args.top]:
        print(f"  {c:<28}{mi:>9.4f}{cmi:>9.4f}{red:>9.4f}{er:>8.1%}")

    redundant = sorted(rows, key=lambda r: -(r[3]))[:12]
    print("\n[市場と重複が大きい特徴]（高 I(X;Y) だが CMI が小＝市場が既に価格化済み）")
    print(f"  {'特徴':<28}{'I(X;Y)':>9}{'CMI':>9}{'edge率':>8}")
    for c, mi, cmi, red, er in redundant:
        print(f"  {c:<28}{mi:>9.4f}{cmi:>9.4f}{er:>8.1%}")

    print("\n※ CMI が高い特徴＝市場が未反映の情報を持つ edge 候補。事前登録し完全OOSで検証すること。"
          "plug-in 推定は小標本で上方バイアスのため、絶対値でなく相対順位で読む。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
