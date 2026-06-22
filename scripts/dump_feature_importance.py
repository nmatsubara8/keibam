#!/usr/bin/env python
"""保存済みモデル（models/<yyyymmdd>/<version>.pickle）から LightGBM の
feature importance を抽出して表示・CSV 出力する。

retrain 直後の効き確認に使う。GBDT は非標準化ストリームで学習しているため、
importance はそのまま生特徴の寄与として読める。

使い方:
    python scripts/dump_feature_importance.py            # 最新モデルを自動選択
    python scripts/dump_feature_importance.py --top 40
    python scripts/dump_feature_importance.py --model models/20260623/v1.pickle
    python scripts/dump_feature_importance.py --grep _py_   # 名前で絞り込み表示

新特徴グループ（dist_change_ratio / opponent strength / damsire / yoso / person_yearly）の
合計寄与もまとめて出す。
"""

from __future__ import annotations

import argparse
import glob
import os
import sys


# 新規追加した特徴グループ → 接頭辞/キーワード（合計寄与の集計用）
FEATURE_GROUPS: dict[str, tuple[str, ...]] = {
    "prev_race": ("dist_change", "kinryo_delta", "jockey_change"),
    "opponent_strength": ("faced_grade", "faced_graded"),
    "sire": ("sire_",),
    "damsire": ("damsire_",),
    "yoso": ("yoso_", "n_marks", "n_honmei", "score_sum", "score_mean"),
    "person_yearly": ("jockey_py_", "trainer_py_", "owner_py_", "breeder_py_"),
}


def _latest_model() -> str | None:
    cands = glob.glob(os.path.join("models", "*", "*.pickle"))
    if not cands:
        return None
    return max(cands, key=os.path.getmtime)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--model", default=None, help="モデル pickle パス（省略時は最新を自動選択）")
    ap.add_argument("--top", type=int, default=30, help="上位 N 件を表示")
    ap.add_argument("--grep", default=None, help="特徴名にこの文字列を含むものだけ表示")
    ap.add_argument("--csv", default=None, help="全 importance を CSV 出力するパス")
    args = ap.parse_args()

    from src.training._keiba_ai_factory import KeibaAIFactory

    path = args.model or _latest_model()
    if path is None or not os.path.exists(path):
        print("モデル pickle が見つかりません（先に retrain を実行してください）", file=sys.stderr)
        return 1
    print(f"model: {path}")

    ai = KeibaAIFactory.load(path)
    fi = ai.feature_importance(num_features=10**9)  # 全件取得
    if fi is None or len(fi) == 0:
        print("feature_importance が空です（LightGBM base が無効？）", file=sys.stderr)
        return 1

    fi = fi.reset_index(drop=True)
    total = float(fi["importance"].sum()) or 1.0
    fi["share_%"] = fi["importance"] / total * 100.0

    if args.csv:
        fi.to_csv(args.csv, index=False)
        print(f"CSV 出力: {args.csv}  ({len(fi)} features)")

    # ── 上位 N ──
    view = fi
    if args.grep:
        view = fi[fi["features"].str.contains(args.grep, regex=False)]
    print(f"\n== top {args.top}{f' (grep={args.grep!r})' if args.grep else ''} ==")
    for _, r in view.head(args.top).iterrows():
        print(f"  {r['importance']:>8.0f}  {r['share_%']:>5.2f}%  {r['features']}")

    # ── 新特徴グループの合計寄与 ──
    print("\n== 新特徴グループ別 合計寄与 ==")
    names = fi["features"].astype(str)
    rows = []
    for grp, keys in FEATURE_GROUPS.items():
        mask = names.apply(lambda n, _k=keys: any(k in n for k in _k))
        sub = fi[mask]
        rows.append((grp, len(sub), float(sub["importance"].sum()), float(sub["share_%"].sum())))
    for grp, ncols, imp, share in sorted(rows, key=lambda x: x[3], reverse=True):
        print(f"  {grp:18s}: {ncols:3d}列  importance合計 {imp:>8.0f}  share {share:>5.2f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
