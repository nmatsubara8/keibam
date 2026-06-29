"""漏洩している特徴量を特定する（held-out 区間で「勝ち馬を単独でほぼ完全に当てる」列を探す）。

モデルの1位指名が held-out で97.6%的中＝結果が特徴量に漏れている。各数値特徴について
「その列だけで勝ち（着順==1）を予測したときの AUC」を計算し、AUC≈1.0 の列＝漏洩源を炙り出す。
因果に作った集計は AUC~0.5〜0.7 に収まるはず。1.0 近傍の列が犯人。

実行:
    python debug_leak.py
    python debug_leak.py --top 30
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--test-frac", type=float, default=0.2)
    args = ap.parse_args()

    import pandas as pd
    from sklearn.metrics import roc_auc_score

    from app._model_compare import recent_race_slice
    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None:
        print("featured_data がありません")
        return
    test = recent_race_slice(featured, args.test_frac)
    print(f"held-out: {test.index.nunique()} レース / {len(test)} 行")

    # 目的変数: 着順==1（単勝の勝ち）。無ければ rank(top3) で代用。
    if "着順" in test.columns:
        y = (pd.to_numeric(test["着順"], errors="coerce") == 1).astype(int)
        tgt = "着順==1"
    elif "rank" in test.columns:
        y = pd.to_numeric(test["rank"], errors="coerce").fillna(0).astype(int)
        tgt = "rank(top3)"
    else:
        print("着順 / rank 列が無く目的変数を作れません")
        return
    print(f"目的変数: {tgt}（勝ち {int(y.sum())} / {len(y)} 行）\n")

    # 目的変数そのもの・ID・日付などは除外（漏洩の“原料”を犯人扱いしない）。
    exclude = {"着順", "rank", "date", "horse_id", "単勝", "馬番", "枠番"}
    results = []
    for col in test.columns:
        if col in exclude:
            continue
        s = pd.to_numeric(test[col], errors="coerce")
        if s.notna().sum() < len(s) * 0.5 or s.nunique() < 2:
            continue
        mask = s.notna()
        yy = y[mask]
        if yy.nunique() < 2:
            continue
        try:
            auc = roc_auc_score(yy, s[mask])
        except Exception:  # noqa: BLE001
            continue
        results.append((col, max(auc, 1 - auc)))  # 単調反転も同等に扱う

    results.sort(key=lambda x: -x[1])
    print(f"{'特徴量':<40}{'単独AUC(勝ち予測)':>16}")
    print("-" * 56)
    for col, auc in results[: args.top]:
        flag = "  ← 漏洩疑い" if auc > 0.9 else ("  ← 要確認" if auc > 0.8 else "")
        print(f"{col:<40}{auc:>16.4f}{flag}")
    print(f"\n（AUC≈0.5 が無情報、≈1.0 が結果を符号化。0.9超の列が漏洩源の最有力。全{len(results)}列中上位{args.top}）")


if __name__ == "__main__":
    main()
