"""漏洩特徴量を特定する（held-out 区間で結果をほぼ完全に当てる列を探す）。

モデルの目的は top3（着順<4＝複勝相当）。勝ち（着順==1）だけで見ると top3 を符号化した
列が AUC~0.7 に埋もれて見逃すため、**勝ち・top3 の両目的**で各数値特徴の単独 AUC を計算し、
どちらかで AUC≈1.0 になる列＝漏洩源を炙り出す。因果に作った集計は ~0.5〜0.8 に収まるはず。

実行:
    python debug_leak.py              # 上位25列
    python debug_leak.py --top 40
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
    ap.add_argument("--era-min", type=int, default=None,
                    help="この年以降のレースだけを対象にする（旧年代の form 再構築リーク監査用）")
    ap.add_argument("--era-max", type=int, default=None,
                    help="この年以前のレースだけを対象にする。--era-min と併用で年代帯を切る")
    args = ap.parse_args()

    import pandas as pd
    from sklearn.metrics import roc_auc_score

    from app._model_compare import recent_race_slice
    from app._model_eval import load_featured_data

    featured = load_featured_data()
    if featured is None:
        print("featured_data がありません")
        return
    if args.era_min is not None or args.era_max is not None:
        # 年代帯を明示指定: その帯の全レースで単独 AUC を測る（リーク列は held-out 無しでも
        # 高 AUC に出る）。form-from-results 再構築が使われる旧年代を直接監査するのに使う。
        yr = pd.to_datetime(featured["date"], errors="coerce").dt.year
        lo = args.era_min if args.era_min is not None else int(yr.min())
        hi = args.era_max if args.era_max is not None else int(yr.max())
        test = featured[(yr >= lo) & (yr <= hi)]
        print(f"era {lo}-{hi}: {test.index.nunique()} レース / {len(test)} 行")
    else:
        test = recent_race_slice(featured, args.test_frac)
        print(f"held-out(直近): {test.index.nunique()} レース / {len(test)} 行")

    # 2 目的: 勝ち(着順==1) と top3(着順<4)。着順が無ければ rank(top3) を使う。
    targets = {}
    if "着順" in test.columns:
        chaku = pd.to_numeric(test["着順"], errors="coerce")
        targets["win"] = (chaku == 1).astype(int)
        targets["top3"] = (chaku < 4).astype(int)
    elif "rank" in test.columns:
        targets["top3"] = pd.to_numeric(test["rank"], errors="coerce").fillna(0).astype(int)
    if not targets:
        print("着順 / rank 列が無く目的変数を作れません")
        return
    for name, y in targets.items():
        print(f"  目的 {name}: 陽性 {int(y.sum())} / {len(y)}")
    print()

    # 目的変数そのもの・ID・日付・オッズ等は監査対象から除外（漏洩の“原料”は犯人扱いしない）。
    exclude = {"着順", "rank", "rank_win", "date", "horse_id", "単勝", "馬番", "枠番"}
    rows = []
    for col in test.columns:
        if col in exclude:
            continue
        s = pd.to_numeric(test[col], errors="coerce")
        if s.notna().sum() < len(s) * 0.5 or s.nunique() < 2:
            continue
        best_auc, best_tgt = 0.0, ""
        for name, y in targets.items():
            mask = s.notna()
            yy = y[mask]
            if yy.nunique() < 2:
                continue
            try:
                auc = roc_auc_score(yy, s[mask])
            except Exception:  # noqa: BLE001
                continue
            auc = max(auc, 1 - auc)  # 単調反転も同等に扱う
            if auc > best_auc:
                best_auc, best_tgt = auc, name
        if best_tgt:
            rows.append((col, best_auc, best_tgt))

    rows.sort(key=lambda x: -x[1])
    print(f"{'特徴量':<40}{'最大単独AUC':>12}{'目的':>7}")
    print("-" * 60)
    for col, auc, tgt in rows[: args.top]:
        flag = "  ← 漏洩疑い" if auc > 0.9 else ("  ← 要確認" if auc > 0.85 else "")
        print(f"{col:<40}{auc:>12.4f}{tgt:>7}{flag}")
    n_leak = sum(1 for _, a, _ in rows if a > 0.9)
    print(f"\n（AUC≈0.5 無情報 / 0.85超=要確認 / 0.9超=漏洩源最有力。"
          f"0.9超は {n_leak} 列 / 全{len(rows)}列中上位{args.top}）")
    print("※ 市場オッズ由来（単勝_log 等）は事前既知なので漏洩ではない。"
          "結果(着順)から作った集計が現在レースを含む場合のみ漏洩。")


if __name__ == "__main__":
    main()
