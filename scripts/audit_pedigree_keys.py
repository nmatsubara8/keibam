"""sire/damsire（父・母父）の実在性・品質監査（TE_ENCODE 凍結条件3・読み取りのみ・要ローカル）。

docs/target_encoding_design.md の凍結条件3を満たすための監査。値の中身は判定に使わず、**キーとしての
妥当性**（列名・非欠損・unique・年別非欠損・ID の時系列安定性・正規キーが名称か数値ID か）だけを出す。
存在しなければ UKC materialization を別作業で先に実施する判断材料にする。featured を書き換えない。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# 父/母父を指す可能性のある列名の手がかり（UKC/netkeiba 由来の揺れを広く拾う）。
_KEYS = ("sire", "damsire", "father", "bms", "broodmare", "f_blood", "m_father",
         "父", "母父", "種牡馬", "血統")


def main() -> int:
    import numpy as np
    import pandas as pd

    ap = argparse.ArgumentParser(description="sire/damsire キー品質監査（読み取りのみ）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl")
    args = ap.parse_args()

    p = Path(args.featured)
    if not p.exists():
        print(f"[エラー] featured が無い: {p}（jrdb_build_features.py --from-store --with-myspeed で生成）",
              file=sys.stderr)
        return 2
    df = pd.read_pickle(p)
    print("=" * 92)
    print(f"sire/damsire キー品質監査（{p}・rows={len(df):,}・読み取りのみ）")

    cand = [c for c in df.columns if any(k in str(c).lower() or k in str(c) for k in _KEYS)]
    if not cand:
        print("  [結果] 父/母父らしき列は featured に**無い**＝UKC materialization を別作業で先に実施が必要。")
        print(f"  （探索キー: {list(_KEYS)}）")
        return 0

    rid = pd.Series(df.index.astype(str))
    year = pd.to_numeric(rid.str[:4], errors="coerce")
    horse = df["horse_id"] if "horse_id" in df.columns else None
    print(f"  候補列 {len(cand)}: {cand}")
    print(f"\n  {'列':<22}{'dtype':>10}{'非欠損':>8}{'unique':>9}{'馬内不変率':>10}{'正規キー':>8}")
    for c in cand:
        s = df[c]
        nonnull = float(s.notna().mean())
        nun = int(s.nunique(dropna=True))
        # ID の時系列安定性: 同一 horse_id 内で値が1種類か（父/母父は生涯不変が正）
        stable = float("nan")
        if horse is not None:
            per = s.groupby(horse.to_numpy()).nunique(dropna=True)
            stable = float((per <= 1).mean()) if len(per) else float("nan")
        # 正規キー推定: 数値化できる割合が高ければ ID、低ければ 名称(文字列)
        num_frac = float(pd.to_numeric(s, errors="coerce").notna().mean())
        canon = "数値ID" if num_frac >= 0.9 else ("名称" if num_frac <= 0.1 else "混在")
        st = f"{stable:.3f}" if stable == stable else "n/a"
        print(f"  {str(c):<22}{str(s.dtype):>10}{nonnull:>8.3f}{nun:>9,}{st:>10}{canon:>8}")

    # 年別非欠損（取込断絶の検知）
    print("\n  [年別 非欠損率]")
    for c in cand:
        s = pd.to_numeric(df[c], errors="coerce").notna() if df[c].dtype != object else df[c].notna()
        by = s.groupby(year.to_numpy()).mean()
        cells = "  ".join(f"{int(y)}:{v:.2f}" for y, v in by.items() if y == y)
        print(f"    {str(c):<22} {cells}")

    print("\n[判定材料] TE キーには (1)非欠損が高い (2)馬内不変率≈1（生涯不変） (3)正規キーが数値ID"
          "\n  （名称は表記揺れ・同名別馬のリスク）が望ましい。母父の非欠損が父より大きく低い場合は"
          "\n  primary bundle を凍結前に改訂（都合よく外さない）。不足なら UKC materialization を別作業で先に。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
