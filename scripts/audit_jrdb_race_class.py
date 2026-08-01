"""JRDB SED の joken/grade 実コード分布を出し、race_class 変換(JOKEN_TO_CLASS)の取りこぼしを確定する。

featured の race_class 一族(level/one-hot/TE)全滅の直接原因は _adapter.build_raw_race_info の
`joken.map(JOKEN_TO_CLASS)`。この監査は **実データの joken/grade コード分布** と現行マップの
被覆率・未マップコードを出し、コードを推測せず正しく JOKEN_TO_CLASS を拡張＋grade を反映するための
根拠を与える（DB table raw_jrdb_sed を JrdbStore 経由で読む・ネットワーク不要）。

使い方:
  python scripts/audit_jrdb_race_class.py
  python scripts/audit_jrdb_race_class.py --db data/keibam.db
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def summarize_codes(joken_raw, grade_raw, joken_to_class):
    """純関数: joken/grade の値分布と JOKEN_TO_CLASS 被覆を集計して dict を返す（テスト可能）。

    joken は strip→数字なら zfill(2) で正規化してからマップ被覆を測る（adapter と同じ規則）。
    """
    def _norm(v):
        s = str(v).strip()
        return s.zfill(2) if s.isdigit() else s

    norm = [_norm(v) for v in joken_raw if str(v).strip() not in ("", "nan", "None")]
    jc = Counter(norm)
    mapped = {k: v for k, v in jc.items() if k in joken_to_class}
    unmapped = {k: v for k, v in jc.items() if k not in joken_to_class}
    total = sum(jc.values())
    grade = Counter(str(v).strip() for v in grade_raw
                    if str(v).strip() not in ("", "nan", "None"))
    return {
        "n": total,
        "coverage": (sum(mapped.values()) / total) if total else 0.0,
        "mapped": dict(sorted(mapped.items(), key=lambda x: -x[1])),
        "unmapped": dict(sorted(unmapped.items(), key=lambda x: -x[1])),
        "grade": dict(sorted(grade.items(), key=lambda x: -x[1])),
    }


def main() -> int:
    import pandas as pd  # noqa: F401

    from src.jrdb._adapter import JOKEN_TO_CLASS
    from src.jrdb._store import JrdbStore

    ap = argparse.ArgumentParser(description="JRDB SED joken/grade 実コード監査")
    ap.add_argument("--db", default=None, help="keibam.db パス（省略で既定）")
    args = ap.parse_args()

    try:
        sed = JrdbStore(args.db).read("SED")
    except Exception as e:  # noqa: BLE001
        print(f"raw_jrdb_sed を読めません: {e}", file=sys.stderr)
        return 1
    if sed is None or sed.empty:
        print("raw_jrdb_sed が空です（先に JRDB SED を ingest してください）", file=sys.stderr)
        return 2
    if "joken" not in sed.columns:
        print(f"SED に joken 列がありません。列: {list(sed.columns)[:20]}", file=sys.stderr)
        return 3

    s = summarize_codes(sed["joken"], sed.get("grade", []), JOKEN_TO_CLASS)
    print(f"=== JRDB SED race_class 監査  n={s['n']:,}（出走馬行・レース単位でなく行単位）===")
    print(f"\n[joken→race_class 被覆] 現行 JOKEN_TO_CLASS で {s['coverage']:.1%} をマップ")
    print(f"  マップ済 joken コード（正規化後・件数降順）: {s['mapped']}")
    print(f"\n[未マップ joken コード]（これらが race_class を NaN にしている＝マップ拡張の対象）")
    for code, cnt in list(s["unmapped"].items())[:30]:
        print(f"  {code!r:>6}  {cnt:>8,}")
    print(f"\n[grade コード分布]（G1/G2/G3/L 等。現在 race_class 未反映＝grade→class も追加すべき）")
    for code, cnt in list(s["grade"].items())[:15]:
        print(f"  {code!r:>6}  {cnt:>8,}")
    print("\n※ 上の未マップ joken と grade の意味（各コードがどのクラスか）を JRDB コード表で確認し、"
          "JOKEN_TO_CLASS 拡張＋grade→G1/G2/G3 反映を行う（コードは推測せず表で確定）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
