"""JRDB TARGET ダウンロードデータ 全8種を取り込み、種別ごとに正規化 pickle へ保存する（STEP5）。

ディレクトリ内の zip/lzh を種別（zip 名先頭英字）で自動分類し、`src.jrdb._target` で
featured 結合可能な正規化 DataFrame にして `--out-dir` へ `jrdb_target_<種別>.pkl` 保存する。
外厩に絞らず 8 種すべてを対象にする。

使い方:
  python scripts/jrdb_target_ingest.py --src /mnt/c/Users/Ayaka/Downloads --out-dir data/jrdb_target
  python scripts/jrdb_target_ingest.py --src data/jrdb_target_dl   # 複数日/年のパックをまとめて

出力（各種別を全ファイル横断で結合）:
  jrdb_target_gaikyu.pkl / _itidori / _bante / _idm       [race_id, ketto, horse_id, <code>]
  jrdb_target_gaikyucomment.pkl  [race_id, umaban, gaikyu_name, kikyu_date, interval_weeks, interval_raw]
  jrdb_target_idmse.pkl          [race_id, umaban, race_date, seiseki_idm]
  jrdb_target_tnrank/_jocrank.pkl [area, person_code, rank, name, kind]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd  # noqa: E402

from src.jrdb._target import TARGET_TYPES, classify, parse_target_bytes  # noqa: E402

_ARCHIVE_SUFFIXES = {".zip", ".lzh"}


def main(argv=None) -> int:
    from src.jrdb._extract import read_jrdb_bytes

    ap = argparse.ArgumentParser(description="JRDB TARGET 全8種を取り込み正規化 pickle へ")
    ap.add_argument("--src", required=True, help="zip/lzh を置いたディレクトリ（複数日/年のパック可）")
    ap.add_argument("--out-dir", default="data/jrdb_target", help="正規化 pickle の出力先")
    ap.add_argument("--types", nargs="+", default=None,
                    help=f"取り込む種別を限定（既定=全8種）。選択肢: {' '.join(TARGET_TYPES)}")
    args = ap.parse_args(argv)

    src = Path(args.src)
    if not src.exists():
        print(f"src がありません: {src}", file=sys.stderr)
        return 1
    archives = sorted(p for p in src.rglob("*") if p.suffix.lower() in _ARCHIVE_SUFFIXES)
    if not archives:
        print(f"zip/lzh が見つかりません: {src}", file=sys.stderr)
        return 1

    want = set(args.types) if args.types else set(TARGET_TYPES)
    # 種別ごとに (内包名, バイト列) を貯めてから一括パース（日/年をまたいで結合）。
    buckets: dict[str, list] = {t: [] for t in TARGET_TYPES}
    n_files: dict[str, int] = {t: 0 for t in TARGET_TYPES}
    for a in archives:
        t = classify(a.name)
        if t is None or t not in want:
            continue
        try:
            entries = read_jrdb_bytes(str(a))
        except (OSError, ValueError) as e:
            print(f"  [warn] {a.name}: 展開失敗 {e}", file=sys.stderr)
            continue
        buckets[t].extend(entries)
        n_files[t] += 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[JRDB TARGET 取込] src={src} / archives={len(archives)}")
    total_rows = 0
    for t in TARGET_TYPES:
        if t not in want or not buckets[t]:
            continue
        df = parse_target_bytes(t, buckets[t])
        out = out_dir / f"jrdb_target_{t}.pkl"
        df.to_pickle(out)
        total_rows += len(df)
        cov = _coverage(df)
        print(f"  {t:<14} files={n_files[t]:>4} rows={len(df):>8,} → {out}   {cov}")
    print(f"合計 {total_rows:,} 行。次: race_id/(umaban|ketto) で featured 結合 → CI下限>1.0 ハーネスへ。")
    return 0


def _coverage(df: pd.DataFrame) -> str:
    if df.empty:
        return "(空)"
    keys = [c for c in ("race_id", "umaban", "ketto", "seiseki_idm", "gaikyu_name", "rank")
            if c in df.columns]
    parts = []
    for k in keys[:3]:
        parts.append(f"{k} 非欠損 {df[k].notna().mean():.0%}")
    return " / ".join(parts)


if __name__ == "__main__":
    sys.exit(main())
