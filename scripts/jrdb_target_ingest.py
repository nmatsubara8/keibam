"""JRDB TARGET ダウンロードデータ 全8種を取り込み、種別ごとに正規化 pickle へ保存する（STEP5）。

ディレクトリ内の zip/lzh を種別（zip 名先頭英字）で自動分類し、`src.jrdb._target` で
featured 結合可能な正規化 DataFrame にして `--out-dir` へ `jrdb_target_<種別>.pkl` 保存する。
外厩に絞らず 8 種すべてを対象にする。

**重複と漏れの検証**（再DLコピー・年次×日次の重なり対策）:
  - アーカイブは内容 sha1 で重複排除（`gaikyu_x.zip` と `gaikyu_x (1).zip` を1回だけ取込）。
  - 種別に分類できなかったアーカイブを件数＋接頭辞サンプルで**明示**（漏れの可視化）。
  - 種別ごとに自然キー（race_id×ketto 等）で重複除去し、除去件数を**明示**。

使い方:
  python scripts/jrdb_target_ingest.py --src /mnt/c/Users/Ayaka/Downloads --out-dir data/jrdb_target
  python scripts/jrdb_target_ingest.py --src data/jrdb_target_dl   # 複数日/年のパックをまとめて
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._target import (  # noqa: E402
    NATURAL_KEYS,
    TARGET_TYPES,
    classify,
    dedup_by_keys,
    parse_target_bytes,
)

_ARCHIVE_SUFFIXES = {".zip", ".lzh"}


def _prefix(name: str) -> str:
    """ファイル名（拡張子除く）の先頭英字（分類できなかった時の漏れ確認用サンプル）。"""
    stem = Path(name).stem
    head = "".join(c for c in stem if c.isalpha() or c == "_")
    return head.split("_")[0][:16] or stem[:16]


def main(argv=None) -> int:
    from src.jrdb._extract import read_jrdb_bytes

    ap = argparse.ArgumentParser(description="JRDB TARGET 全8種を取り込み正規化 pickle へ（重複/漏れ検証つき）")
    ap.add_argument("--src", required=True, help="zip/lzh を置いたディレクトリ（複数日/年のパック可）")
    ap.add_argument("--out-dir", default="data/jrdb_target", help="正規化 pickle の出力先")
    ap.add_argument("--types", nargs="+", default=None,
                    help=f"取り込む種別を限定（既定=全8種）。選択肢: {' '.join(TARGET_TYPES)}")
    ap.add_argument("--no-dedup", action="store_true", help="自然キーの重複除去を行わない（生の結合）")
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
    buckets: dict[str, list] = {t: [] for t in TARGET_TYPES}
    n_files: dict[str, int] = {t: 0 for t in TARGET_TYPES}
    seen_sha1: dict[str, str] = {}          # sha1 → 最初に見たファイル名（内容重複の検出）
    dup_archives: list[tuple[str, str]] = []  # (重複ファイル, 元ファイル)
    unclassified: list[str] = []
    unreadable: list[str] = []

    for a in archives:
        # 先に名前で分類し、8種でないもの（KYI/SED/SKB 等）は読まない（無駄な I/O を避ける）。
        t = classify(a.name)
        if t is None:
            unclassified.append(a.name)
            continue
        if t not in want:
            continue
        data = a.read_bytes()
        sha = hashlib.sha1(data).hexdigest()
        if sha in seen_sha1:                # 内容が同一＝再DLコピー等 → 1回だけ
            dup_archives.append((a.name, seen_sha1[sha]))
            continue
        seen_sha1[sha] = a.name
        try:
            entries = read_jrdb_bytes(str(a))
        except (OSError, ValueError) as e:
            unreadable.append(f"{a.name}: {e}")
            continue
        if not entries:
            unreadable.append(f"{a.name}: 内包ファイルなし")
            continue
        # (source_name, internal_name, data)＝ランクの日付は source_name(zip名)から取る。
        buckets[t].extend((a.name, name, d) for name, d in entries)
        n_files[t] += 1

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[JRDB TARGET 取込] src={src} / archives={len(archives)}"
          f" / 内容重複スキップ={len(dup_archives)} / 未分類={len(unclassified)}"
          f" / 読取不可={len(unreadable)}")

    total_rows = 0
    for t in TARGET_TYPES:
        if t not in want or not buckets[t]:
            continue
        df = parse_target_bytes(t, buckets[t])
        raw = len(df)
        dropped = 0
        if not args.no_dedup:
            df, dropped = dedup_by_keys(df, NATURAL_KEYS.get(t, []))
        out = out_dir / f"jrdb_target_{t}.pkl"
        df.to_pickle(out)
        total_rows += len(df)
        dup_note = f"重複除去 {dropped}" if dropped else "重複なし"
        print(f"  {t:<14} files={n_files[t]:>4} rows={len(df):>8,}"
              f"（生 {raw:,}・{dup_note}） {_coverage(df)}")

    # ── 漏れ/重複の検証レポート ──
    if dup_archives:
        print(f"\n[内容重複アーカイブ {len(dup_archives)} 件（sha1 一致・取込は1回）]")
        for dup, orig in dup_archives[:10]:
            print(f"  {dup}  ≡  {orig}")
        if len(dup_archives) > 10:
            print(f"  … 他 {len(dup_archives) - 10} 件")
    if unclassified:
        pref = Counter(_prefix(n) for n in unclassified)
        print(f"\n[未分類アーカイブ {len(unclassified)} 件（TARGET 8種以外＝取込対象外）]")
        print("  接頭辞別: " + ", ".join(f"{p}×{c}" for p, c in pref.most_common(20)))
        print("  ※ この中に取り込みたい TARGET データが無いか接頭辞を確認してください。")
    if unreadable:
        print(f"\n[読取不可 {len(unreadable)} 件]")
        for msg in unreadable[:10]:
            print(f"  {msg}")

    print(f"\n合計 {total_rows:,} 行。次: race_id/(umaban|ketto) で featured 結合 → CI下限>1.0 ハーネスへ。")
    return 0


def _coverage(df) -> str:
    if df.empty:
        return "(空)"
    keys = [c for c in ("race_id", "umaban", "ketto", "seiseki_idm", "gaikyu_name", "rank")
            if c in df.columns]
    return " / ".join(f"{k} {df[k].notna().mean():.0%}" for k in keys[:3])


if __name__ == "__main__":
    sys.exit(main())
