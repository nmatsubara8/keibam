"""JRDB TARGET ダウンロードファイルの実ファイル解析（Phase2-3 自動化・STEP5）。

ページ HTML でなく実ファイルを解析する方針の入口。zip/lzh/txt（or ディレクトリ）を受け取り、
内包ファイルごとに Phase2（size/encoding/record数/改行）と Phase3（固定長/CSV/TSV 判定・
バイト長分布・先頭レコード・固定長ならバイトルーラ）を出力する。出力を docs/jrdb_target_downloads.md
へ転記してキー(Phase4)・情報量(Phase5)の人手解析に繋ぐ。

使い方:
  python scripts/jrdb_target_probe.py data/jrdb_dl/gaikyu20260726.zip
  python scripts/jrdb_target_probe.py data/jrdb_target/          # ディレクトリ一括
  python scripts/jrdb_target_probe.py file.txt --records 5 --ruler

依存は標準ライブラリのみ（lzh は lhafile があれば対応）。JRDB 認証情報は扱わない（解析専用）。
"""
from __future__ import annotations

import argparse
import sys
import zipfile
from pathlib import Path

# 試行する文字コード（JRDB 標準は cp932=SJIS。UTF-8 併記）。
_ENCODINGS = ["cp932", "utf-8", "euc_jp"]
_ARCHIVE_SUFFIXES = {".zip", ".lzh"}
_TEXT_SUFFIXES = {".txt", ".csv", ".tsv", ".dat"}


def read_entries(path: Path) -> list[tuple[str, bytes]]:
    """zip/lzh/txt から (内包名, バイト列) を返す（read_jrdb_bytes と同方針・自己完結）。"""
    suf = path.suffix.lower()
    if suf == ".zip":
        with zipfile.ZipFile(path) as z:
            return [(n, z.read(n)) for n in z.namelist() if not n.endswith("/")]
    if suf == ".lzh":
        try:
            import lhafile
        except ImportError:
            print(f"  [warn] {path.name}: lzh 展開に lhafile が必要（pip install lhafile）", file=sys.stderr)
            return []
        f = lhafile.Lhafile(str(path))
        return [(i.filename, f.read(i.filename)) for i in f.infolist() if not i.filename.endswith("/")]
    return [(path.name, path.read_bytes())]


def detect_newline(data: bytes) -> tuple[str, bytes]:
    """改行コードを判定し (ラベル, 区切りバイト) を返す。"""
    crlf, lf_only, cr_only = data.count(b"\r\n"), data.count(b"\n"), data.count(b"\r")
    if crlf and crlf >= lf_only - crlf:
        return "CRLF(\\r\\n)", b"\r\n"
    if lf_only and cr_only <= 1:
        return "LF(\\n)", b"\n"
    if cr_only and lf_only == 0:
        return "CR(\\r)", b"\r"
    return "none/unknown", b"\n"


def detect_encoding(data: bytes) -> str:
    """先頭〜64KB を decode してみて通る最初の候補を返す（chardet があれば併用）。"""
    sample = data[:65536]
    for enc in _ENCODINGS:
        try:
            sample.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    try:
        import chardet
        guess = chardet.detect(sample).get("encoding")
        if guess:
            return f"{guess}(chardet)"
    except Exception:
        pass
    return "unknown(latin-1 fallback)"


def analyze(name: str, data: bytes, *, n_records: int, ruler: bool) -> None:
    print(f"\n── {name} ──")
    print(f"  size: {len(data):,} bytes")
    if not data:
        print("  [empty]")
        return
    nl_label, nl = detect_newline(data)
    enc = detect_encoding(data)
    body = data[:-len(nl)] if data.endswith(nl) else data
    records = body.split(nl) if nl in body else [body]
    records = [r for r in records if r != b""]
    print(f"  改行: {nl_label} / 文字コード: {enc} / record数: {len(records):,}")

    lengths = [len(r) for r in records]
    if lengths:
        uniq = sorted(set(lengths))
        modal = max(uniq, key=lengths.count)
        frac = lengths.count(modal) / len(lengths)
        # 固定長 = レコードのバイト長がほぼ一定
        if frac >= 0.95 and len(uniq) <= 5:
            print(f"  形式推定: 固定長（byte長 {modal}・{frac:.0%} が同一・distinct={uniq[:8]}）")
            fmt = "fixed"
        else:
            dec = _safe_decode(records[0], enc)
            if "\t" in dec:
                print(f"  形式推定: TSV（先頭行にタブ {dec.count(chr(9))} 個・byte長 distinct={len(uniq)}）")
            elif "," in dec:
                print(f"  形式推定: CSV（先頭行にカンマ {dec.count(',')} 個・byte長 distinct={len(uniq)}）")
            else:
                print(f"  形式推定: 未確定（byte長 distinct={len(uniq)}・min={min(uniq)} max={max(uniq)}）")
            fmt = "delimited"

    print(f"  先頭 {min(n_records, len(records))} レコード:")
    for i, r in enumerate(records[:n_records]):
        dec = _safe_decode(r, enc)
        show = dec if len(dec) <= 160 else dec[:160] + " …"
        print(f"    [{i}] ({len(r)}B) {show}")

    if ruler and lengths and fmt == "fixed":
        print("  バイトルーラ（固定長のオフセット読み取り用・先頭レコード）:")
        _print_byte_ruler(records[0], enc)


def _safe_decode(b: bytes, enc: str) -> str:
    for e in ([enc.split("(")[0]] + _ENCODINGS):
        try:
            return b.decode(e)
        except (UnicodeDecodeError, LookupError):
            continue
    return b.decode("latin-1", errors="replace")


def _print_byte_ruler(rec: bytes, enc: str) -> None:
    """10 バイト刻みの目盛りと先頭レコードを並べて表示（固定長オフセット解析用）。"""
    tens = "".join(str((i // 10) % 10) for i in range(len(rec)))
    ones = "".join(str(i % 10) for i in range(len(rec)))
    dec = _safe_decode(rec, enc)
    print(f"    十位: {tens}")
    print(f"    一位: {ones}")
    # マルチバイトがあるとズレるため、まず latin-1（1byte=1文字）で raw も出す
    print(f"    raw : {rec.decode('latin-1', errors='replace')}")
    if len(dec) == len(rec):  # ASCII/半角のみなら decode 表示も整列する
        print(f"    dec : {dec}")


def iter_targets(root: Path) -> list[Path]:
    if root.is_dir():
        return sorted(p for p in root.iterdir()
                      if p.suffix.lower() in _ARCHIVE_SUFFIXES | _TEXT_SUFFIXES)
    return [root]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="JRDB TARGET ダウンロードファイルの実ファイル解析（Phase2-3）")
    ap.add_argument("path", help="zip/lzh/txt ファイル、またはそれらを含むディレクトリ")
    ap.add_argument("--records", type=int, default=3, help="表示する先頭レコード数（既定 3）")
    ap.add_argument("--ruler", action="store_true", help="固定長時にバイトルーラを表示（オフセット解析用）")
    args = ap.parse_args(argv)

    root = Path(args.path)
    if not root.exists():
        print(f"パスがありません: {root}", file=sys.stderr)
        return 1
    targets = iter_targets(root)
    if not targets:
        print(f"解析対象（.zip/.lzh/.txt/.csv/.tsv/.dat）が見つかりません: {root}", file=sys.stderr)
        return 1
    for t in targets:
        print(f"\n========== {t.name} ==========")
        try:
            entries = read_entries(t)
        except (zipfile.BadZipFile, OSError) as e:
            print(f"  [error] 展開失敗: {e}", file=sys.stderr)
            continue
        if not entries:
            print("  [warn] 内包ファイルなし（壊れ/未対応の可能性）")
        for name, data in entries:
            analyze(name, data, n_records=args.records, ruler=args.ruler)
    print("\n→ 出力を docs/jrdb_target_downloads.md の Phase2-5 台帳へ転記してください。"
          "\n  次: キー列(Phase4)を確定 → race_id/ketto で結合可能か検証 → CI下限>1.0 ハーネスへ。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
