"""JRDB 配布アーカイブ（.lzh / .zip）から固定長テキストを取り出す。

JRDB は lzh（推奨は zip）で配布される。lzh は純 Python の lhafile で読む（stdlib 非対応）。
展開済み .txt はそのまま返す。ファイル種別（KYI/SED/SKB…）は接頭辞で判定する。
"""
from __future__ import annotations

import zipfile
from pathlib import Path


def read_jrdb_bytes(path: str) -> list[tuple[str, bytes]]:
    """アーカイブ or テキストから (内包ファイル名, バイト列) のリストを返す。

    .txt はそのファイル自身、.zip/.lzh は内包する全エントリを展開して返す。
    lhafile 未導入で .lzh を渡した場合は明示エラー（pip install lhafile を促す）。
    """
    p = Path(path)
    suf = p.suffix.lower()
    if suf == ".txt":
        return [(p.name, p.read_bytes())]
    if suf == ".zip":
        with zipfile.ZipFile(p) as z:
            return [(n, z.read(n)) for n in z.namelist() if not n.endswith("/")]
    if suf == ".lzh":
        try:
            import lhafile
        except ImportError as e:  # noqa: BLE001
            raise SystemExit(
                "lzh の展開に lhafile が必要です: pip install lhafile"
                "（または zip 版を使ってください＝JRDB 推奨）"
            ) from e
        lf = lhafile.Lhafile(str(p))
        return [(i.filename, lf.read(i.filename)) for i in lf.infolist()]
    # 拡張子不明はテキスト扱いで試す
    return [(p.name, p.read_bytes())]


def extract_dir(src_dir: str, out_dir: str) -> dict[str, list[str]]:
    """src_dir 内の .txt/.zip/.lzh を out_dir に .txt 展開し、種別→パスの辞書を返す。

    種別は展開後ファイル名の接頭辞（KYI/SED/SKB/KKA/BAC/SRB/TYB/CYB/CHA…）で分類。
    既存の同名 .txt があれば上書きせずスキップ（再実行の冪等性）。
    """
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    by_type: dict[str, list[str]] = {}
    for src in sorted(Path(src_dir).glob("*")):
        if src.suffix.lower() not in (".txt", ".zip", ".lzh"):
            continue
        for name, data in read_jrdb_bytes(str(src)):
            dest = out / Path(name).name
            if not dest.exists():
                dest.write_bytes(data)
            pref = dest.name[:3].upper()
            by_type.setdefault(pref, []).append(str(dest))
    return by_type
