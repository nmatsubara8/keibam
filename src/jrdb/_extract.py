"""JRDB 配布アーカイブ（.lzh / .zip）から固定長テキストを取り出す。

JRDB は lzh（推奨は zip）で配布される。lzh は純 Python の lhafile で読む（stdlib 非対応）。
展開済み .txt はそのまま返す。ファイル種別（KYI/SED/SKB…）は接頭辞で判定する。
"""
from __future__ import annotations

import logging
import zipfile
from pathlib import Path

logger = logging.getLogger(__name__)


def _diagnose_bad_archive(p: Path, data: bytes) -> str:
    """壊れたアーカイブの中身を推定して短い説明を返す（HTML=認証/URL 誤りの典型）。"""
    head = data[:512].lstrip()
    if head[:1] == b"<" or b"<html" in head.lower() or b"<!doctype" in head.lower():
        return "HTML（認証失敗/URL 誤り/エラーページの可能性）"
    if not data:
        return "空ファイル（0 byte・DL 失敗の可能性）"
    return f"先頭バイト {data[:4]!r}（zip 署名 b'PK\\x03\\x04' でない）"


def read_jrdb_bytes(path: str) -> list[tuple[str, bytes]]:
    """アーカイブ or テキストから (内包ファイル名, バイト列) のリストを返す。

    .txt はそのファイル自身、.zip/.lzh は内包する全エントリを展開して返す。
    壊れた/中身が異なるアーカイブ（HTML エラーページ等）は **例外で落とさず**、
    警告して空リストを返す（1 ファイルの破損で取込全体を止めないため）。
    lhafile 未導入で .lzh を渡した場合のみ明示エラー（pip install lhafile を促す）。
    """
    p = Path(path)
    suf = p.suffix.lower()
    if suf == ".txt":
        return [(p.name, p.read_bytes())]
    if suf == ".zip":
        try:
            with zipfile.ZipFile(p) as z:
                return [(n, z.read(n)) for n in z.namelist() if not n.endswith("/")]
        except zipfile.BadZipFile:
            logger.warning("[jrdb-extract] %s は正しい zip ではありません: %s → スキップ",
                           p.name, _diagnose_bad_archive(p, p.read_bytes()))
            return []
    if suf == ".lzh":
        try:
            import lhafile
        except ImportError as e:  # noqa: BLE001
            raise SystemExit(
                "lzh の展開に lhafile が必要です: pip install lhafile"
                "（または zip 版を使ってください＝JRDB 推奨）"
            ) from e
        try:
            lf = lhafile.Lhafile(str(p))
            return [(i.filename, lf.read(i.filename)) for i in lf.infolist()]
        except Exception as e:  # noqa: BLE001 — lhafile は独自例外を投げるため広く捕捉
            logger.warning("[jrdb-extract] %s の lzh 展開に失敗（%s）→ スキップ", p.name, e)
            return []
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
