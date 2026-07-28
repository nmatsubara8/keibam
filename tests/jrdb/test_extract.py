"""JRDB アーカイブ展開（.txt/.zip 透過読み込み）の単体テスト。

.lzh は外部依存(lhafile)かつバイナリ fixture が必要なため、ここでは .txt と .zip の
経路と extract_dir の種別分類を検証する（lzh 経路は read_jrdb_bytes の分岐のみ）。
"""
from __future__ import annotations

import zipfile

from src.jrdb._extract import extract_dir, read_jrdb_bytes


def test_read_txt(tmp_path):
    p = tmp_path / "KYI250712.txt"
    p.write_bytes(b"abc\r\n")
    got = read_jrdb_bytes(str(p))
    assert got == [("KYI250712.txt", b"abc\r\n")]


def test_read_zip(tmp_path):
    z = tmp_path / "KYI250712.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("KYI250712.txt", b"xyz\r\n")
    got = read_jrdb_bytes(str(z))
    assert got == [("KYI250712.txt", b"xyz\r\n")]


def test_read_bad_zip_is_skipped_not_raised(tmp_path):
    """壊れた zip（HTML エラーページ等）は例外で落ちず空リストを返す。"""
    bad = tmp_path / "KSA260726.zip"
    bad.write_bytes(b"<html><body>Not Found</body></html>")
    assert read_jrdb_bytes(str(bad)) == []      # 例外を投げない


def test_extract_dir_skips_bad_zip_and_keeps_good(tmp_path):
    """1 つ壊れた zip があっても、他の正常ファイルの分類・展開は継続する。"""
    src = tmp_path / "src"
    src.mkdir()
    (src / "KSA260726.zip").write_bytes(b"<html>auth error</html>")  # 壊れ
    with zipfile.ZipFile(src / "CSA260726.zip", "w") as zf:          # 正常
        zf.writestr("CSA260726.txt", b"c\r\n")
    by_type = extract_dir(str(src), str(tmp_path / "out"))
    assert "CSA" in by_type and "KSA" not in by_type   # 壊れた KSA は分類されない
    assert (tmp_path / "out" / "CSA260726.txt").exists()


def test_extract_dir_classifies_by_prefix(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    (src / "KYI250712.txt").write_bytes(b"k\r\n")
    (src / "SED250712.txt").write_bytes(b"s\r\n")
    with zipfile.ZipFile(src / "SKB250712.zip", "w") as zf:
        zf.writestr("SKB250712.txt", b"b\r\n")
    out = tmp_path / "out"
    by_type = extract_dir(str(src), str(out))
    assert set(by_type) == {"KYI", "SED", "SKB"}
    assert (out / "KYI250712.txt").exists()
    assert (out / "SKB250712.txt").exists()  # zip から展開された
    # 冪等: 再実行しても壊れない
    by_type2 = extract_dir(str(src), str(out))
    assert by_type2["KYI"]
