"""jrdb_target_probe の純ロジック（改行/文字コード判定・解凍・整列）テスト（STEP5）。"""
from __future__ import annotations

import importlib.util
import zipfile
from pathlib import Path

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "jrdb_target_probe.py"
_spec = importlib.util.spec_from_file_location("jrdb_target_probe", _MOD)
m = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(m)


def test_detect_newline_crlf_lf_cr():
    assert m.detect_newline(b"a\r\nb\r\n")[0].startswith("CRLF")
    assert m.detect_newline(b"a\nb\n")[0].startswith("LF")
    assert m.detect_newline(b"a\rb\r")[0].startswith("CR")


def test_detect_encoding_cp932_and_utf8():
    # 「印」= cp932 でエンコードした固定長っぽいレコード
    assert m.detect_encoding("外厩◎".encode("cp932")) == "cp932"
    # ASCII は最初の候補 cp932 で decode 通る（cp932 は ASCII 上位互換）
    assert m.detect_encoding(b"RaceID,HorseNo") == "cp932"


def test_safe_decode_falls_back():
    assert m._safe_decode("放牧先".encode("cp932"), "cp932") == "放牧先"
    # 壊れたバイトでも例外を投げず replace で返す
    assert isinstance(m._safe_decode(b"\xff\xfe\x00", "cp932"), str)


def test_read_entries_zip_and_txt(tmp_path):
    txt = tmp_path / "gaikyu.txt"
    txt.write_bytes(b"2026010101011\r\n2026010101022\r\n")
    assert m.read_entries(txt) == [("gaikyu.txt", b"2026010101011\r\n2026010101022\r\n")]

    zp = tmp_path / "gaikyu20260726.zip"
    with zipfile.ZipFile(zp, "w") as z:
        z.writestr("GAIKYU260726.txt", b"row1\r\nrow2\r\n")
    entries = m.read_entries(zp)
    assert entries == [("GAIKYU260726.txt", b"row1\r\nrow2\r\n")]


def test_iter_targets_dir_filters_by_suffix(tmp_path):
    (tmp_path / "a.zip").write_bytes(b"PK")
    (tmp_path / "b.txt").write_bytes(b"x")
    (tmp_path / "c.md").write_bytes(b"ignore")  # 対象外拡張子
    names = sorted(p.name for p in m.iter_targets(tmp_path))
    assert names == ["a.zip", "b.txt"]
