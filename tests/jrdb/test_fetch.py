"""JrdbFetcher の取得・重複防止テスト（合成 session。実接続はしない）。

download 層（jrdb_fetched_files 台帳）と ingest 層（JrdbStore の sha1 台帳）の
二重で「取得済み vs 新規」の重複が排除されることを検証する。
"""
from __future__ import annotations

import io
import zipfile

from src.jrdb import JrdbStore
from src.jrdb._fetch import JrdbFetcher
from src.jrdb._fetch import parse_index
from src.jrdb._fetch import select_files


# ── 合成 TYB レコード & zip ──
def _put(buf, start1, s):
    b = s.encode("cp932")
    buf[start1 - 1: start1 - 1 + len(b)] = b


def _tyb_record(umaban="01", idm=" 48.0"):
    r = bytearray(b" " * 128)
    _put(r, 1, "02152201")   # race_key -> 201502020201
    _put(r, 9, umaban)
    _put(r, 11, idm)
    _put(r, 31, " 55.0")     # パドック指数
    _put(r, 73, "  12.3")    # 直前単勝オッズ
    _put(r, 89, "480")       # 馬体重
    return bytes(r) + b"\r\n"


def _make_zip(entries):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, data in entries.items():
            z.writestr(name, data)
    return buf.getvalue()


class _Resp:
    def __init__(self, text=None, content=None):
        self.text = text
        self.content = content
        self.headers = {}


class _FakeSession:
    """index HTML と zip バイトを返す最小 session。get 呼び出し URL を記録する。"""

    def __init__(self, index_html, files):
        self.index_html = index_html
        self.files = files  # {filename: bytes}
        self.calls = []

    def get(self, url):
        self.calls.append(url)
        if url.endswith("index.html"):
            return _Resp(text=self.index_html)
        name = url.rsplit("/", 1)[-1]
        return _Resp(content=self.files[name])


_INDEX = (
    '<a href="TYB_2025.zip">2025</a>'
    '<a href="TYB_2024.zip">2024</a>'
    '<a href="TYB_2013.zip">2013</a>'
    '<a href="TYB250712.zip">0712</a>'
    '<a href="TYB250705.zip">0705</a>'
    '<a href="TYB.lzh">lzh</a>'
    '<a href="TYB.zip">zip</a>'
)


# ======================================================================
# index パース / 選別
# ======================================================================

def test_parse_index_year_and_single():
    files = parse_index(_INDEX, "https://x/member/datazip/Tyb/index.html")
    by = {f.name: f for f in files}
    assert by["TYB_2025.zip"].kind == "year" and by["TYB_2025.zip"].year == 2025
    assert by["TYB250712.zip"].kind == "single" and by["TYB250712.zip"].year == 2025
    # 絶対 URL に解決される
    assert by["TYB_2025.zip"].url == "https://x/member/datazip/Tyb/TYB_2025.zip"


def test_select_prefer_zip_over_lzh():
    files = parse_index(_INDEX, "https://x/Tyb/index.html")
    picked = select_files(files, prefer="zip", kinds=("year", "single", "other"))
    names = {f.name for f in picked}
    assert "TYB.zip" in names and "TYB.lzh" not in names  # 同一 stem は zip を残す


def test_select_since_year_and_latest():
    files = parse_index(_INDEX, "https://x/Tyb/index.html")
    years = select_files(files, since_year=2015, kinds=("year",))
    assert {f.name for f in years} == {"TYB_2025.zip", "TYB_2024.zip"}  # 2013 は除外
    latest = select_files(files, kinds=("single",), latest=1, singles_without_pack_only=False)
    assert [f.name for f in latest] == ["TYB250712.zip"]  # 新しい順に 1 件


def test_singles_without_pack_only_drops_covered_years():
    """年度パックのある年（2025）の single は除外し、パック未作成の当年 single だけ残す。"""
    idx = (
        '<a href="TYB_2025.zip">y25</a>'          # 2025 年度パックあり
        '<a href="2025/TYB250712.zip">d</a>'      # 2025 の日別 → 除外されるべき
        '<a href="2026/TYB260726.zip">e</a>'      # 2026（パック無し）→ 残す
        '<a href="2026/TYB260725.zip">f</a>'
    )
    files = parse_index(idx, "https://x/Tyb/index.html")
    picked = select_files(files, since_year=2015, kinds=("year", "single"))
    names = {f.name for f in picked}
    assert "TYB_2025.zip" in names
    assert "TYB250712.zip" not in names           # 2025 パックが覆うので single 除外
    assert {"TYB260726.zip", "TYB260725.zip"} <= names  # 2026 は single を残す


# ======================================================================
# download 層の重複防止（取得台帳）
# ======================================================================

def test_fetch_skips_recorded_year_pack(tmp_path):
    zipb = _make_zip({"TYB250712.txt": _tyb_record()})
    sess = _FakeSession(_INDEX, {"TYB_2025.zip": zipb})
    db = str(tmp_path / "t.db")
    fetcher = JrdbFetcher(sess, base_url="https://x/member/datazip",
                          cache_dir=str(tmp_path / "dl"), db_path=db, sleep=lambda: None)
    files = select_files(fetcher.list_type("Tyb"), since_year=2025, kinds=("year",))
    r1 = fetcher.fetch(files)
    assert len(r1["downloaded"]) == 1 and r1["skipped"] == 0
    r2 = fetcher.fetch(files)  # 2 回目は台帳一致で skip
    assert len(r2["downloaded"]) == 0 and r2["skipped"] == 1


def test_refresh_forces_redownload(tmp_path):
    zipb = _make_zip({"TYB250712.txt": _tyb_record()})
    sess = _FakeSession(_INDEX, {"TYB_2025.zip": zipb})
    db = str(tmp_path / "t.db")
    fetcher = JrdbFetcher(sess, base_url="https://x/member/datazip",
                          cache_dir=str(tmp_path / "dl"), db_path=db, sleep=lambda: None)
    files = select_files(fetcher.list_type("Tyb"), since_year=2025, kinds=("year",))
    fetcher.fetch(files)
    r = fetcher.fetch(files, refresh=True)
    assert len(r["downloaded"]) == 1  # refresh で再取得


# ======================================================================
# ingest 層まで通し（fetch → 冪等取込）
# ======================================================================

def test_fetch_and_ingest_end_to_end(tmp_path):
    zipb = _make_zip({"TYB250712.txt": _tyb_record(umaban="01") + _tyb_record(umaban="02")})
    sess = _FakeSession(_INDEX, {"TYB_2025.zip": zipb})
    db = str(tmp_path / "t.db")
    store = JrdbStore(db_path=db)
    fetcher = JrdbFetcher(sess, base_url="https://x/member/datazip",
                          cache_dir=str(tmp_path / "dl"), db_path=db, sleep=lambda: None)

    r1 = fetcher.fetch_and_ingest("Tyb", store=store, since_year=2025, kinds=("year",))
    assert r1["downloaded"] == 1
    assert r1["ingest"]["TYB"]["rows"] == 2   # 2 頭ぶん取込
    assert len(store.read("TYB")) == 2

    # 再実行: DL も ingest も skip（二重で重複排除）
    r2 = fetcher.fetch_and_ingest("Tyb", store=store, since_year=2025, kinds=("year",))
    assert r2["downloaded"] == 0 and r2["skipped_download"] == 1
    assert r2["ingest"]["TYB"]["skipped"] == 1 and r2["ingest"]["TYB"]["rows"] == 0
    assert len(store.read("TYB")) == 2  # 重複せず


def test_ledger_persists_across_instances(tmp_path):
    zipb = _make_zip({"TYB250712.txt": _tyb_record()})
    sess = _FakeSession(_INDEX, {"TYB_2025.zip": zipb})
    db = str(tmp_path / "t.db")
    files_arg = dict(base_url="https://x/member/datazip", cache_dir=str(tmp_path / "dl"),
                     db_path=db, sleep=lambda: None)
    f1 = JrdbFetcher(sess, **files_arg)
    files = select_files(f1.list_type("Tyb"), since_year=2025, kinds=("year",))
    f1.fetch(files)
    f2 = JrdbFetcher(sess, **files_arg)  # 別インスタンス
    r = f2.fetch(files)
    assert r["skipped"] == 1  # 台帳は永続
