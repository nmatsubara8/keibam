"""JRDB 会員エリアからの取得（Basic 認証・index パース・取得台帳）。

実取得はユーザのローカルで会員資格（環境変数 JRDB_USER / JRDB_PASS）により行う。
`/member/datazip/` は HTTP Basic 認証で保護され、ページは PHPSESSID セッションを持つ
（`requests.Session` が Cookie を自動保持するので Basic 認証だけ渡せばよい）。

重複防止（「取得済み vs 新規」）は二層で効く:
  - **download 層**: `jrdb_fetched_files` 台帳（url 主キー）で既取得 URL を記録し、
    未取得/サイズ変化のみ DL する（帯域・時間の節約）。
  - **ingest 層**: DL 物を `JrdbStore.ingest_dir` に渡すと sha1 台帳で内容重複を排除し、
    (race_id, umaban) keep-last で行重複も排除する（既存の store 側 2 軸）。

ネットワーク層（session）は注入可能。テストは合成 index HTML + zip バイトで検証し、
実接続はユーザのローカルでのみ行う（この環境からは会員データへ接続しない）。
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import re
import time
from pathlib import Path
from typing import Callable
from typing import Optional
from urllib.parse import urljoin

from sqlalchemy import text

from src.storage._db import get_engine

logger = logging.getLogger(__name__)

FETCH_LEDGER_TABLE = "jrdb_fetched_files"

# index ページの .zip / .lzh リンク。
_LINK_RE = re.compile(r'href=["\']([^"\']+\.(?:zip|lzh))["\']', re.IGNORECASE)
# 年度パック（例 TYB_2025.zip）と単体（例 TYB260726.zip）。
_YEAR_RE = re.compile(r"^([A-Za-z]{2,3})_(\d{4})\.(zip|lzh)$", re.IGNORECASE)
_SINGLE_RE = re.compile(r"^([A-Za-z]{2,4})(\d{6})\.(zip|lzh)$", re.IGNORECASE)


@dataclasses.dataclass(frozen=True)
class RemoteFile:
    """index からパースした 1 ファイル。"""

    name: str            # ファイル名（例 TYB_2025.zip）
    url: str             # 絶対 URL
    kind: str            # "year" | "single" | "other"
    year: Optional[int]  # 年（4 桁。single は yyMMdd の yy を 2000+/1900+ で補完）
    ext: str             # "zip" | "lzh"


def _yy_to_year(yy: int) -> int:
    """単体ファイルの 2 桁年を 4 桁に補完（<80→2000 代、>=80→1900 代）。"""
    return 2000 + yy if yy < 80 else 1900 + yy


def parse_index(html: str, index_url: str) -> list[RemoteFile]:
    """index HTML から .zip/.lzh リンクを抽出して RemoteFile のリストにする。"""
    out: list[RemoteFile] = []
    seen: set[str] = set()
    for m in _LINK_RE.finditer(html):
        href = m.group(1)
        name = Path(href).name
        if name in seen:
            continue
        seen.add(name)
        url = urljoin(index_url, href)
        ym = _YEAR_RE.match(name)
        sm = _SINGLE_RE.match(name)
        if ym:
            out.append(RemoteFile(name, url, "year", int(ym.group(2)), ym.group(3).lower()))
        elif sm:
            out.append(RemoteFile(name, url, "single", _yy_to_year(int(sm.group(2)[:2])), sm.group(3).lower()))
        else:
            out.append(RemoteFile(name, url, "other", None, Path(name).suffix.lstrip(".").lower()))
    return out


def _stem(name: str) -> str:
    return name.rsplit(".", 1)[0]


def select_files(
    files: list[RemoteFile],
    *,
    prefer: str = "zip",
    since_year: Optional[int] = None,
    kinds: tuple[str, ...] = ("year", "single"),
    latest: Optional[int] = None,
) -> list[RemoteFile]:
    """設定に従って取得対象を選別する。

    - `prefer`（zip/lzh）: 同一 stem に zip と lzh があれば prefer 側だけ残す
      （lzh は開発中止・zip 推奨）。
    - `kinds`: "year"（年度パック）/"single"（単体日次）のどちらを対象にするか。
    - `since_year`: 指定年以降のみ（year/single とも year 属性で判定）。
    - `latest`: single を新しい順に N 件だけ（日次運用用）。
    """
    # prefer で重複拡張子を解消
    by_stem: dict[str, RemoteFile] = {}
    for f in files:
        st = _stem(f.name)
        cur = by_stem.get(st)
        if cur is None:
            by_stem[st] = f
        elif f.ext == prefer and cur.ext != prefer:
            by_stem[st] = f
    picked = [f for f in by_stem.values() if f.kind in kinds]
    if since_year is not None:
        picked = [f for f in picked if f.year is None or f.year >= since_year]
    # year はファイル名昇順、single は新しい順、その他（日付無し最新版等）は末尾に。
    years = sorted((f for f in picked if f.kind == "year"), key=lambda f: f.name)
    singles = sorted((f for f in picked if f.kind == "single"), key=lambda f: f.name, reverse=True)
    others = sorted((f for f in picked if f.kind not in ("year", "single")), key=lambda f: f.name)
    if latest is not None:
        singles = singles[:latest]
    return years + singles + others


class FetchLedger:
    """取得済み URL 台帳（download 層の重複防止）。"""

    def __init__(self, db_path: Optional[str] = None) -> None:
        self._engine = get_engine(db_path)
        with self._engine.begin() as conn:
            conn.execute(text(
                f'CREATE TABLE IF NOT EXISTS "{FETCH_LEDGER_TABLE}" (\n'
                '  "url"        TEXT PRIMARY KEY,\n'
                '  "filename"   TEXT,\n'
                '  "size"       INTEGER,\n'
                '  "sha1"       TEXT,\n'
                '  "fetched_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n)'
            ))

    def recorded_size(self, url: str) -> Optional[int]:
        """既取得ならサイズを返す（未取得は None）。size 未知記録は -1。"""
        with self._engine.connect() as conn:
            row = conn.execute(
                text(f'SELECT "size" FROM "{FETCH_LEDGER_TABLE}" WHERE "url" = :u'), {"u": url}
            ).fetchone()
        if row is None:
            return None
        return int(row[0]) if row[0] is not None else -1

    def record(self, url: str, filename: str, size: int, sha1: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(text(
                f'INSERT OR REPLACE INTO "{FETCH_LEDGER_TABLE}" '
                '("url","filename","size","sha1") VALUES (:u,:f,:s,:h)'
            ), {"u": url, "f": filename, "s": size, "h": sha1})

    def all(self):
        import pandas as pd  # noqa: PLC0415
        with self._engine.connect() as conn:
            return pd.read_sql(text(f'SELECT * FROM "{FETCH_LEDGER_TABLE}"'), conn)


class JrdbFetcher:
    """JRDB 会員エリアからの取得器（session 注入・ポライトネス・取得台帳）。

    session: `.get(url)` で `.text`/`.content`/`.headers` を持つレスポンスを返すオブジェクト
        （実運用は `requests.Session`（Basic 認証済）、テストは fake を注入）。
    """

    def __init__(
        self,
        session,
        base_url: str = "https://jrdb.com/member/datazip",
        *,
        cache_dir: str = "data/jrdb_dl",
        db_path: Optional[str] = None,
        sleep: Optional[Callable[[], None]] = None,
    ) -> None:
        self._s = session
        self._base = base_url.rstrip("/")
        self._cache = Path(cache_dir)
        self._ledger = FetchLedger(db_path)
        # 既定は KEIBA_SCRAPE_DELAY（≥4 秒）＋揺らぎ。テストは sleep=lambda: None を注入。
        self._sleep = sleep if sleep is not None else self._polite_sleep

    @staticmethod
    def _polite_sleep() -> None:
        from src.preparing._rate_limiter import polite_interval  # noqa: PLC0415
        time.sleep(polite_interval())

    def list_type(self, type_dir: str) -> list[RemoteFile]:
        """データ型の index を取得してファイル一覧を返す。"""
        index_url = f"{self._base}/{type_dir.strip('/')}/index.html"
        resp = self._s.get(index_url)
        html = getattr(resp, "text", None)
        if html is None:
            html = resp.content.decode("cp932", "replace")
        files = parse_index(html, index_url)
        logger.info("[jrdb-fetch] %s: index に %d ファイル", type_dir, len(files))
        return files

    def fetch(self, files: list[RemoteFile], *, refresh: bool = False) -> dict:
        """未取得（or サイズ変化）のファイルだけ DL してキャッシュに保存する。

        Returns: {"downloaded": [Path...], "skipped": int, "bytes": int}
        """
        self._cache.mkdir(parents=True, exist_ok=True)
        downloaded: list[Path] = []
        skipped = 0
        total_bytes = 0
        for f in files:
            recorded = self._ledger.recorded_size(f.url)
            dest = self._cache / f.name
            # 年度パックは不変 → 既取得なら常に skip。単体は日中更新され得るので
            # refresh か「ローカル実体が無い」場合のみ再取得する。
            if not refresh and recorded is not None and (f.kind == "year" or dest.exists()):
                skipped += 1
                continue
            self._sleep()  # ポライトネス
            resp = self._s.get(f.url)
            data = resp.content
            dest.write_bytes(data)
            sha1 = hashlib.sha1(data).hexdigest()  # noqa: S324 — 内容一致判定のみ
            self._ledger.record(f.url, f.name, len(data), sha1)
            downloaded.append(dest)
            total_bytes += len(data)
        logger.info("[jrdb-fetch] DL %d 件 / skip %d 件 / %d bytes",
                    len(downloaded), skipped, total_bytes)
        return {"downloaded": downloaded, "skipped": skipped, "bytes": total_bytes}

    def fetch_and_ingest(
        self,
        type_dir: str,
        *,
        store,
        since_year: Optional[int] = None,
        kinds: tuple[str, ...] = ("year", "single"),
        latest: Optional[int] = None,
        refresh: bool = False,
    ) -> dict:
        """1 データ型を: index 取得 → 選別 → DL（未取得のみ） → 冪等取込 する。

        store: `JrdbStore`（ingest_dir を持つ）。
        Returns: 取得サマリ + ingest サマリ。
        """
        files = select_files(
            self.list_type(type_dir), since_year=since_year, kinds=kinds, latest=latest
        )
        fetched = self.fetch(files, refresh=refresh)
        ingest = store.ingest_dir(str(self._cache), extract_to=str(self._cache / "txt"))
        return {
            "type_dir": type_dir,
            "listed": len(files),
            "downloaded": len(fetched["downloaded"]),
            "skipped_download": fetched["skipped"],
            "bytes": fetched["bytes"],
            "ingest": ingest,
        }
