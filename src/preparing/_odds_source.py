"""オッズ取得ソースの抽象化（netkeiba / JRA-VAN Data Lab を差し替え可能にする）。

JRA-VAN Data Lab（JV-Link）は Windows 専用 COM のため、Linux VPS で動く本システムは
直接呼べない。そこで取得元を AbstractOddsSource として抽象化し:

- NetkeibaOddsSource     — 既存の Playwright スクレイパに委譲（現行の既定）
- JraVanFileDropSource   — Windows 側エージェントが置く JSON を読むだけの受信側

## JRA-VAN 連携契約（Windows 側エージェントの仕様）

Windows 機で JV-Link（速報系オッズ）を購読する小さなエージェント（別途実装）が、
取得のたびに以下の JSON を `data/incoming/jravan/` へ 1 レース 1 ファイルで配置する
（ファイル名は `<race_id>_<UTC タイムスタンプ>.json` 等、一意であれば任意）:

    {
      "race_id": "202605030211",
      "post_time": "2026-06-07T15:40:00",      // 発走時刻（ISO8601、ローカル）
      "captured_at": "2026-06-07T15:10:05",    // 取得時刻（ISO8601）
      "win_odds": [{"umaban": 1, "odds": 2.4}, {"umaban": 2, "odds": 15.1}, ...]
    }

JraVanFileDropSource はディレクトリ内の最新ファイルを race_id ごとに読む。
処理済みファイルの削除/退避はエージェント側または運用に委ねる（読み側は冪等）。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from abc import ABC
from abc import abstractmethod

logger = logging.getLogger(__name__)

JRAVAN_INCOMING_DIR = os.path.join("data", "incoming", "jravan")


class AbstractOddsSource(ABC):
    """単勝オッズ取得元の契約。"""

    @abstractmethod
    def fetch_today_races(self, date_str: str) -> list[tuple[str, dt.datetime]]:
        """指定日の (race_id, 発走時刻) リストを返す。"""
        raise NotImplementedError

    @abstractmethod
    def fetch_win_odds(self, race_id: str) -> list[tuple[int, float]]:
        """1 レースの現在オッズ [(馬番, 単勝オッズ), ...] を返す。"""
        raise NotImplementedError

    def fetch_win_and_place_odds(
        self, race_id: str
    ) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
        """(単勝, 複勝) の [(馬番, オッズ), ...] を返す。

        複勝オッズを配信できるソース（netkeiba の b1 ページは単勝と複勝が同居）だけが
        意味のある place を返す。持たないソース（JRA-VAN 速報の単勝のみ 等）は複勝を空で返す。
        odds_watch は overlay 蓄積のためこれを優先して使い、複勝は同一ページ取得ぶんで
        追加リクエストなしに得る。
        """
        return self.fetch_win_odds(race_id), []

    def close(self) -> None:  # noqa: B027 — 既定は何もしない（リソース保持ソース用フック）
        pass


class NetkeibaOddsSource(AbstractOddsSource):
    """netkeiba スクレイパへの委譲実装（現行の既定ソース）。"""

    def __init__(self, scraper=None) -> None:
        self._scraper = scraper  # OddsSnapshotScraper（遅延生成）

    def _ensure_scraper(self):
        if self._scraper is None:
            from src.preparing._odds_snapshot import OddsSnapshotScraper

            self._scraper = OddsSnapshotScraper()
        return self._scraper

    def fetch_today_races(self, date_str: str) -> list[tuple[str, dt.datetime]]:
        from src.preparing._scrape_shutuba import scrape_race_id_race_time_list
        from src.preparing.odds_scheduler import build_race_post_times

        race_ids, times = scrape_race_id_race_time_list(date_str)
        return build_race_post_times(race_ids, times, date_str)

    def _parse_b1(self, race_id: str):
        """b1 ページ（単勝・複勝が同居）を 1 回取得し (単勝rows, 複勝rows) を返す。

        単勝は id パーサ→旧DOMのクラスパーサへフォールバック。複勝は id パーサのみ
        （旧DOMにクラスフォールバックが無いページでは空＝そのティックは複勝取れず）。
        取得は 1 回だけなので複勝を足しても netkeiba へのリクエストは増えない。
        """
        from src.constants._bet_types import BetType
        from src.preparing._odds_snapshot import parse_combo_odds_html
        from src.preparing._odds_snapshot import parse_win_odds_html

        scraper = self._ensure_scraper()
        html = scraper.fetch_html(race_id, BetType.TANSHO)
        win = parse_combo_odds_html(html, BetType.TANSHO) or parse_win_odds_html(html)
        place = parse_combo_odds_html(html, BetType.FUKUSHO)
        return win, place

    @staticmethod
    def _to_pairs(rows) -> list[tuple[int, float]]:
        return [(int(combo[0]), float(odds)) for combo, odds in rows]

    def fetch_win_odds(self, race_id: str) -> list[tuple[int, float]]:
        win, _ = self._parse_b1(race_id)
        return self._to_pairs(win)

    def fetch_win_and_place_odds(
        self, race_id: str
    ) -> tuple[list[tuple[int, float]], list[tuple[int, float]]]:
        """b1 を 1 回取得して単勝・複勝を同時に返す（追加リクエストなし）。"""
        win, place = self._parse_b1(race_id)
        return self._to_pairs(win), self._to_pairs(place)

    def close(self) -> None:
        if self._scraper is not None and getattr(self._scraper, "_scraper", None) is not None:
            try:
                self._scraper._scraper.close_sync()
            except Exception:  # noqa: BLE001
                pass


class JraVanFileDropSource(AbstractOddsSource):
    """JRA-VAN（Windows エージェント経由のファイル連携）の受信実装。

    モジュール docstring の JSON 契約に従い、`incoming_dir` 内のファイルから
    race_id ごとに最新（captured_at 最大）のオッズを読む。
    """

    def __init__(self, incoming_dir: str = JRAVAN_INCOMING_DIR) -> None:
        self._dir = incoming_dir

    def _latest_payloads(self) -> dict[str, dict]:
        """race_id → 最新 payload。読めないファイルは警告してスキップ。"""
        latest: dict[str, dict] = {}
        if not os.path.isdir(self._dir):
            return latest
        for fname in sorted(os.listdir(self._dir)):
            if not fname.endswith(".json"):
                continue
            path = os.path.join(self._dir, fname)
            try:
                with open(path) as f:
                    payload = json.load(f)
                race_id = str(payload["race_id"])
            except Exception as e:  # noqa: BLE001
                logger.warning("[jravan] 読込失敗 %s: %s", path, e)
                continue
            prev = latest.get(race_id)
            if prev is None or payload.get("captured_at", "") >= prev.get("captured_at", ""):
                latest[race_id] = payload
        return latest

    def fetch_today_races(self, date_str: str) -> list[tuple[str, dt.datetime]]:
        out = []
        for race_id, payload in self._latest_payloads().items():
            post = payload.get("post_time")
            if post is None:
                continue
            post_dt = dt.datetime.fromisoformat(post)
            if post_dt.strftime("%Y%m%d") == date_str:
                out.append((race_id, post_dt))
        return sorted(out, key=lambda x: x[1])

    def fetch_win_odds(self, race_id: str) -> list[tuple[int, float]]:
        payload = self._latest_payloads().get(str(race_id))
        if payload is None:
            return []
        return [
            (int(row["umaban"]), float(row["odds"]))
            for row in payload.get("win_odds", [])
            if row.get("odds") is not None and float(row["odds"]) > 0
        ]


def create_odds_source(kind: str = "netkeiba") -> AbstractOddsSource:
    """設定文字列からソースを生成するファクトリ。"""
    if kind == "netkeiba":
        return NetkeibaOddsSource()
    if kind == "jravan":
        return JraVanFileDropSource()
    raise ValueError(f"未対応のオッズソースです: {kind}")
