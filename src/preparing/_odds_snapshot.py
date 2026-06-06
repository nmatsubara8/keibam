"""段階オッズ スナップショットのドメインモデルと取得アダプタ。

設計方針（副作用の隔離）:
- 純粋ロジック（DTO・`minutes_to_post` 算出・フェーズ分類・重複排除・URL 構築・
  パース結果からの DTO 生成）は Playwright / bs4 に依存せず単体テスト可能。
- I/O（ブラウザ操作・HTML 取得・bs4 パース・ファイル永続化）は本モジュール内の
  アダプタに閉じ込め、HTML 取得は AbstractScraper（Playwright）を DI、bs4 は遅延 import。

`OddsSnapshot` は frozen DTO。過去の連オッズは遡及取得不可のため、ここで収集・蓄積した
スナップショット系列が Layer2（締切確定オッズ予測）の学習データになる。
"""

from __future__ import annotations

import dataclasses
import datetime as dt
from typing import Iterable
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._odds_phases import classify_phase


# netkeiba オッズページの type コード（馬券種 → ?type=bN）。
# 単勝・複勝は同一ページ(b1)に同居する。
ODDS_PAGE_TYPE = {
    BetType.TANSHO: "b1",
    BetType.FUKUSHO: "b1",
    BetType.UMAREN: "b4",
    BetType.WIDE: "b5",
    BetType.UMATAN: "b6",
    BetType.SANRENPUKU: "b7",
    BetType.SANRENTAN: "b8",
}

_ODDS_BASE_URL = "https://race.netkeiba.com/odds/index.html"


@dataclasses.dataclass(frozen=True)
class OddsSnapshot:
    """ある時点で取得した 1 馬券・1 組合せのオッズ。

    combo は馬番のタプル（順序付き馬券は順序を保持）。captured_at は取得時刻、
    minutes_to_post は締切までの残り分数、phase は分類されたフェーズ識別子。
    """

    race_id: str
    bet_type: str
    combo: tuple[int, ...]
    odds: float
    captured_at: dt.datetime
    minutes_to_post: int
    phase: str


def build_odds_url(race_id: str, bet_type: str) -> str:
    """馬券種に応じた netkeiba オッズページ URL を構築する（純粋関数）。"""
    page_type = ODDS_PAGE_TYPE.get(bet_type)
    if page_type is None:
        raise ValueError(f"未対応の馬券種です: {bet_type}")
    return f"{_ODDS_BASE_URL}?type={page_type}&race_id={race_id}"


def compute_minutes_to_post(post_time: dt.datetime, captured_at: dt.datetime) -> int:
    """締切（発走）時刻までの残り分数を算出する（純粋関数、切り捨て）。

    既に過ぎている場合は負値を返す。
    """
    delta = post_time - captured_at
    return int(delta.total_seconds() // 60)


def make_snapshot(
    race_id: str,
    bet_type: str,
    combo: Sequence[int],
    odds: float,
    post_time: dt.datetime,
    captured_at: dt.datetime,
) -> OddsSnapshot:
    """生値から OddsSnapshot を構築する（minutes_to_post・phase を付与、純粋関数）。"""
    mtp = compute_minutes_to_post(post_time, captured_at)
    return OddsSnapshot(
        race_id=race_id,
        bet_type=bet_type,
        combo=tuple(int(x) for x in combo),
        odds=float(odds),
        captured_at=captured_at,
        minutes_to_post=mtp,
        phase=classify_phase(mtp),
    )


def snapshots_from_rows(
    race_id: str,
    bet_type: str,
    rows: Iterable[tuple[Sequence[int], float]],
    post_time: dt.datetime,
    captured_at: dt.datetime,
) -> list[OddsSnapshot]:
    """(combo, odds) のリストから OddsSnapshot 群を生成する（パース結果の整形、純粋関数）。

    オッズが欠損（None / 0 以下）の行はスキップする。
    """
    out: list[OddsSnapshot] = []
    for combo, odds in rows:
        if odds is None or float(odds) <= 0:
            continue
        out.append(make_snapshot(race_id, bet_type, combo, odds, post_time, captured_at))
    return out


def _dedup_key(s: OddsSnapshot) -> tuple:
    """冪等な蓄積のための一意キー（同一レース・馬券・組合せ・フェーズは 1 件）。"""
    return (s.race_id, s.bet_type, s.combo, s.phase)


def merge_snapshots(
    existing: Sequence[OddsSnapshot], new: Sequence[OddsSnapshot]
) -> list[OddsSnapshot]:
    """既存スナップショットに新規を冪等マージする（純粋関数）。

    同一キー（race_id, bet_type, combo, phase）は新しい取得時刻のもので上書きする。
    日次の再取得・リジューム時の二重計上を防ぐ。
    """
    by_key: dict = {_dedup_key(s): s for s in existing}
    for s in new:
        key = _dedup_key(s)
        prev = by_key.get(key)
        if prev is None or s.captured_at >= prev.captured_at:
            by_key[key] = s
    return list(by_key.values())


# ---------------------------------------------------------------------------
# I/O アダプタ（selenium / bs4 / pandas を遅延 import。純粋層からは分離）
# ---------------------------------------------------------------------------


def parse_win_odds_html(html: str) -> list[tuple[tuple[int, ...], float]]:
    """単勝オッズページの HTML から (combo, odds) 行を抽出する（bs4 遅延 import）。

    netkeiba の単勝オッズテーブルは馬番セルと oods セルを持つ。DOM 変更に追従して
    調整する想定の薄いパーサ。失敗時は空リスト。
    """
    from bs4 import BeautifulSoup  # 遅延 import

    soup = BeautifulSoup(html, "lxml")
    rows: list[tuple[tuple[int, ...], float]] = []
    for tr in soup.select("tr"):
        umaban_el = tr.select_one(".Umaban, .UmabanNum, td.Umaban")
        odds_el = tr.select_one(".Odds span, td.Odds")
        if umaban_el is None or odds_el is None:
            continue
        try:
            umaban = int(umaban_el.get_text(strip=True))
            odds = float(odds_el.get_text(strip=True))
        except (ValueError, TypeError):
            continue
        rows.append(((umaban,), odds))
    return rows


class OddsSnapshotScraper:
    """段階オッズ取得アダプタ（Playwright `AbstractScraper` を DI）。

    scraper は DI（テスト時はスタブ scraper を注入）。HTML 取得とフェーズ別 DTO 化を担い、
    永続化は呼び出し側（odds_scheduler）に委譲する（単一責務）。

    Parameters
    ----------
    scraper : AbstractScraper 実装。None の場合は PlaywrightScraper を遅延生成する。
    odds_table_selector : JS 描画完了を待つ CSS セレクタ（既定はオッズテーブル）。
    """

    def __init__(self, scraper=None, odds_table_selector: str = ".RaceOdds_HorseList, .Odds") -> None:
        self._scraper = scraper
        self._odds_table_selector = odds_table_selector

    def _ensure_scraper(self):
        if self._scraper is None:
            from src.preparing._scraper import PlaywrightScraper

            self._scraper = PlaywrightScraper()
        return self._scraper

    def fetch_html(self, race_id: str, bet_type: str) -> str:
        """JS 描画されたオッズページの HTML を取得する（Playwright 同期ブリッジ）。"""
        scraper = self._ensure_scraper()
        url = build_odds_url(race_id, bet_type)
        return scraper.fetch_sync(url, wait_selector=self._odds_table_selector)

    async def fetch_html_async(self, race_id: str, bet_type: str) -> str:
        """JS 描画されたオッズページの HTML を取得する（async 直接版）。

        odds_scheduler 等の async エントリから asyncio.gather で並列取得する用途。
        """
        scraper = self._ensure_scraper()
        url = build_odds_url(race_id, bet_type)
        return await scraper.fetch(url, wait_selector=self._odds_table_selector)

    def capture(
        self, race_id: str, bet_type: str, post_time: dt.datetime, captured_at: dt.datetime | None = None
    ) -> list[OddsSnapshot]:
        """1 レース・1 馬券種のオッズを取得して OddsSnapshot 群に整形する。"""
        captured_at = captured_at or dt.datetime.now()
        html = self.fetch_html(race_id, bet_type)
        if bet_type in (BetType.TANSHO, BetType.FUKUSHO):
            rows = parse_win_odds_html(html)
        else:
            # 連系の汎用パーサは段階的に拡充（Phase B）。当面は単勝で蓄積を開始する。
            rows = []
        return snapshots_from_rows(race_id, bet_type, rows, post_time, captured_at)
