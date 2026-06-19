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
import logging
import os
import re
from typing import Iterable
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._odds_phases import classify_phase

logger = logging.getLogger(__name__)


# netkeiba オッズページの type コード（馬券種 → ?type=bN）。
# 単勝・複勝は同一ページ(b1)に同居する。枠連は b3。
ODDS_PAGE_TYPE = {
    BetType.TANSHO: "b1",
    BetType.FUKUSHO: "b1",
    BetType.WAKUREN: "b3",
    BetType.UMAREN: "b4",
    BetType.WIDE: "b5",
    BetType.UMATAN: "b6",
    BetType.SANRENPUKU: "b7",
    BetType.SANRENTAN: "b8",
}

# オッズセルの id 属性 `odds-<type>-<馬番列>` の type コード（馬券種 → N）。
# ページの type=bN とは異なり、単勝(1)・複勝(2)は b1 ページ内で別コードを持つ。
# 枠連(3) は馬番ではなく枠番の組合せを表す（払戻側も枠単位）。
ODDS_ID_TYPE = {
    BetType.TANSHO: "1",
    BetType.FUKUSHO: "2",
    BetType.WAKUREN: "3",
    BetType.UMAREN: "4",
    BetType.WIDE: "5",
    BetType.UMATAN: "6",
    BetType.SANRENPUKU: "7",
    BetType.SANRENTAN: "8",
}

# 馬券種ごとの id 抽出パターン（パース時の再コンパイルを避けるため事前コンパイル）
_ODDS_ID_RE = {
    bet_type: re.compile(rf"^odds-{code}-(\d+)$") for bet_type, code in ODDS_ID_TYPE.items()
}

# オッズ値（"12.3" / レンジ "1.5 - 2.0"）の数値部分
_ODDS_VALUE_RE = re.compile(r"\d+(?:\.\d+)?")

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


def combo_to_str(combo: Sequence[int]) -> str:
    """combo タプルを DB 保存用の文字列（例 ``"3-7-11"``）へ変換する（純粋関数）。"""
    return "-".join(str(int(x)) for x in combo)


def build_final_odds_lookup(
    snapshots: Sequence[OddsSnapshot], bet_types: Sequence[str] | None = None
) -> dict[tuple[str, str, str], float]:
    """スナップショット群から確定オッズ lookup を構築する（純粋関数）。

    キーは ``(race_id, bet_type, combo_key)``、値は最新 captured_at のオッズ。
    combo_key は `canonical_combo`（順不同は昇順正規化）で EV 選定時の combo と一致させる。
    fetch-final-odds で取得した実績オッズを `StoredFinalOddsProvider` に渡す用途。

    bet_types を指定すると当該券種だけに絞る。
    """
    from src.constants._bet_types import combo_key

    allow = set(bet_types) if bet_types is not None else None
    best: dict[tuple[str, str, str], tuple[dt.datetime, float]] = {}
    for s in snapshots:
        if allow is not None and s.bet_type not in allow:
            continue
        if s.odds is None or float(s.odds) <= 0:
            continue
        key = (str(s.race_id), s.bet_type, combo_key(s.bet_type, s.combo))
        prev = best.get(key)
        if prev is None or s.captured_at >= prev[0]:
            best[key] = (s.captured_at, float(s.odds))
    return {k: v[1] for k, v in best.items()}


def snapshots_to_records(snapshots: Sequence[OddsSnapshot]) -> list[dict]:
    """OddsSnapshot 群を `raw_odds_snapshots` テーブルの行 dict へ変換する（純粋関数）。

    combo は文字列、captured_at は ISO8601 文字列にして SQLite に保存できる形にする。
    """
    return [
        {
            "race_id": s.race_id,
            "bet_type": s.bet_type,
            "combo": combo_to_str(s.combo),
            "odds": s.odds,
            "captured_at": s.captured_at.isoformat(),
            "minutes_to_post": s.minutes_to_post,
            "phase": s.phase,
        }
        for s in snapshots
    ]


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


def parse_odds_value(text: str) -> float | None:
    """オッズセルのテキストからオッズ値を取り出す（純粋関数）。

    ワイド・複勝は ``"1.5 - 2.0"`` のようなレンジ表記になるため、保守的に
    最小値（下限）を採用する（期待値計算で過大評価しないため）。
    数値が取れない（"---" 等の未確定表示）場合は None。
    """
    nums = _ODDS_VALUE_RE.findall(text)
    if not nums:
        return None
    return float(nums[0])


def _split_combo_digits(digits: str) -> tuple[int, ...] | None:
    """id 末尾の馬番列（2 桁ずつ連結、例 ``"010203"``）を馬番タプルに分解する（純粋関数）。"""
    if not digits or len(digits) % 2 != 0:
        return None
    combo = tuple(int(digits[i : i + 2]) for i in range(0, len(digits), 2))
    if any(n <= 0 for n in combo):
        return None
    return combo


def parse_combo_odds_html(html: str, bet_type: str) -> list[tuple[tuple[int, ...], float]]:
    """オッズページの HTML から指定馬券種の (combo, odds) 行を抽出する（bs4 遅延 import）。

    netkeiba のオッズセルは ``id="odds-<type>-<馬番列>"``（例: 馬連 ``odds-4-0102``、
    三連単 ``odds-8-010203``）を持つため、id ベースで全馬券種を一様にパースできる。
    馬番列は 2 桁ずつの連結で、順序付き馬券（馬単・三連単）は並び順を保持する。
    同一 combo が複数回現れた場合（一覧と人気順表示の重複等）は最初の値を採用する。
    失敗時・未確定（"---"）の行はスキップする。
    """
    from bs4 import BeautifulSoup  # 遅延 import

    id_re = _ODDS_ID_RE.get(bet_type)
    if id_re is None:
        raise ValueError(f"未対応の馬券種です: {bet_type}")

    soup = BeautifulSoup(html, "lxml")
    seen: dict[tuple[int, ...], float] = {}
    for el in soup.find_all(id=id_re):
        m = id_re.match(str(el.get("id")))
        if m is None:
            continue
        combo = _split_combo_digits(m.group(1))
        if combo is None or combo in seen:
            continue
        odds = parse_odds_value(el.get_text(strip=True))
        if odds is None:
            continue
        seen[combo] = odds
    return list(seen.items())


class OddsSnapshotScraper:
    """段階オッズ取得アダプタ（Playwright `AbstractScraper` を DI）。

    scraper は DI（テスト時はスタブ scraper を注入）。HTML 取得とフェーズ別 DTO 化を担い、
    永続化は呼び出し側（odds_scheduler）に委譲する（単一責務）。

    Parameters
    ----------
    scraper : AbstractScraper 実装。None の場合は PlaywrightScraper を遅延生成する。
    odds_table_selector : JS 描画完了を待つ CSS セレクタ（既定はオッズセル id）。

    Notes
    -----
    連系（馬連〜三連単）のオッズページは組合せ数が多く JS 描画に時間がかかるため、
    既定 PlaywrightScraper の短いセレクタ待ち（3 秒）では描画前に空 HTML を返し
    パース 0 件になりやすい。本アダプタは待機セレクタを実オッズセル
    （``[id^=odds-]``）にし、タイムアウトを延長する（環境変数で調整可）:
        KEIBA_ODDS_TIMEOUT_MS           ページ遷移タイムアウト（既定 45000）
        KEIBA_ODDS_SELECTOR_TIMEOUT_MS  オッズセル描画待ち（既定 15000）
    """

    def __init__(
        self, scraper=None, odds_table_selector: str = "[id^=odds-], .RaceOdds_HorseList, .Odds"
    ) -> None:
        self._scraper = scraper
        self._odds_table_selector = odds_table_selector

    def _ensure_scraper(self):
        if self._scraper is None:
            import os

            from src.preparing._scraper import PlaywrightScraper

            self._scraper = PlaywrightScraper(
                timeout_ms=int(os.environ.get("KEIBA_ODDS_TIMEOUT_MS", "45000")),
                selector_timeout_ms=int(os.environ.get("KEIBA_ODDS_SELECTOR_TIMEOUT_MS", "15000")),
            )
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
        """1 レース・1 馬券種のオッズを取得して OddsSnapshot 群に整形する。

        まず id ベースの汎用パーサ（全馬券種対応）を試し、id 属性が無い旧 DOM の
        単勝・複勝ページではクラスベースの `parse_win_odds_html` にフォールバックする。
        """
        captured_at = captured_at or dt.datetime.now()
        html = self.fetch_html(race_id, bet_type)
        rows = parse_combo_odds_html(html, bet_type)
        if not rows and bet_type in (BetType.TANSHO, BetType.FUKUSHO):
            rows = parse_win_odds_html(html)
        if not rows:
            self._diagnose_empty(race_id, bet_type, html)
        return snapshots_from_rows(race_id, bet_type, rows, post_time, captured_at)

    @staticmethod
    def _diagnose_empty(race_id: str, bet_type: str, html: str) -> None:
        """取得 0 件のとき、生 HTML を解析して原因の切り分け情報を出す。

        KEIBA_ODDS_DEBUG=1 のときのみ動作（通常運用ではノイズを出さない）:
        - 生 HTML に ``id="odds-"`` セルがあるのにパース 0 件 → パーサ/セレクタ問題。
        - 無い + 「終了/提供前」等の文言 → ページが確定オッズを配信していない。
        KEIBA_ODDS_DEBUG_DIR 指定時は HTML をファイルへダンプして目視確認できる。
        """
        if os.environ.get("KEIBA_ODDS_DEBUG") not in ("1", "true", "True"):
            return
        has_odds_id = 'id="odds-' in html
        markers = [m for m in ("オッズ", "発売", "確定", "終了", "提供") if m in html]
        logger.warning(
            "[odds-debug] race=%s bet=%s 0件: html_len=%d odds_idセル=%s 文言=%s",
            race_id, bet_type, len(html), has_odds_id, markers,
        )
        dump_dir = os.environ.get("KEIBA_ODDS_DEBUG_DIR")
        if dump_dir:
            os.makedirs(dump_dir, exist_ok=True)
            path = os.path.join(dump_dir, f"{race_id}_{bet_type}.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            logger.warning("[odds-debug] HTML をダンプ: %s", path)
