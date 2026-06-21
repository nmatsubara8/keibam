"""データ取得ソースの抽象化（netkeiba / JRA-VAN 等を差し替え可能にする）。

レース系の全データ取得（race_id 解決・結果/情報/払戻・馬ページ/血統）を
`AbstractRaceDataSource` で抽象化し、取得元を実行時に差し替えられるようにする。

- NetkeibaDataSource    — 既存の Playwright スクレイパに委譲（現行の既定）
- JraVanFileDropSource  — Windows 側エージェント/エクスポートが置くファイルを読む受信側
                          （JV-Link は Windows 専用 COM のため Linux からは直接呼べない）

各ソースは「指定 race_id/horse_id の raw データを `data/raw` の標準 pickle(+SQLite) に
反映する」契約。どの新規 race_id/馬を取りに行くか（dedup/resume）の判断は呼び出し側
（pipeline）が行い、ソースは取得（acquire）に専念する（単一責務）。

レイヤ: preparing。オッズ専用の `_odds_source.AbstractOddsSource` の上位互換的な位置づけ
（将来 odds 取得もこの DataSource に統合可能だが、当面は別契約として併存）。

## JRA-VAN ファイル連携契約（Windows 側エージェントの仕様）

Windows 機で JV-Link を購読するエージェント（別途実装）が、取得した raw データを
`data/incoming/jravan/<kind>/` に CSV/JSON で配置する。kind は以下:

    results/<race_id>.json       レース結果（着順表）。列は netkeiba results 相当。
    race_info/<race_id>.json     レース情報（コース・天候・馬場等）。
    return/<race_id>.json        払戻（券種別の的中組合せ・配当）。
    horse/<horse_id>.json        馬の基本情報・過去成績。
    peds/<horse_id>.json         血統。

各 JSON は ``{"columns": [...], "data": [[...], ...]}``（pandas split 形式）で、
読み側は `pd.DataFrame(**payload)` で復元し標準 raw スキーマへ merge する。
ファイルが無い kind は「未提供」として空でスキップ（読み側は冪等）。
"""

from __future__ import annotations

import datetime as dt
import logging
import os
from abc import ABC
from abc import abstractmethod
from typing import Sequence

logger = logging.getLogger(__name__)

JRAVAN_DATA_DIR = os.path.join("data", "incoming", "jravan")

# UI 表示用ラベル
DATA_SOURCE_LABELS = {
    "netkeiba": "netkeiba（スクレイピング）",
    "jravan": "JRA-VAN（ファイル連携・受信）",
}


class AbstractRaceDataSource(ABC):
    """レース系データ取得元の契約（race_id 解決 + raw データ取得）。"""

    name: str = "abstract"

    @abstractmethod
    def resolve_race_ids(self, date_str: str) -> list[str]:
        """開催日（YYYYMMDD）の race_id リストを返す。"""
        raise NotImplementedError

    @abstractmethod
    def acquire_races(self, race_ids: Sequence[str]) -> None:
        """指定 race_id の結果/情報/払戻を取得し data/raw に反映する。"""
        raise NotImplementedError

    @abstractmethod
    def acquire_horses(self, horse_ids: Sequence[str]) -> None:
        """指定 horse_id の馬ページ/血統を取得し data/raw に反映する。"""
        raise NotImplementedError

    def acquire_race_day_notes(self, race_ids: Sequence[str]) -> None:  # noqa: B027
        """指定 race_id の当日ノート（調教/パドック/コメント）のみを取得する。

        コア取得と独立に走らせる backfill 用フック。既定は何もしない
        （ノートを持たないソース向け）。netkeiba 等で override する。
        """
        pass

    def close(self) -> None:  # noqa: B027 — 既定は何もしない（リソース保持ソース用フック）
        pass


class NetkeibaDataSource(AbstractRaceDataSource):
    """netkeiba スクレイパへの委譲実装（現行の既定ソース）。"""

    name = "netkeiba"

    def resolve_race_ids(self, date_str: str) -> list[str]:
        from src.preparing._scrape_shutuba import scrape_race_id_race_time_list

        race_ids, _ = scrape_race_id_race_time_list(date_str)
        return [str(r) for r in (race_ids or [])]

    def acquire_races(self, race_ids: Sequence[str]) -> None:
        import pandas as pd

        from src.preparing._get_rawdata import get_rawdata_info
        from src.preparing._get_rawdata import get_rawdata_results
        from src.preparing._get_rawdata import get_rawdata_return
        from src.preparing._scrape_html_race import scrape_html_race

        ids = [str(r) for r in race_ids]
        if not ids:
            return
        scrape_html_race(pd.DataFrame({"race_id": ids}), skip=True)
        # only_ids で新規分の bin だけ再パース（全コーパス再パースを回避）
        get_rawdata_results(skip=False, only_ids=ids)
        get_rawdata_info(skip=False, only_ids=ids)
        get_rawdata_return(skip=False, only_ids=ids)
        # レース当日ノート（調教評価/パドック/厩舎コメント）。無料・リーク無し・任意。
        self._acquire_race_day_notes(ids)

    def _acquire_race_day_notes(self, ids: Sequence[str]) -> None:
        """調教評価/パドック/厩舎コメントを取得し raw pickle(+DB) に反映する。

        失敗しても本体取得を妨げないよう全例外を握りつぶす。各ページ取得の間には
        polite_interval を挟む（HourlyRateLimiter とは別の自主規制）。
        環境変数 ``KEIBA_SKIP_RACE_DAY_NOTES=1`` で丸ごと無効化できる。
        当日ノートのページは近年のみ存在するため、``KEIBA_RACE_DAY_NOTES_MIN_YEAR``
        （既定 2010）未満の開催年（race_id 先頭4桁）は取得を丸ごとスキップする。
        古い年代で 1 レース ~15s の空振り（セレクタ待ち×3種）を避けるための年代ゲート。
        """
        import os
        import time

        if os.environ.get("KEIBA_SKIP_RACE_DAY_NOTES") == "1":
            return
        try:
            min_year = int(os.environ.get("KEIBA_RACE_DAY_NOTES_MIN_YEAR", "2010"))
        except ValueError:
            min_year = 2010

        def _race_year(rid: str) -> "int | None":
            s = str(rid)
            return int(s[:4]) if len(s) >= 4 and s[:4].isdigit() else None

        target_ids = [r for r in ids if (_race_year(r) or min_year) >= min_year]
        skipped = len(ids) - len(target_ids)
        if skipped:
            logger.info(
                "race_day_notes: 年代ゲートで %d/%d レースをスキップ（開催年 < %d）",
                skipped, len(ids), min_year,
            )
        if not target_ids:
            return
        try:
            from src.constants._local_paths import LocalPaths
            from src.preparing._race_day_notes import RaceDayNotesScraper
            from src.preparing._race_day_notes import persist_notes
            from src.preparing._rate_limiter import polite_interval
        except Exception as e:  # noqa: BLE001
            logger.warning("race_day_notes: モジュール読込失敗のためスキップ: %s", e)
            return

        path_by_type = {
            "training": LocalPaths.RAW_TRAINING_PATH,
            "paddock": LocalPaths.RAW_PADDOCK_PATH,
            "comment": LocalPaths.RAW_COMMENT_PATH,
        }
        scraper = RaceDayNotesScraper()
        first = True
        for race_id in target_ids:
            for note_type, path in path_by_type.items():
                if not first:
                    interval = polite_interval()
                    if interval > 0:
                        time.sleep(interval)
                first = False
                try:
                    df = scraper.capture(race_id, note_type)
                    persist_notes(df, path)
                except Exception as e:  # noqa: BLE001 — 1ページの失敗で全体を止めない
                    logger.warning(
                        "race_day_notes 失敗 type=%s race_id=%s: %s", note_type, race_id, e
                    )

    def acquire_race_day_notes(self, race_ids: Sequence[str]) -> None:
        """当日ノートのみを取得する公開フック（backfill-notes ジョブ用）。"""
        ids = [str(r) for r in race_ids]
        if ids:
            self._acquire_race_day_notes(ids)

    def acquire_horses(self, horse_ids: Sequence[str]) -> None:
        from src.preparing._get_rawdata import get_rawdata_horse_info
        from src.preparing._get_rawdata import get_rawdata_horse_results
        from src.preparing._get_rawdata import get_rawdata_peds
        from src.preparing._scrape_html_horse import scrape_html_horse_with_master
        from src.preparing._scrape_html_ped import scrape_html_ped

        ids = [str(h) for h in horse_ids]
        if not ids:
            return
        scrape_html_horse_with_master(ids, skip=True)
        scrape_html_ped(ids, skip=True)
        get_rawdata_horse_results(skip=False, only_ids=ids)
        get_rawdata_horse_info(skip=False, only_ids=ids)
        get_rawdata_peds(skip=False, only_ids=ids)


class JraVanFileDropSource(AbstractRaceDataSource):
    """JRA-VAN（Windows エージェント経由のファイル連携）の受信実装。

    モジュール docstring の契約に従い `data/incoming/jravan/<kind>/<id>.json` を読み、
    標準 raw pickle(+SQLite) へ merge する。ファイルが無い kind はスキップ（冪等）。
    速報オッズの受信は別契約（`_odds_source.JraVanFileDropSource`）。
    """

    name = "jravan"

    # kind（取得種別）→ (サブディレクトリ, 標準 raw pickle パス属性名, DB alias)
    _RACE_KINDS = ("results", "race_info", "return")
    _HORSE_KINDS = ("horse_results", "horse_info", "peds")

    def __init__(self, incoming_dir: str = JRAVAN_DATA_DIR) -> None:
        self._dir = incoming_dir

    def _read_kind(self, kind: str, ids: Sequence[str]):
        """`<dir>/<kind>/<id>.json`（pandas split 形式）を読み DataFrame を返す。"""
        import pandas as pd

        sub = os.path.join(self._dir, kind)
        if not os.path.isdir(sub):
            return pd.DataFrame()
        frames = []
        for _id in ids:
            path = os.path.join(sub, f"{_id}.json")
            if not os.path.exists(path):
                continue
            try:
                frames.append(pd.read_json(path, orient="split"))
            except Exception as e:  # noqa: BLE001
                logger.warning("[jravan] 読込失敗 %s: %s", path, e)
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    def _merge_to_raw(self, kind_to_path: dict, ids: Sequence[str]) -> None:
        from src.preparing._get_rawdata import update_rawdata

        for kind, path in kind_to_path.items():
            df = self._read_kind(kind, ids)
            if df.empty:
                logger.info("[jravan] %s: 受信ファイルなし（スキップ）", kind)
                continue
            update_rawdata(path, df)
            logger.info("[jravan] %s: %d 行を取り込み", kind, len(df))

    def resolve_race_ids(self, date_str: str) -> list[str]:
        """results ディレクトリのファイル名（race_id）から当日分を返す。"""
        sub = os.path.join(self._dir, "results")
        if not os.path.isdir(sub):
            return []
        return sorted(
            os.path.splitext(f)[0]
            for f in os.listdir(sub)
            if f.endswith(".json") and os.path.splitext(f)[0].startswith(date_str[:8])
        )

    def acquire_races(self, race_ids: Sequence[str]) -> None:
        from src.constants._local_paths import LocalPaths

        self._merge_to_raw(
            {
                "results": LocalPaths.RAW_RESULTS_PATH,
                "race_info": LocalPaths.RAW_RACE_INFO_PATH,
                "return": LocalPaths.RAW_RETURN_TABLES_PATH,
            },
            [str(r) for r in race_ids],
        )

    def acquire_horses(self, horse_ids: Sequence[str]) -> None:
        from src.constants._local_paths import LocalPaths

        self._merge_to_raw(
            {
                "horse_results": LocalPaths.RAW_HORSE_RESULTS_PATH,
                "horse_info": LocalPaths.RAW_HORSE_INFO_PATH,
                "peds": LocalPaths.RAW_PEDS_PATH,
            },
            [str(h) for h in horse_ids],
        )


_REGISTRY = {
    "netkeiba": NetkeibaDataSource,
    "jravan": JraVanFileDropSource,
}


def available_data_sources() -> list[str]:
    """登録済みデータソース名の一覧（UI 選択肢用）。"""
    return list(_REGISTRY)


def create_data_source(kind: str = "netkeiba") -> AbstractRaceDataSource:
    """設定文字列からデータソースを生成するファクトリ。"""
    cls = _REGISTRY.get(kind)
    if cls is None:
        raise ValueError(f"未対応のデータソースです: {kind}（対応: {available_data_sources()}）")
    return cls()


# ---------------------------------------------------------------------------
# 選択中ソースの永続化（UI ←→ CLI 共有）
# ---------------------------------------------------------------------------

SELECTED_SOURCE_PATH = os.path.join("models", "selected_data_source.json")


def load_selected_source(path: str = SELECTED_SOURCE_PATH) -> str:
    """保存済みの選択データソース名を返す（無ければ "netkeiba"）。"""
    import json

    if not os.path.exists(path):
        return "netkeiba"
    try:
        with open(path) as f:
            return str(json.load(f).get("data_source", "netkeiba"))
    except Exception:  # noqa: BLE001
        return "netkeiba"


def save_selected_source(kind: str, path: str = SELECTED_SOURCE_PATH) -> None:
    """選択データソース名を保存する（UI からの選択を CLI/pipeline が参照）。"""
    import json

    if kind not in _REGISTRY:
        raise ValueError(f"未対応のデータソースです: {kind}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump({"data_source": kind, "saved_at": dt.datetime.now().isoformat()}, f)
