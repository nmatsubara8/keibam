"""破損した race_id（連番化）を HTML キャッシュから復旧する。

data/html/race_results・race_return のキャッシュHTML（ファイル名=race_id）を再パースし、
正しい race_id を持つ results.pkl / return_tables.pkl を作り直して DB を入れ直す。
netkeiba へは一切アクセスしない（キャッシュ再処理のみ）。

破損の原因: RawDataRepo.upsert が既定 RangeIndex を race_id に昇格させ、行番号(0,1,2…)が
race_id として保存されていた（本コミットでガード済み）。本スクリプトは既に壊れた正本を
キャッシュから作り直す。

安全策:
- 既定は --dry-run（現状調査のみ。破壊操作なし）。
- --execute 指定時のみ実行。実行前に results.pkl / return_tables.pkl / DB を *.bak へ退避。
- transfer_temp_file は既存pickleとマージし破損行を温存するため、再パース前に正本と一時を削除。

使い方:
  python recover_from_html_cache.py              # 調査のみ（dry-run）
  python recover_from_html_cache.py --execute    # 退避→クリア→再パース→DB再投入→検証
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import shutil

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

# results / return はどちらも race ページHTML（data/html/race/*.bin）を入力に再パースする。
# data/html/race_results・race_return は「出力」側なので入力件数の指標にはならない。
RACE_HTML_DIR = "data/html/race"


def _count_cached(dir_path: str) -> int:
    if not os.path.isdir(dir_path):
        return 0
    return len([f for f in os.listdir(dir_path) if not f.startswith(".")])


def _race_id_health(df: pd.DataFrame) -> dict:
    """race_id（index or 列）の健全性を測る。12桁のレースIDが揃っているか。"""
    if df is None or df.empty:
        return {"rows": 0, "races": 0, "valid12": 0, "bad": 0}
    ids = df.index.astype(str) if df.index.name == "race_id" else (
        df["race_id"].astype(str) if "race_id" in df.columns else pd.Series([], dtype=str)
    )
    valid12 = int((ids.str.len() == 12).sum())
    bad = int((ids.str.len() != 12).sum())
    return {"rows": len(df), "races": ids.nunique(), "valid12": valid12, "bad": bad}


def _backup(path: str) -> None:
    if os.path.exists(path):
        bak = path + ".bak"
        shutil.copy2(path, bak)
        logger.info("[recover] 退避: %s → %s", path, bak)


def _clear_pickle_and_temp(raw_path: str, temp_glob: str) -> None:
    if os.path.exists(raw_path):
        os.remove(raw_path)
        logger.info("[recover] 削除（正本）: %s", raw_path)
    for tmp in glob.glob(temp_glob):
        os.remove(tmp)
        logger.info("[recover] 削除（一時）: %s", tmp)


def _investigate() -> None:
    print("=" * 72)
    print("HTML キャッシュ復旧 — 現状調査（dry-run）")
    print("=" * 72)
    print("\n■ race HTML キャッシュ件数（results/return の再パース入力。ローカル読込のみ）")
    print(f"  race(.bin)   {_count_cached(RACE_HTML_DIR)} 件  ({RACE_HTML_DIR})")

    print("\n■ 現在の正本（results.pkl / return_tables.pkl）の race_id 健全性")
    # race_info は date 列の供給源（merge の groupby を支える）。少数だと featured が
    # 数十レースに化けるため、results/return と同様に健全性を確認する。
    for label, path, alias in [
        ("results", LocalPaths.RAW_RESULTS_PATH, "raw_results"),
        ("race_info", LocalPaths.RAW_RACE_INFO_PATH, "raw_race_info"),
        ("return_tables", LocalPaths.RAW_RETURN_TABLES_PATH, "raw_return_tables"),
    ]:
        df = pd.read_pickle(path) if os.path.exists(path) else pd.DataFrame()
        h = _race_id_health(df)
        dbh = _db_race_id_health(alias)
        print(f"  {label:<14} pkl行={h['rows']} pklレース={h['races']} 非12桁={h['bad']}"
              f"  |  DBレース={dbh['races']} DB非12桁={dbh['bad']}")

    print("\n→ レース数が極端に少ない/非12桁が多いテーブルは復旧対象。--execute で作り直します。")
    print("   （results/return が健全でも race_info が少数なら featured が数十レースに化けます）")
    print("=" * 72)


def _rebuild_one(alias_label: str, getter, raw_path: str, temp_glob: str, db_alias: str) -> pd.DataFrame:
    from src.storage import RawDataRepo

    _backup(raw_path)
    _clear_pickle_and_temp(raw_path, temp_glob)
    logger.info("[recover] %s: HTML キャッシュから再パース中…", alias_label)
    df = getter(skip=False)  # skip=False で全 bin を再パース（既存pkl無視）
    h = _race_id_health(df)
    logger.info(
        "[recover] %s 再生成: 行=%d レース=%d 12桁=%d 非12桁=%d",
        alias_label, h["rows"], h["races"], h["valid12"], h["bad"],
    )
    # DB を作り直す（破損行を消してから clean pickle を投入）
    repo = RawDataRepo()
    repo.clear(db_alias)
    if not df.empty:
        # to_raw_format 相当: race_id を列に保持した正準形式で upsert
        from src.pipeline._ingestion import save_raw
        save_raw(df, raw_path)  # pickle 保存 + DB upsert
    logger.info("[recover] %s: DB(%s) を作り直しました", alias_label, db_alias)
    return df


def _db_race_id_health(alias: str) -> dict:
    """DB 上の raw テーブルの race_id 健全性（再実行ガード用）。"""
    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        if repo.has_rows(alias):
            return _race_id_health(repo.read(alias))
    except Exception as e:  # noqa: BLE001
        logger.warning("[recover] DB(%s) 健全性チェック失敗: %s", alias, e)
    return {"rows": 0, "races": 0, "valid12": 0, "bad": 0}


def _table_healthy(alias: str) -> bool:
    """DB の当該テーブルが健全（race_id 破損0・1000レース超）か。"""
    h = _db_race_id_health(alias)
    return h["bad"] == 0 and h["races"] > 1000


def _execute(force: bool = False) -> None:
    from src.preparing._get_rawdata import get_rawdata_info
    from src.preparing._get_rawdata import get_rawdata_results
    from src.preparing._get_rawdata import get_rawdata_return

    # data/html/race から再パースできる3テーブル（いずれも race_id をファイル名から付与）。
    # race_info は merge の groupby("date") を支える「date」列の供給源で、これが古いと
    # 全レースが少数に化ける（results が大量でも featured が数十レースになる）。
    targets = [
        ("results", get_rawdata_results, LocalPaths.RAW_RESULTS_PATH,
         "data/tmp/race_results*/*", "raw_results"),
        ("race_info", get_rawdata_info, LocalPaths.RAW_RACE_INFO_PATH,
         "data/tmp/race_info*/*", "raw_race_info"),
        ("return_tables", get_rawdata_return, LocalPaths.RAW_RETURN_TABLES_PATH,
         "data/tmp/race_return*/*", "raw_return_tables"),
    ]

    # 既に健全なテーブルはスキップ（--force で全再構築）。テーブル単位の冪等性。
    todo = []
    for label, getter, path, tmp, alias in targets:
        if not force and _table_healthy(alias):
            h = _db_race_id_health(alias)
            logger.info("[recover] %s は既に健全（%d レース、破損0）→ スキップ", label, h["races"])
        else:
            todo.append((label, getter, path, tmp, alias))

    if not todo:
        logger.warning("[recover] 対象テーブルは全て健全です。再構築不要（やり直すなら --force）。")
        return

    n_cache = _count_cached(RACE_HTML_DIR)
    if n_cache == 0:
        logger.error("[recover] race HTMLキャッシュ(%s)が空です。復旧できません", RACE_HTML_DIR)
        return
    logger.info(
        "[recover] %d テーブルを race HTMLキャッシュ %d 件からローカル再パースします: %s"
        "（netkeiba 非アクセス。1テーブルあたり数十分〜の場合あり）",
        len(todo), n_cache, [t[0] for t in todo],
    )

    # DB ファイルも退避（初回のみ）
    if os.path.exists(LocalPaths.DB_PATH) and not os.path.exists(LocalPaths.DB_PATH + ".bak"):
        _backup(LocalPaths.DB_PATH)

    results: dict = {}
    for label, getter, path, tmp, alias in todo:
        results[label] = _rebuild_one(label, getter, path, tmp, alias)

    print("\n" + "=" * 72)
    all_ok = True
    for label, df in results.items():
        h = _race_id_health(df)
        ok = h["bad"] == 0 and h["races"] > 100
        all_ok = all_ok and ok
        print(f"{label:<14} 行={h['rows']} レース={h['races']} 非12桁={h['bad']}")
    print(f"結果: {'✅ 復旧成功（race_id 正常）' if all_ok else '⚠ 要確認（非12桁残存 or レース数僅少）'}")
    print("次: rebuild-featured → calibrate-takeout --dry-run で重なりを確認")
    print("=" * 72)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="HTML キャッシュから race_id 破損を復旧")
    ap.add_argument("--execute", action="store_true", help="実際に復旧する（既定は調査のみ）")
    ap.add_argument(
        "--force", action="store_true",
        help="DB が既に健全でも強制的に再パース・再構築する",
    )
    args = ap.parse_args()
    if args.execute:
        _execute(force=args.force)
    else:
        _investigate()


if __name__ == "__main__":
    main()
