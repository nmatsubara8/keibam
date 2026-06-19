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

HTML_DIRS = {
    "race_results": "data/html/race_results",
    "race_return": "data/html/race_return",
}


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
    print("\n■ HTML キャッシュ件数")
    for name, d in HTML_DIRS.items():
        print(f"  {name:<14} {_count_cached(d)} 件  ({d})")

    print("\n■ 現在の正本（results.pkl / return_tables.pkl）の race_id 健全性")
    for label, path in [
        ("results", LocalPaths.RAW_RESULTS_PATH),
        ("return_tables", LocalPaths.RAW_RETURN_TABLES_PATH),
    ]:
        df = pd.read_pickle(path) if os.path.exists(path) else pd.DataFrame()
        h = _race_id_health(df)
        print(f"  {label:<14} 行={h['rows']} レース={h['races']} "
              f"12桁={h['valid12']} 非12桁(破損)={h['bad']}")

    print("\n→ 非12桁(破損)が多ければ復旧対象。--execute で HTML から作り直します。")
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


def _execute() -> None:
    from src.preparing._get_rawdata import get_rawdata_results
    from src.preparing._get_rawdata import get_rawdata_return

    if _count_cached(HTML_DIRS["race_results"]) == 0:
        logger.error("[recover] race_results のHTMLキャッシュが空です。復旧できません")
        return

    # DB ファイルも退避
    if os.path.exists(LocalPaths.DB_PATH):
        _backup(LocalPaths.DB_PATH)

    res = _rebuild_one(
        "results", get_rawdata_results, LocalPaths.RAW_RESULTS_PATH,
        "data/tmp/race_results*/*", "raw_results",
    )
    ret = _rebuild_one(
        "return_tables", get_rawdata_return, LocalPaths.RAW_RETURN_TABLES_PATH,
        "data/tmp/race_return*/*", "raw_return_tables",
    )

    print("\n" + "=" * 72)
    rh, th = _race_id_health(res), _race_id_health(ret)
    ok = rh["bad"] == 0 and rh["races"] > 100 and th["bad"] == 0
    print(f"results       行={rh['rows']} レース={rh['races']} 非12桁={rh['bad']}")
    print(f"return_tables 行={th['rows']} レース={th['races']} 非12桁={th['bad']}")
    print(f"結果: {'✅ 復旧成功（race_id 正常）' if ok else '⚠ 要確認（非12桁が残存 or レース数が少ない）'}")
    print("次: python -m src.pipeline.run_pipeline calibrate-takeout --dry-run で重なりを確認")
    print("=" * 72)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="HTML キャッシュから race_id 破損を復旧")
    ap.add_argument("--execute", action="store_true", help="実際に復旧する（既定は調査のみ）")
    args = ap.parse_args()
    if args.execute:
        _execute()
    else:
        _investigate()


if __name__ == "__main__":
    main()
