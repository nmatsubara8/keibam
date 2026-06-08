"""race HTML（bin）から各種テーブルを生成する高水準ラッパー。

main.ipynb から呼ぶ `get_rawdata_results` / `get_rawdata_info` /
`get_rawdata_return` を提供する。いずれも ./data/html/race/ に保存済みの
race HTML（bin）を TableCreator で解析し、pkl に保存後 DataFrame を返す。

html_files_race 引数はインターフェース互換のために受け取るが、解析対象は
TableCreator が from_local_location（race/）配下の bin を直接走査する。
skip=True かつ既存の出力 pkl があれば再解析せずキャッシュを返す。
"""

import os

import pandas as pd

from src.constants._url_paths import UrlPaths
from src.preparing.table_creator import TableCreator


def _create_table(alias: str, create_method: str, skip: bool) -> pd.DataFrame:
    url_paths = UrlPaths()
    # alias に対応する UrlPaths タプルから出力 pkl パスを求める
    attr = None
    for field in url_paths.__dataclass_fields__:
        val = getattr(url_paths, field)
        if isinstance(val, tuple) and len(val) > 0 and val[0] == alias:
            attr = val
            break
    if attr is None:
        raise ValueError(f"unknown alias: {alias}")
    save_path = os.path.join(attr[4], attr[5])

    if skip and os.path.exists(save_path):
        return pd.read_pickle(save_path)

    creator = TableCreator()
    creator.set_args(alias)
    getattr(creator, create_method)()
    if not os.path.exists(save_path):
        import logging
        logging.getLogger(__name__).warning(
            "%s: 出力 pkl が作成されませんでした（処理対象ファイルなし）", alias
        )
        return pd.DataFrame()
    return pd.read_pickle(save_path)


def get_rawdata_results(html_files_race=None, skip: bool = False) -> pd.DataFrame:
    """race HTML からレース結果テーブルを生成する（results.pkl）。"""
    return _create_table("race_results_table", "create_race_results_table", skip)


def get_rawdata_info(html_files_race=None, skip: bool = False) -> pd.DataFrame:
    """race HTML からレース情報テーブルを生成する（race_info.pkl）。"""
    return _create_table("race_info_table", "create_race_info_table", skip)


def get_rawdata_return(html_files_race=None, skip: bool = False) -> pd.DataFrame:
    """race HTML から払戻テーブルを生成する（return_tables.pkl）。"""
    return _create_table("race_return_table", "create_race_return_table", skip)


def get_rawdata_horse_info(html_files_horse=None, skip: bool = False) -> pd.DataFrame:
    """horse HTML から馬の基本情報テーブルを生成する（horse_info.pkl）。"""
    return _create_table("horse_info_table", "create_horse_info_table", skip)


def get_rawdata_horse_results(html_files_horse=None, skip: bool = False) -> pd.DataFrame:
    """horse HTML から馬の過去成績テーブルを生成する（horse_results.pkl）。"""
    return _create_table("horse_results_table", "create_horse_results_table", skip)


def update_rawdata(filepath: str, new_df: pd.DataFrame) -> None:
    """既存の pkl テーブルに new_df をマージして上書き保存する。

    filepath の pkl が存在する場合は new_df に含まれない旧インデックスを保持したまま
    マージする（重複インデックスは new_df を優先）。存在しない場合は新規作成。
    """
    import logging
    _logger = logging.getLogger(__name__)

    if new_df is None or new_df.empty:
        _logger.warning("update_rawdata: new_df が空のためスキップ (%s)", filepath)
        return

    if os.path.isfile(filepath):
        backup = filepath + ".bak"
        existing = pd.read_pickle(filepath)
        filtered_old = existing[~existing.index.isin(new_df.index)]
        updated = pd.concat([filtered_old, new_df])
        if os.path.isfile(backup):
            os.remove(backup)
        os.rename(filepath, backup)
        updated.to_pickle(filepath)
    else:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        new_df.to_pickle(filepath)
