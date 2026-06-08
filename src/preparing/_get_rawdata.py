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
