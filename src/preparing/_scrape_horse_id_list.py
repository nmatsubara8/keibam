"""馬IDリストのスクレイピング（差分ダウンロード対応）。"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)


def scrape_horse_id_list(race_id_list=None, skip: bool = False):
    """race_id_listから馬IDリストを取得して返す。

    Parameters
    ----------
    race_id_list : DataFrame, optional
        scrape_race_id_list() の戻り値。
    skip : bool
        True かつ pkl 存在の場合、スクレイピングを完全省略する。
    """
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader
    from src.preparing.DataLoader import DataLoader

    url_paths = UrlPaths()
    hl = url_paths.HORSE_LIST_URL
    save_path = os.path.join(DataLoader._abs(hl[4]), hl[5])

    if os.path.exists(save_path):
        if skip:
            logger.info("horse_id_list: skip=True かつ pkl 存在 → キャッシュを返す")
            return pd.read_pickle(save_path)

    _abs = DataLoader._abs
    if race_id_list is not None:
        if not isinstance(race_id_list, pd.DataFrame):
            race_id_list = pd.DataFrame({"race_id": list(race_id_list)})
        if len(race_id_list) == 0:
            raise ValueError(
                "scrape_horse_id_list: race_id_list が空です。"
                " 先に scrape_race_id_list で race_id を取得してください。"
            )
        input_pkl_path = os.path.join(_abs(hl[7]), hl[8])
        os.makedirs(_abs(hl[7]), exist_ok=True)
        race_id_list.to_pickle(input_pkl_path)

    loader = KaisaiDateLoader(
        alias=hl[0],
        from_location=hl[1],
        to_temp_location=_abs(hl[2]),
        temp_save_file_name=hl[3],
        to_location=_abs(hl[4]),
        save_file_name=hl[5],
        batch_size=hl[6],
        from_local_location=_abs(hl[7]),
        from_local_file_name=hl[8],
    )
    loader.scrape_horse_id_list()
    return pd.read_pickle(save_path)
