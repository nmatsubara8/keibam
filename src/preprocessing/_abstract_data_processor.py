from __future__ import annotations

import logging
import os
from abc import ABCMeta
from abc import abstractmethod

import pandas as pd

logger = logging.getLogger(__name__)


class AbstractDataProcessor(metaclass=ABCMeta):
    def __init__(self, filepath: str):
        self.__raw_data = self._load_raw(filepath)
        self.__preprocessed_data = self._preprocess()

    @staticmethod
    def _load_raw(filepath: str) -> pd.DataFrame:
        """pickle を読む。存在しない場合は DB からリストアしてキャッシュを再生成する。"""
        try:
            return pd.read_pickle(filepath)
        except FileNotFoundError:
            pass

        from src.storage._db import PICKLE_PATH_TO_ALIAS
        from src.storage._repo import RawDataRepo

        alias = PICKLE_PATH_TO_ALIAS.get(filepath)
        if alias is None:
            raise FileNotFoundError(
                f"pickle が見つからず、DB alias も不明です: {filepath}"
            )

        logger.warning(
            "[AbstractDataProcessor] %s が見つかりません。DB(%s) からリストアします。",
            filepath, alias,
        )
        df = RawDataRepo().read(alias)
        if df.empty:
            raise FileNotFoundError(
                f"pickle {filepath} がなく DB({alias}) にもデータがありません"
            )

        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        df.to_pickle(filepath)
        logger.info("[AbstractDataProcessor] %s を DB から再生成しました (shape=%s)", filepath, df.shape)
        return df

    @abstractmethod
    def _preprocess(self):
        pass

    @property
    def raw_data(self):
        # 列名を変換
        new_columns = {col: int(col) if str(col).isdigit() else col for col in self.__raw_data.columns}
        self.__raw_data.rename(columns=new_columns, inplace=True)
        return self.__raw_data.copy()

    @property
    def preprocessed_data(self):
        return self.__preprocessed_data.copy()

    # rawデータを一つのファイルにまとめる運用に変更したため、以下は不要
    """def _delete_duplicate(self, old, new):
        filtered_old = old[~old.index.isin(new.index)]
        return pd.concat([filtered_old, new])

    def _read_pickle(self, path_list):
        df = pd.read_pickle(path_list[0])
        for path in path_list[1:]:
            df = self._delete_duplicate(df, pd.read_pickle(path))
        return df"""
