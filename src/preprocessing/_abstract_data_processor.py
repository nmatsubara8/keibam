from __future__ import annotations

from abc import ABCMeta
from abc import abstractmethod
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from src.storage._repo import RawDataRepo


class AbstractDataProcessor(metaclass=ABCMeta):
    def __init__(
        self,
        filepath: str | None = None,
        *,
        repo: "RawDataRepo | None" = None,
        alias: str | None = None,
    ) -> None:
        """raw データを読み込む。

        Parameters
        ----------
        filepath : pickle ファイルパス（従来通り）。
        repo : RawDataRepo インスタンス（DB 直読み時）。alias と合わせて指定する。
        alias : TABLE_SPECS のキー（例 "featured_data"）。repo と合わせて指定する。
        """
        if filepath is not None:
            self.__raw_data = pd.read_pickle(filepath)
        elif repo is not None and alias is not None:
            self.__raw_data = repo.read(alias)
        else:
            raise ValueError("filepath か (repo と alias) のどちらかを指定してください")
        self.__preprocessed_data = self._preprocess()

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
