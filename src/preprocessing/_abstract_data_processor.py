from abc import ABCMeta
from abc import abstractmethod
from typing import Optional

import pandas as pd


class AbstractDataProcessor(metaclass=ABCMeta):
    """raw データを読み込んで前処理する基底クラス。

    入力ソースは 2 通り（Phase 2 で DB 直読みを追加）:
    - `filepath` 指定: pickle を `pd.read_pickle` で読む（従来動作）。
    - `repo` + `alias` 指定: `repo.read(alias)` で SQLite から読む。
      pickle が揮発しても DB から直接前処理を再開できる。

    `repo` は `read(alias) -> pd.DataFrame` を持つオブジェクト（`RawDataRepo`）を
    想定するが、duck typing で受けるため storage レイヤへの import 依存は持たない。
    """

    def __init__(
        self,
        filepath: Optional[str] = None,
        *,
        repo: object = None,
        alias: Optional[str] = None,
    ):
        if filepath is not None:
            self.__raw_data = pd.read_pickle(filepath)
        elif repo is not None and alias is not None:
            self.__raw_data = repo.read(alias)  # type: ignore[attr-defined]
        else:
            raise ValueError(
                "AbstractDataProcessor: filepath か (repo, alias) のいずれかを指定してください"
            )
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
