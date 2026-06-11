"""raw データ永続化レイヤ。

`RawDataRepo` 経由で SQLite に対する upsert / read / delete を行う。
pickle はキャッシュとして併存し、Processor 群は従来通り pickle を読み続ける。
"""

from src.storage._db import PICKLE_PATH_TO_ALIAS
from src.storage._db import TABLE_SPECS
from src.storage._db import TableSpec
from src.storage._db import alias_to_pickle_path
from src.storage._db import get_engine
from src.storage._featured_repo import FeaturedDataRepo
from src.storage._repo import RawDataRepo

__all__ = [
    "RawDataRepo",
    "FeaturedDataRepo",
    "TABLE_SPECS",
    "TableSpec",
    "PICKLE_PATH_TO_ALIAS",
    "alias_to_pickle_path",
    "get_engine",
]
