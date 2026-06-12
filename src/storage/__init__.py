"""raw データ永続化レイヤ。

`RawDataRepo` 経由で SQLite に対する upsert / read / delete を行う。
pickle はキャッシュとして併存し、Processor 群は従来通り pickle を読み続ける。
"""

from src.storage._db import PICKLE_PATH_TO_ALIAS
from src.storage._db import TABLE_SPECS
from src.storage._db import TableSpec
from src.storage._db import alias_to_pickle_path
from src.storage._db import get_engine
from src.storage._execution_log import load_executions
from src.storage._execution_log import record_execution
from src.storage._featured import load_featured_meta
from src.storage._featured import load_parquet
from src.storage._featured import save_featured_meta
from src.storage._featured import save_parquet
from src.storage._repo import RawDataRepo

__all__ = [
    "RawDataRepo",
    "TABLE_SPECS",
    "TableSpec",
    "PICKLE_PATH_TO_ALIAS",
    "alias_to_pickle_path",
    "get_engine",
    "save_parquet",
    "load_parquet",
    "save_featured_meta",
    "load_featured_meta",
    "record_execution",
    "load_executions",
]
