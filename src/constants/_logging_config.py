"""構造化ログの共通セットアップ（§10）。

stdlib `logging` のみに依存する横断的インフラ。`preparing`〜`pipeline` まで全レイヤから
import 可能にするため `constants` に置く（他 src レイヤへの依存は一切持たない）。

利用方針:
- ライブラリ側（_ingestion / _retrain / odds_scheduler 等）は
  `logger = logging.getLogger(__name__)` でロガーを取得し、print の代わりに使う。
- CLI エントリ（各 main）が `setup_logging()` を一度だけ呼んでハンドラを構成する。
  VPS 本番ではファイル出力も併用し、SSH 切断後も追跡できるようにする。
"""

from __future__ import annotations

import logging
import os
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def setup_logging(
    logfile: Optional[str] = None,
    level: int = logging.INFO,
    *,
    max_bytes: int = 0,
    backup_count: int = 0,
) -> None:
    """ルートロガーを StreamHandler（＋任意で FileHandler）で構成する。

    Parameters
    ----------
    logfile : 指定するとファイルにも出力する（親ディレクトリは自動作成）。
    level : ログレベル（既定 INFO）。
    max_bytes : >0 のとき RotatingFileHandler を使い、このサイズでローテーションする
        （長期運用でのログ無制限増長を防ぐ）。0（既定）なら従来の FileHandler。
    backup_count : ローテーション世代数（max_bytes>0 時のみ有効）。
    """
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if logfile:
        log_dir = os.path.dirname(logfile)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        if max_bytes > 0:
            from logging.handlers import RotatingFileHandler

            handlers.append(
                RotatingFileHandler(
                    logfile, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
                )
            )
        else:
            handlers.append(logging.FileHandler(logfile, encoding="utf-8"))
    logging.basicConfig(level=level, format=_DEFAULT_FORMAT, handlers=handlers, force=True)
