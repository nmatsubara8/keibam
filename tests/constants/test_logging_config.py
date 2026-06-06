"""§10 構造化ログ setup_logging のテスト。"""

from __future__ import annotations

import logging
import os

from src.constants._logging_config import setup_logging


class TestSetupLogging:
    def test_configures_stream_handler(self):
        setup_logging()
        root = logging.getLogger()
        assert any(isinstance(h, logging.StreamHandler) for h in root.handlers)

    def test_level_applied(self):
        setup_logging(level=logging.WARNING)
        assert logging.getLogger().level == logging.WARNING
        # 後続テストに影響しないよう INFO へ戻す
        setup_logging(level=logging.INFO)

    def test_file_handler_created_and_writes(self, tmp_path):
        logfile = str(tmp_path / "logs" / "app.log")
        setup_logging(logfile=logfile, level=logging.INFO)
        logging.getLogger("test.logger").info("hello-file")
        for h in logging.getLogger().handlers:
            h.flush()
        assert os.path.isfile(logfile)
        with open(logfile, encoding="utf-8") as f:
            assert "hello-file" in f.read()
        # ファイルハンドラを外して後続テストへの影響を避ける
        setup_logging(level=logging.INFO)

    def test_force_replaces_handlers(self):
        setup_logging()
        n1 = len(logging.getLogger().handlers)
        setup_logging()
        n2 = len(logging.getLogger().handlers)
        # force=True なので重複追加されない（StreamHandler 1 個に保たれる）
        assert n1 == n2
