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


class TestRotation:
    def test_rotating_handler_when_max_bytes(self, tmp_path):
        from logging.handlers import RotatingFileHandler

        logfile = str(tmp_path / "logs" / "rot.log")
        setup_logging(logfile=logfile, max_bytes=1024, backup_count=3)
        handlers = logging.getLogger().handlers
        assert any(isinstance(h, RotatingFileHandler) for h in handlers)
        setup_logging()  # 後続テストへの影響を避ける

    def test_plain_file_handler_when_no_max_bytes(self, tmp_path):
        from logging.handlers import RotatingFileHandler

        logfile = str(tmp_path / "logs" / "plain.log")
        setup_logging(logfile=logfile)  # 既定 max_bytes=0
        handlers = logging.getLogger().handlers
        file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
        assert file_handlers
        assert not any(isinstance(h, RotatingFileHandler) for h in file_handlers)
        setup_logging()
