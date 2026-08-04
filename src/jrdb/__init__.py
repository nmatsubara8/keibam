"""JRDB データ取り込み（固定長・cp932・特記/基準オッズ/IDM）。既存パイプラインと独立。

永続化: `JrdbStore`（SQLite・冪等 upsert・処理済みファイル台帳で重複ロード防止）。
"""

from src.jrdb._store import JrdbStore

__all__ = ["JrdbStore"]
