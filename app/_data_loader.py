"""UI 向けデータ読込ヘルパ。

モデル・スナップショット・バージョン履歴・設定ファイルを読み込む関数群。
重い処理は呼び出し側（Streamlit の @st.cache_resource）でキャッシュする。
ファイルが存在しない場合は空値・空リストで安全に返す（graceful degradation）。
"""

from __future__ import annotations

import datetime as dt
import json
import os
import pickle

import pandas as pd

from src.constants._local_paths import LocalPaths
from src.operation._config import OperationConfig


# ---------------------------------------------------------------------------
# 設定
# ---------------------------------------------------------------------------


def load_operation_config(path: str = "config.yaml") -> OperationConfig:
    """config.yaml から OperationConfig を読み込む（なければ既定値）。"""
    if not os.path.exists(path):
        return OperationConfig()
    return OperationConfig.load(path)


# ---------------------------------------------------------------------------
# モデル
# ---------------------------------------------------------------------------


def list_model_versions(models_dir: str = "models") -> list[dict]:
    """version_history.json からバージョン一覧を新しい順で返す。"""
    path = os.path.join(models_dir, "version_history.json")
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        history: list[dict] = json.load(f)
    return list(reversed(history))


def find_model_paths(models_dir: str = "models") -> list[str]:
    """models/ 配下の .pickle ファイル一覧を新しい順で返す。"""
    paths = []
    if not os.path.isdir(models_dir):
        return []
    for date_dir in sorted(os.listdir(models_dir), reverse=True):
        full = os.path.join(models_dir, date_dir)
        if not os.path.isdir(full):
            continue
        for fname in sorted(os.listdir(full), reverse=True):
            if fname.endswith(".pickle"):
                paths.append(os.path.join(full, fname))
    return paths


def load_latest_model(models_dir: str = "models"):
    """最新モデルを読み込む（モデルがなければ None）。"""
    from src.training._keiba_ai_factory import KeibaAIFactory

    paths = find_model_paths(models_dir)
    if not paths:
        return None
    return KeibaAIFactory.load(paths[0])


def load_model_by_version(version: str, models_dir: str = "models"):
    """version 名に一致する .pickle ファイルを読み込む。"""
    from src.training._keiba_ai_factory import KeibaAIFactory

    for path in find_model_paths(models_dir):
        if version in os.path.basename(path):
            return KeibaAIFactory.load(path)
    raise FileNotFoundError(f"バージョン '{version}' のモデルが見つかりません: {models_dir}")


# ---------------------------------------------------------------------------
# オッズ スナップショット
# ---------------------------------------------------------------------------


def load_odds_snapshots(path: str = LocalPaths.RAW_ODDS_SNAPSHOT_PATH) -> list:
    """集約 pickle からスナップショット一覧を読み込む（なければ空リスト）。"""
    if not os.path.exists(path):
        return []
    with open(path, "rb") as f:
        return pickle.load(f)


def snapshots_to_dataframe(snapshots: list) -> pd.DataFrame:
    """OddsSnapshot リストを DataFrame に変換する（フィルタ・集計用）。"""
    if not snapshots:
        return pd.DataFrame(columns=["race_id", "bet_type", "combo", "odds", "captured_at", "minutes_to_post", "phase"])
    rows = [
        {
            "race_id": s.race_id,
            "bet_type": s.bet_type,
            "combo": "-".join(str(h) for h in s.combo),
            "odds": s.odds,
            "captured_at": s.captured_at,
            "minutes_to_post": s.minutes_to_post,
            "phase": s.phase,
        }
        for s in snapshots
    ]
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# システム状態サマリ（ダッシュボード用）
# ---------------------------------------------------------------------------


def load_system_status(
    config_path: str = "config.yaml",
    models_dir: str = "models",
    snapshots_path: str = LocalPaths.RAW_ODDS_SNAPSHOT_PATH,
    featured_path: str = LocalPaths.FEATURED_DATA_PATH,
) -> dict:
    """ダッシュボード用のシステム状態サマリを返す。"""
    versions = list_model_versions(models_dir)
    latest_version = versions[0]["version"] if versions else None
    latest_auc = versions[0].get("auc_test") if versions else None

    n_snapshots = len(load_odds_snapshots(snapshots_path))

    last_ingest = None
    if os.path.exists(featured_path):
        mtime = os.path.getmtime(featured_path)
        last_ingest = dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")

    return {
        "model_version": latest_version,
        "model_auc": latest_auc,
        "n_snapshots": n_snapshots,
        "last_ingest": last_ingest,
        "operation_mode": load_operation_config(config_path).operation_mode,
    }


# ---------------------------------------------------------------------------
# DB 統計（Phase 2）
# ---------------------------------------------------------------------------


def load_db_stats() -> dict:
    """テーブル別行数と DB ファイルサイズを返す。

    Returns
    -------
    {
      "table_counts": {alias: int},
      "db_size_mb": float,
    }
    """
    try:
        from src.storage import TABLE_SPECS
        from src.storage._db import get_engine
        from sqlalchemy import text

        engine = get_engine()
        counts: dict[str, int] = {}
        with engine.connect() as conn:
            for alias, spec in TABLE_SPECS.items():
                try:
                    row = conn.execute(
                        text(f'SELECT COUNT(*) FROM "{spec.table_name}"')
                    ).fetchone()
                    counts[alias] = int(row[0]) if row else 0
                except Exception:
                    counts[alias] = -1
    except Exception:
        counts = {}

    db_size_mb = 0.0
    if os.path.exists(LocalPaths.DB_PATH):
        db_size_mb = os.path.getsize(LocalPaths.DB_PATH) / (1024 * 1024)

    return {"table_counts": counts, "db_size_mb": db_size_mb}
