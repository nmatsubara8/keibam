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
import re

import pandas as pd

from src.constants._local_paths import LocalPaths
from src.operation._config import OperationConfig

# 本番モデルの命名規則: version_name() = "YYYYMMDD_<prefix>"（既定 prefix=keibam）。
# 実験モデル（--version-name noodds_keibam / base2016 等）は日付接頭辞を持たないので、
# この正規表現で除外して「使い捨て実験が最新扱いで本番既定を乗っ取る」事故を防ぐ。
_PROD_MODEL_DATE_PREFIX = re.compile(r"^\d{8}_")


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
    """models/ 配下の KeibaAI モデル pickle 一覧を新しい順で返す。

    retrain が生成するモデルはバージョン名 `YYYYMMDD_<prefix>`（既定 prefix=keibam）
    で `*_keibam.pickle` として保存される。一方 models/ には旧フォーマットの
    pickle（basemodel_*.pickle / tansho.pickle / fukusho.pickle 等の馬券ポリシーや
    旧 DataFrame）が混在しており、これらは KeibaAI ではないため予測ページで
    `'DataFrame' object has no attribute 'effective_model'` を引き起こす。
    そのため正規モデルの命名規則（`_keibam.pickle` 接尾辞）に一致するもののみ返す。

    さらに **日付接頭辞（YYYYMMDD_）を必須**にして、`--version-name` で付けた使い捨ての
    実験モデル（noodds_keibam / base2016 等）を除外する。これがないと実験モデルが最新扱いで
    本番既定を乗っ取る（find_model_paths[0] が実験モデルになる）事故が起きる。実験は日付接頭辞を
    持たない名前を使えば自動で除外される。
    """
    paths = []
    if not os.path.isdir(models_dir):
        return []
    for date_dir in sorted(os.listdir(models_dir), reverse=True):
        full = os.path.join(models_dir, date_dir)
        if not os.path.isdir(full):
            continue
        for fname in sorted(os.listdir(full), reverse=True):
            if fname.endswith("_keibam.pickle") and _PROD_MODEL_DATE_PREFIX.match(fname):
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


def load_model_from_path(path: str):
    """指定した .pickle パスのモデルを読み込む（version 名の曖昧一致を避ける厳密版）。

    UI でユーザーが選んだバージョンを適用する際、`find_model_paths()` が返す
    実ファイルパスをそのまま渡せば、version 名の部分一致による取り違えが起きない。
    """
    from src.training._keiba_ai_factory import KeibaAIFactory

    return KeibaAIFactory.load(path)


def win_head_path_for(place_path: str) -> str:
    """Place モデルパスから対応する Win ヘッドのパスを導く（``X.pickle`` → ``X__win.pickle``）。"""
    if place_path.endswith(".pickle"):
        return place_path[: -len(".pickle")] + "__win.pickle"
    return place_path + "__win.pickle"


def load_win_head_for(place_path: str):
    """Place モデルに対応する Win ヘッド（<version>__win.pickle）を読み込む。

    存在しなければ None（Win ヘッド未学習の旧モデル・--no-win-head 運用との後方互換）。
    Stage B: 連系の Harville に真の勝率を供給する 1着予測モデル。
    """
    from src.training._keiba_ai_factory import KeibaAIFactory

    win_path = win_head_path_for(place_path)
    if not os.path.exists(win_path):
        return None
    return KeibaAIFactory.load(win_path)


def category_model_path_for(place_path: str, category: str) -> str:
    """統合 Place モデルパスから、カテゴリ別 Place ヘッドのパスを導く。

    ``<version>.pickle`` → ``<version>__<category>.pickle``（例 ``__central_turf``）。
    """
    stem = place_path[: -len(".pickle")] if place_path.endswith(".pickle") else place_path
    return f"{stem}__{category}.pickle"


def resolve_place_model_path_for_race(place_path: str, race_id, race_type_value) -> tuple[str, str]:
    """レースの主催者区分×馬場種別に対応する Place ヘッドのパスを解決する。

    該当カテゴリ別モデル（<version>__<category>.pickle）があればそれを、無ければ
    統合 Place モデル（place_path）へフォールバックする。

    Returns
    -------
    (path, used) : path は使用するモデル pickle、used は選ばれたカテゴリ slug
        （フォールバック時は "combined"）。
    """
    from src.constants._model_category import COMBINED
    from src.constants._model_category import categorize

    cat = categorize(race_id, race_type_value)
    if cat is not None:
        cat_path = category_model_path_for(place_path, cat)
        if os.path.exists(cat_path):
            return cat_path, cat
    return place_path, COMBINED


def available_categories_for(place_path: str) -> list[str]:
    """指定バージョンで実在するカテゴリ別 Place ヘッドの slug 一覧を返す。"""
    from src.constants._model_category import ALL_CATEGORIES

    return [c for c in ALL_CATEGORIES if os.path.exists(category_model_path_for(place_path, c))]


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
    from src.preparing._odds_snapshot import combo_to_str

    rows = [
        {
            "race_id": s.race_id,
            "bet_type": s.bet_type,
            "combo": combo_to_str(s.combo),
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
    """ダッシュボード用のシステム状態サマリを返す。

    Phase 2: featured_data のメタ情報を SQLite から優先取得し、
    DB がない場合は pickle のファイル更新日時にフォールバックする。
    """
    versions = list_model_versions(models_dir)
    latest_version = versions[0]["version"] if versions else None
    latest_auc = versions[0].get("auc_test") if versions else None

    n_snapshots = len(load_odds_snapshots(snapshots_path))

    # Phase 2: DB メタから featured_data 統計を取得
    last_ingest = None
    n_featured_rows = None
    n_featured_cols = None
    featured_min_race_id = None
    featured_max_race_id = None

    try:
        from src.storage._featured import load_featured_meta
        meta = load_featured_meta()
        if meta:
            last_ingest = meta["created_at"][:16] if meta.get("created_at") else None
            n_featured_rows = meta.get("n_rows")
            n_featured_cols = meta.get("n_cols")
            featured_min_race_id = meta.get("min_race_id")
            featured_max_race_id = meta.get("max_race_id")
    except Exception:
        pass

    # DB メタがない場合は pickle のファイル更新日時にフォールバック
    if last_ingest is None and os.path.exists(featured_path):
        mtime = os.path.getmtime(featured_path)
        last_ingest = dt.datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")

    return {
        "model_version": latest_version,
        "model_auc": latest_auc,
        "n_snapshots": n_snapshots,
        "last_ingest": last_ingest,
        "operation_mode": load_operation_config(config_path).operation_mode,
        # Phase 2 追加フィールド
        "n_featured_rows": n_featured_rows,
        "n_featured_cols": n_featured_cols,
        "featured_min_race_id": featured_min_race_id,
        "featured_max_race_id": featured_max_race_id,
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


def load_freshness_status() -> dict:
    """システム健全性（データ/モデル/DB/ディスク鮮度）の点検結果を返す。

    src.pipeline._doctor の純粋ロジックを再利用する（app→pipeline は順方向で合法）。

    Returns
    -------
    {"level": "OK"|"WARN"|"ERROR", "checks": [{"name","level","detail"}, ...]}
    """
    try:
        from src.pipeline._doctor import run_doctor

        results, level = run_doctor()
        return {
            "level": level,
            "checks": [{"name": r.name, "level": r.level, "detail": r.detail} for r in results],
        }
    except Exception as e:  # noqa: BLE001
        return {"level": "ERROR", "checks": [{"name": "doctor", "level": "ERROR", "detail": str(e)}]}


def load_execution_log(limit: int = 20) -> list[dict]:
    """cron/CLI ジョブの実行記録を新しい順で返す（execution_log）。"""
    try:
        from src.storage import load_executions

        return load_executions(limit=limit)
    except Exception:  # noqa: BLE001
        return []
