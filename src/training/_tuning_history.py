"""Optuna ハイパラ探索結果の保存・選択（Layer: training）。

LightGBMTuner（optuna_integration.lightgbm.train）は段階的探索を行い、
各 trial の完全な LightGBM パラメータを
``trial.system_attrs["lightgbm_tuner:lgbm_params"]`` に JSON で記録する。
本モジュールはそれを成績順（binary_logloss 昇順等、study.direction に従う）の
レコード一覧へ整形し、``models/tuning_history.json`` に保存する。

ユーザーは保存済みの探索結果から任意の rank を選んで再学習に使える:
    python -m src.pipeline.run_pipeline retrain --params-rank 2
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Sequence

from src.constants._model_category import COMBINED

logger = logging.getLogger(__name__)

TUNING_HISTORY_FILENAME = "tuning_history.json"

# LightGBMTuner が各 trial の完全パラメータを記録する system_attrs キー
_LGBM_PARAMS_ATTR = "lightgbm_tuner:lgbm_params"


def tuning_history_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, TUNING_HISTORY_FILENAME)


def trials_to_records(
    study, version: str, top_n: int = 50, category: str = COMBINED
) -> list[dict]:
    """study の全 trial を成績順のレコードに整形する（純粋関数）。

    - 並び順は study.direction（LightGBMTuner は binary_logloss の minimize）。
    - 同一パラメータの trial は最良値のものだけ残す（段階探索は既定値を重複試行するため）。
    - 各レコード: rank / value / metric / params / trial_number / version / category / tuned_at。

    category は探索対象モデル（統合は "combined"、カテゴリ別は central_turf 等）。
    (version, category) 単位で履歴を区別し、6 分割の各モデルの探索結果を独立に扱う。
    """
    candidates: list[dict] = []
    for t in study.trials:
        if t.value is None:
            continue
        raw = t.system_attrs.get(_LGBM_PARAMS_ATTR)
        if raw is None:
            continue
        params = json.loads(raw) if isinstance(raw, str) else dict(raw)
        candidates.append(
            {
                "trial_number": t.number,
                "value": float(t.value),
                "metric": params.get("metric", "binary_logloss"),
                "params": params,
            }
        )

    reverse = getattr(study.direction, "name", str(study.direction)).upper() == "MAXIMIZE"
    candidates.sort(key=lambda r: r["value"], reverse=reverse)

    # 同一パラメータは最良値（ソート後の先勝ち）だけ残す
    seen: set = set()
    unique: list[dict] = []
    for r in candidates:
        key = json.dumps(r["params"], sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)

    tuned_at = dt.datetime.now().isoformat()
    records = unique[:top_n]
    for rank, r in enumerate(records, start=1):
        r["rank"] = rank
        r["version"] = version
        r["category"] = category
        r["tuned_at"] = tuned_at
    return records


def _record_category(r: dict) -> str:
    """履歴レコードの category（未設定の旧レコードは "combined"）。"""
    return r.get("category") or COMBINED


def save_tuning_history(records: Sequence[dict], path: str) -> None:
    """探索結果を JSON 履歴に保存する。同一 (version, category) のレコードは置き換える。"""
    if not records:
        return
    version = records[0].get("version")
    category = _record_category(records[0])
    existing = [
        r
        for r in load_tuning_history(path)
        if not (r.get("version") == version and _record_category(r) == category)
    ]
    merged = existing + list(records)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    logger.info(
        "[tuning_history] %s: %d trials saved (version=%s category=%s)",
        path, len(records), version, category,
    )


def load_tuning_history(path: str) -> list[dict]:
    """探索履歴を読み込む（無ければ空リスト）。"""
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def latest_version(history: Sequence[dict], category: str | None = None) -> str | None:
    """履歴中で最も新しい tuned_at を持つ version を返す。

    category 指定時はそのカテゴリのレコードに限定して選ぶ。
    """
    subset = history if category is None else [r for r in history if _record_category(r) == category]
    if not subset:
        return None
    return max(subset, key=lambda r: r.get("tuned_at", ""))["version"]


def get_params_by_rank(
    history: Sequence[dict],
    rank: int,
    version: str | None = None,
    category: str = COMBINED,
) -> dict:
    """指定 (category, rank) のパラメータを返す。

    version 未指定時は当該カテゴリの最新探索（tuned_at が最大の version）から選ぶ。
    見つからない場合は ValueError。
    """
    target_version = version or latest_version(history, category=category)
    if target_version is None:
        raise ValueError(
            f"チューニング履歴が空です（category={category}。"
            "retrain --with-tuning / --tune-categories を先に実行してください）"
        )
    for r in history:
        if (
            r.get("version") == target_version
            and _record_category(r) == category
            and r.get("rank") == rank
        ):
            return dict(r["params"])
    raise ValueError(
        f"rank={rank} の探索結果が見つかりません (version={target_version} category={category})"
    )
