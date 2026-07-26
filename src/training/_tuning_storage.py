"""Optuna study の永続化ヘルパ（探索の再開＋best の単調改善）。

既定では各 `create_study` はプロセス内メモリの一時 study（実行ごとに新規＝過去の探索を
引き継がない）。環境変数 ``KEIBA_TUNING_STORAGE`` に SQLite URL を設定すると、モデル別の
``study_name`` で **同じ study に trial を追記**するようになり、再実行で best が単調改善する。

retrain --resume-tuning が ``KEIBA_TUNING_STORAGE=sqlite:///<models_dir>/optuna_studies.db``
を立てる。永続再開は探索空間（distributions）が実行間で同一である前提。空間を変えた場合は
Optuna が警告する／別 study_name を使うこと。
"""
from __future__ import annotations

import os


def tuning_storage_url() -> str:
    """設定されていれば Optuna storage URL（未設定なら空文字）。"""
    return os.environ.get("KEIBA_TUNING_STORAGE", "").strip()


def study_kwargs(name: str) -> dict:
    """create_study に渡す永続化 kwargs を返す。

    ``KEIBA_TUNING_STORAGE`` 未設定なら空 dict（＝従来のメモリ内 study）。設定時は
    ``storage`` / ``study_name`` / ``load_if_exists=True`` を返し、再実行で同名 study を
    再開する（trial 追記＝best 単調改善）。

    Parameters
    ----------
    name : study 名（モデル別に一意。例 "nn" / "xgboost" / "catboost" / "lightgbm"）
    """
    url = tuning_storage_url()
    if not url:
        return {}
    return {"storage": url, "study_name": name, "load_if_exists": True}
