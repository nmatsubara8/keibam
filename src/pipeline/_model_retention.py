"""モデルの世代管理（retention）。

models/<YYYYMMDD>/ の日付ディレクトリを新しい順に keep 世代だけ残し、古いものを
削除する。ディスク枯渇を防ぐ。誤削除防止のため既定はドライラン。

レイヤ規約: pipeline（最上位）。純粋寄り（listdir/rmtree のみ I/O）。
"""

from __future__ import annotations

import os
import shutil


def list_model_date_dirs(models_dir: str) -> list[str]:
    """models/ 直下の日付ディレクトリ名を新しい順（名前降順）で返す。

    YYYYMMDD 命名のため辞書順 = 時系列順。ファイルや非日付ディレクトリは除外。
    """
    if not os.path.isdir(models_dir):
        return []
    dirs = [
        d for d in os.listdir(models_dir)
        if os.path.isdir(os.path.join(models_dir, d)) and d.isdigit()
    ]
    return sorted(dirs, reverse=True)


def select_models_to_delete(models_dir: str, keep: int) -> list[str]:
    """新しい順に keep 世代を残し、削除対象の日付ディレクトリ（フルパス）を返す。

    keep <= 0 は安全のため「全保持（削除なし）」として扱う。
    """
    if keep <= 0:
        return []
    date_dirs = list_model_date_dirs(models_dir)
    return [os.path.join(models_dir, d) for d in date_dirs[keep:]]


def prune_models(models_dir: str, keep: int, *, dry_run: bool = True) -> list[str]:
    """古い世代の日付ディレクトリを削除する。削除（または削除予定）パスを返す。

    dry_run=True（既定）では削除せず対象だけ返す。実削除は dry_run=False。
    """
    targets = select_models_to_delete(models_dir, keep)
    if not dry_run:
        for path in targets:
            shutil.rmtree(path, ignore_errors=True)
    return targets
