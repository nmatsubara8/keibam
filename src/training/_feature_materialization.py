"""feature 実体化の fail-closed ガード（期待列が欠けたら学習を止める）。

続31 監査で「定義43特徴中 featured 実体化は5列・38 ABSENT（attach 未配線）」が判明。原因は
「期待列が欠けても学習が黙って進む」こと。本ガードで **required が欠けたら RuntimeError**、
optional/expected の欠落は**リスト返却（警告用）**にする。純関数。

- REQUIRED_JRDB_MIN: 現状でも本線 featured に実在すべき最小集合（欠けたら即失敗＝退行検知）。
- EXPECTED_JRDB_FULL: attach 完全配線後に実体化すべき全集合（現状は多くが欠落＝warn で可視化）。
"""
from __future__ import annotations

from typing import Iterable

from src.jrdb._augment import KYI_FEATURE_MAP, MYSPEED_COLS

# 現行本線が注入する固定サブセット（_results_processor / _adapter._KYI_INDEX_COLS）＝退行検知の必須列。
REQUIRED_JRDB_MIN = ("jrdb_idm", "jrdb_kijun_odds", "jrdb_kyakushitsu",
                     "jrdb_joho_idx", "jrdb_kishu_idx")

# attach 完全配線後に実体化されるべき全 JRDB 特徴（現状の多くは未実体化＝repair 対象）。
EXPECTED_JRDB_FULL = tuple(dict.fromkeys(
    list(KYI_FEATURE_MAP.values())
    + ["jrdb_pace_hms", "jrdb_kijun_gap", "prev_deokure", "prev_trouble"]
    + list(MYSPEED_COLS)))


def assert_features_materialized(columns: Iterable[str], required: Iterable[str],
                                 *, optional: Iterable[str] = ()) -> list:
    """required が全て columns に在ることを強制（欠落は RuntimeError）。返す optional の欠落リスト（警告用）。

    「期待列が欠けても学習が進む」黙殺を止める。required=必須（fail-closed）、optional=欠けてよい
    （呼び出し側が warn ログ）。純関数。
    """
    cols = set(str(c) for c in columns)
    missing = sorted(set(str(c) for c in required) - cols)
    if missing:
        raise RuntimeError(
            f"feature materialization failed: 必須 JRDB 列 {missing} が featured に無い"
            "（attach/build 経路の未適用 or 古い artifact を疑う）。")
    return sorted(set(str(c) for c in optional) - cols)
