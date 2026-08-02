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


# 学習に使ってよい JRDB/派生列の接頭辞・列名（allowlist 判定で「混入監視」対象を特定する）。
_JRDB_LIKE_PREFIXES = ("jrdb_",)
_JRDB_LIKE_NAMES = ("prev_deokure", "prev_trouble")


def _is_jrdb_like(col: str) -> bool:
    return str(col).startswith(_JRDB_LIKE_PREFIXES) or str(col) in _JRDB_LIKE_NAMES


def assert_training_allowlist(featured_columns: Iterable[str], allowlist: Iterable[str],
                              *, jrdb_guard: bool = True) -> list:
    """学習に使う特徴を **明示 allowlist** で確定し、不足も未登録混入も fail-fast にする。純関数。

    本線の特徴選択は denylist（`_data_splitter._DROP_FOR_TRAIN` を落として残り全部）であり、
    attach 配線後に新規実体化した JRDB 列が **黙って既存モデルへ混入**する（続36 で危惧）。
    これを止めるための明示的関門:

    1. allowlist の列が featured に **無ければ RuntimeError**（未実体化のまま学習しない）。
       ＝ユーザ指定の `missing = set(model_features) - set(featured.columns)` チェック。
    2. jrdb_guard=True のとき、featured にある jrdb_*/prev_* のうち **allowlist 外**を検出して
       RuntimeError（新規列の silent 混入を止める。allowlist へ足すのは明示決定＝人手 or config）。

    Returns: allowlist に含まれ実在する列（学習に使う確定集合・allowlist 順）。
    """
    cols = set(str(c) for c in featured_columns)
    allow = [str(c) for c in allowlist]
    allow_set = set(allow)
    missing = sorted(allow_set - cols)
    if missing:
        raise RuntimeError(
            f"Missing configured features: {missing}"
            "（allowlist の列が featured に未実体化。attach/build 経路を先に修復）。")
    if jrdb_guard:
        intruders = sorted(c for c in cols if _is_jrdb_like(c) and c not in allow_set)
        if intruders:
            raise RuntimeError(
                f"未登録の JRDB 派生列が featured に混入: {intruders}"
                "（denylist 経路で既存モデルへ silent 混入する恐れ。使うなら allowlist へ明示追加、"
                "使わないなら featured 生成時に落とす）。")
    return [c for c in allow if c in cols]
