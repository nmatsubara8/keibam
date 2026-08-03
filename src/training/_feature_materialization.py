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

# --- 実体化の3契約（続36-d：「列が在るだけで全欠測でも PASS」を防ぐため判定を分離）-----------
# CURRENT_ACTIVE: KYI 由来 current-race の馬別 active 特徴（33列）＝presence + coverage + race分散 を要求。
CURRENT_ACTIVE_JRDB = tuple(dict.fromkeys(
    list(KYI_FEATURE_MAP.values()) + ["jrdb_kijun_gap"]))
# CONTEXT: race 内で全馬共通の場コンテキスト（pace_hms）＝presence + coverage のみ（race分散は要求しない）。
CONTEXT_JRDB = ("jrdb_pace_hms",)
# HISTORY: 過去走 as-of 集約（prev_* + MySpeed 8列）＝presence だけでなく semantic coverage を要求。
#   history を有効化した build で全欠測なら fail-closed（列は在るが結合が壊れている状態を検知）。
HISTORY_JRDB = tuple(["prev_deokure", "prev_trouble"] + list(MYSPEED_COLS))

# TEMPORALLY_DEAD: 過去は変動したが近年 JRDB が定数化（0）した列。診断で確定したもののみ列挙。
#   jrdb_kokyu_flag: 2015-2018 は {0,1,2}・race内分散≈0.34 だが 2020+ は全行 0（saved/fresh parity=1.0）。
#   → test(2027) 期は定数ゆえ residual head で θ 寄与≈0＝inert（リーク/有害でない）。freeze は 41 維持。
TEMPORALLY_DEAD_JRDB = frozenset({"jrdb_kokyu_flag"})

# 既存本線が従来から注入していた 5 列（legacy schema）。これらは denylist 経路でも許容される。
LEGACY_JRDB_COLUMNS = frozenset(REQUIRED_JRDB_MIN)
# 完全 augment でのみ増える 37 列（legacy を除く EXPECTED 全体）。この列が featured に在るのに
# feature_allowlist 未指定なら、denylist 経路で silent 混入する＝**拒否すべき状態**。
JRDB_AUGMENT_ONLY_COLUMNS = frozenset(EXPECTED_JRDB_FULL) - LEGACY_JRDB_COLUMNS


def assert_no_unguarded_augment(columns: Iterable[str], feature_allowlist) -> list:
    """augment 専用列が在るのに allowlist 未指定なら fail-closed（denylist での silent 混入を拒否）。

    完全 augment artifact を誤って従来 denylist 経路（feature_allowlist=None）へ渡すと、新規37列が
    黙って学習へ入る。この関門で「augment 列を検出したが明示 allowlist が無い」を停止する。
    DataSplitter だけでなく将来の trainer 入口にも置けるよう純関数にする。返り値: 検出した augment 列。
    """
    present = sorted(JRDB_AUGMENT_ONLY_COLUMNS & set(str(c) for c in columns))
    if present and feature_allowlist is None:
        raise RuntimeError(
            "Augmented JRDB columns detected, but no explicit feature_allowlist was supplied. "
            f"Refusing denylist-based training. 検出列={present[:10]}"
            f"{' …' if len(present) > 10 else ''}"
            "（完全 augment artifact を学習する時は feature_allowlist を必ず明示）。")
    return present


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


# ── 実体化監査の共有ヘルパ（verify/audit/diagnose の重複を一元化・続37 refactor）────────────
def classify_jrdb_feature(col) -> str:
    """JRDB 特徴の群を返す: CONTEXT / HISTORY / ACTIVE（純関数・群集合の単一定義元）。"""
    c = str(col)
    if c in CONTEXT_JRDB:
        return "CONTEXT"
    if c in HISTORY_JRDB:
        return "HISTORY"
    return "ACTIVE"


def materialization_verdict(group: str, nm: float, sent: float, vf: float) -> str:
    """群別の実体化判定トークン（純関数）。ACTIVE=OK/薄い/DEAD, CONTEXT=CTX_OK, HISTORY=HIST_OK。

    ACTIVE: presence+coverage+race分散、CONTEXT: presence+coverage（分散不問）、
    HISTORY: semantic coverage（過去走ゼロの NaN は正常・全欠測のみ DEAD）。
    """
    if group == "ACTIVE":
        return "OK" if (nm >= 0.3 and sent < 0.2 and vf > 0.1) else ("DEAD" if nm < 0.02 else "薄い")
    if group == "CONTEXT":
        return "CTX_OK" if (nm >= 0.3 and sent < 0.2) else ("DEAD" if nm < 0.02 else "薄い")
    return "HIST_OK" if nm > 0.02 else "DEAD"


def missing_frozen_hint(missing) -> str:
    """未実体化列リストから actionable ヒント（jrdb_ms_* は --with-myspeed 要）を返す。純関数。"""
    if any(str(c).startswith("jrdb_ms_") for c in missing):
        return "（jrdb_ms_* は MySpeed＝jrdb_build_features.py に --with-myspeed を付けて再 build）"
    return "（完全 augment build を先に実行）"


def within_race_var_frac(series, race_ids) -> float:
    """race(race_ids)内で series が >1 の相異値を持つレース割合＝馬間分散あり率（純関数）。

    verify/audit/diagnose で三重定義されていた groupby(index).nunique()>1 を一元化。
    """
    import pandas as pd  # noqa: PLC0415
    s = pd.to_numeric(pd.Series(list(series)), errors="coerce")
    nun = s.groupby(pd.Series(list(race_ids)).to_numpy()).nunique(dropna=True)
    return float((nun > 1).mean()) if len(nun) else 0.0


def feature_list_hash(cols) -> str:
    """特徴列リストの順序込み短縮 hash（学習入力の実消費列を artifact 間で照合する単一定義元）。"""
    import hashlib  # noqa: PLC0415
    return hashlib.sha256(",".join(str(c) for c in cols).encode("utf-8")).hexdigest()[:16]
