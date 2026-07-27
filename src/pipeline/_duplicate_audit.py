"""raw データの主キー重複点検（純粋ロジック）。

netkeiba 取得データの pickle / DataFrame について、`TABLE_SPECS` の主キーで
重複行の有無を検査する。doctor / 単体テストから再利用する。

レイヤ規約: pipeline。constants/storage を import 可。I/O はパス読込に閉じる。
"""

from __future__ import annotations

import dataclasses
import os
from typing import Optional, Sequence

import pandas as pd

from src.storage._db import TABLE_SPECS
from src.storage._db import TableSpec
from src.storage._db import alias_to_pickle_path


@dataclasses.dataclass(frozen=True)
class DupReport:
    """1 alias 分の重複点検結果。"""

    alias: str
    n_rows: int
    n_extra: int  # drop_duplicates(PK) 後に消える余剰行数
    sample_keys: tuple[str, ...]
    skipped: bool = False
    skip_reason: str = ""

    @property
    def has_duplicates(self) -> bool:
        return self.n_extra > 0 and not self.skipped


def _normalize_index_col(df: pd.DataFrame, index_col: Optional[str]) -> pd.DataFrame:
    """upsert と同様に index_col を通常列へ起こす（破壊的変更なし）。"""
    if df.empty or index_col is None:
        return df.copy()
    out = df.copy()
    if out.index.name == index_col:
        if index_col in out.columns:
            out = out.drop(columns=[index_col])
        return out.reset_index()
    if index_col in out.columns:
        return out.reset_index(drop=True)
    return out


def resolve_pk_cols(df: pd.DataFrame, spec: TableSpec) -> Optional[tuple[str, ...]]:
    """検査に使う主キー列を返す。揃わなければ None（スキップ）。

    ``auto_row_idx_col`` で pickle に row_idx が無い場合は、全列一致の完全重複のみ検査する
    （race_id 単独だと正当な複数行を誤検知するため）。
    """
    df_n = _normalize_index_col(df, spec.index_col)
    missing = [c for c in spec.primary_key if c not in df_n.columns]
    if not missing:
        return spec.primary_key
    if spec.auto_row_idx_col and missing == ["row_idx"]:
        # 完全行一致のみ（キー列なし → 呼び出し側で全列を使う）
        return ()
    return None


def count_pk_extras(
    df: pd.DataFrame,
    pk_cols: Sequence[str],
    *,
    sample_limit: int = 5,
) -> tuple[int, tuple[str, ...]]:
    """主キー（または全列）重複の余剰行数とサンプルキーを返す。"""
    if df.empty:
        return 0, ()
    work = df if pk_cols else df
    subset = list(pk_cols) if pk_cols else None
    if subset is not None:
        missing = [c for c in subset if c not in work.columns]
        if missing:
            raise ValueError(f"PK 列が無い: {missing}")
    n_unique = len(work.drop_duplicates(subset=subset, keep="first"))
    n_extra = int(len(work) - n_unique)
    if n_extra <= 0:
        return 0, ()

    if subset is None:
        dup_mask = work.duplicated(keep=False)
        samples = [f"full_row#{i}" for i in work.index[dup_mask][:sample_limit]]
        return n_extra, tuple(str(s) for s in samples)

    dup_mask = work.duplicated(subset=subset, keep=False)
    keys = (
        work.loc[dup_mask, list(subset)]
        .astype(str)
        .drop_duplicates()
        .head(sample_limit)
    )
    samples = ["|".join(row) for row in keys.itertuples(index=False, name=None)]
    return n_extra, tuple(samples)


def audit_dataframe(alias: str, df: pd.DataFrame, *, sample_limit: int = 5) -> DupReport:
    """1 つの DataFrame を TABLE_SPECS の主キーで点検する。"""
    if alias not in TABLE_SPECS:
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason="未知の alias")
    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason="空")

    if not isinstance(df, pd.DataFrame):
        return DupReport(
            alias, 0, 0, (), skipped=True,
            skip_reason=f"DataFrame ではない ({type(df).__name__})",
        )

    spec = TABLE_SPECS[alias]
    df_n = _normalize_index_col(df, spec.index_col)
    pk = resolve_pk_cols(df, spec)
    if pk is None:
        return DupReport(
            alias, len(df_n), 0, (), skipped=True,
            skip_reason=f"主キー列不足: {list(spec.primary_key)}",
        )

    n_extra, samples = count_pk_extras(df_n, pk, sample_limit=sample_limit)
    return DupReport(alias=alias, n_rows=len(df_n), n_extra=n_extra, sample_keys=samples)


def audit_results_horse_id(df: pd.DataFrame, *, sample_limit: int = 5) -> DupReport:
    """results 専用: (race_id, horse_id) の意味論的重複（馬番 PK とは別軸）。"""
    alias = "raw_results:horse_id"
    if df is None or df.empty:
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason="空")
    work = _normalize_index_col(df, "race_id")
    if "horse_id" not in work.columns or "race_id" not in work.columns:
        return DupReport(
            alias, len(work), 0, (), skipped=True,
            skip_reason="race_id/horse_id 列なし",
        )
    n_extra, samples = count_pk_extras(
        work, ("race_id", "horse_id"), sample_limit=sample_limit,
    )
    return DupReport(alias=alias, n_rows=len(work), n_extra=n_extra, sample_keys=samples)


def _audit_odds_snapshot_list(obj: object, *, sample_limit: int = 5) -> DupReport:
    """odds_snapshots.pkl（list[OddsSnapshot]）のフェーズ単位 dedup キーで点検する。"""
    alias = "raw_odds_snapshots"
    if not isinstance(obj, (list, tuple)):
        return DupReport(
            alias, 0, 0, (), skipped=True,
            skip_reason=f"list ではない ({type(obj).__name__})",
        )
    if not obj:
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason="空")
    try:
        from src.preparing._odds_snapshot import _dedup_key
    except ImportError as e:  # pragma: no cover
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason=str(e))

    keys = [_dedup_key(s) for s in obj]
    n_unique = len(set(keys))
    n_extra = len(keys) - n_unique
    samples: tuple[str, ...] = ()
    if n_extra > 0:
        seen: set = set()
        dups: list[str] = []
        for k in keys:
            if k in seen and str(k) not in dups:
                dups.append(str(k))
                if len(dups) >= sample_limit:
                    break
            seen.add(k)
        samples = tuple(dups)
    return DupReport(alias=alias, n_rows=len(keys), n_extra=n_extra, sample_keys=samples)


def load_and_audit_alias(alias: str, path: Optional[str] = None) -> DupReport:
    """pickle パスを読み、alias に応じた重複点検を行う。"""
    pickle_path = path or alias_to_pickle_path(alias)
    if not pickle_path or not os.path.exists(pickle_path):
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason="ファイルなし")

    try:
        obj = pd.read_pickle(pickle_path)
    except Exception as e:  # noqa: BLE001
        return DupReport(alias, 0, 0, (), skipped=True, skip_reason=f"読込失敗: {e}")

    if alias == "raw_odds_snapshots" and not isinstance(obj, pd.DataFrame):
        return _audit_odds_snapshot_list(obj)

    return audit_dataframe(alias, obj if isinstance(obj, pd.DataFrame) else pd.DataFrame())


# doctor 既定で点検する raw alias（オッズ予測は運用任意のため任意スキップ可）
DEFAULT_AUDIT_ALIASES: tuple[str, ...] = (
    "raw_results",
    "raw_race_info",
    "raw_return_tables",
    "raw_horse_results",
    "raw_horse_info",
    "raw_peds",
    "raw_training",
    "raw_paddock",
    "raw_comment",
    "raw_yoso_marks",
    "raw_person_yearly",
    "raw_yoso_predictor",
    "raw_odds_snapshots",
)


def audit_all_raw(
    aliases: Sequence[str] = DEFAULT_AUDIT_ALIASES,
    *,
    include_results_horse_id: bool = True,
    path_overrides: Optional[dict[str, str]] = None,
) -> list[DupReport]:
    """登録 raw を一括点検する。results は (race_id, horse_id) も追加検査。"""
    overrides = path_overrides or {}
    reports: list[DupReport] = []
    for alias in aliases:
        reports.append(load_and_audit_alias(alias, overrides.get(alias)))

    if include_results_horse_id and "raw_results" in aliases:
        path = overrides.get("raw_results") or alias_to_pickle_path("raw_results")
        if path and os.path.exists(path):
            try:
                df = pd.read_pickle(path)
                if isinstance(df, pd.DataFrame):
                    reports.append(audit_results_horse_id(df))
            except Exception as e:  # noqa: BLE001
                reports.append(DupReport(
                    "raw_results:horse_id", 0, 0, (),
                    skipped=True, skip_reason=f"読込失敗: {e}",
                ))
    return reports
