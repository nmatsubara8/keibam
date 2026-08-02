"""history/soten の as-of 結合が strictly-prior（今走より過去のみ参照）かを**全件集計**で認定する。

続36-e: canary（1頭の時系列確認）に加え、本線 artifact の manifest に残す全件証跡を出す。
attach と同じ merge_asof(direction="backward", allow_exact_matches=False) を正規化済み日付で
再現し、target 各行が参照した source 日付が「今走より未来/同日」でないことを数える。

leak_safe = future_reference_count == 0 かつ same_day_reference_count == 0。純関数（pandas のみ）。
"""
from __future__ import annotations

import pandas as pd

from src.jrdb._augment import _to_race_datetime


def strictly_prior_join_report(target: pd.DataFrame, source: pd.DataFrame, *,
                               ketto_col: str = "ketto", target_date_col: str = "date",
                               source_date_col: str = "hist_date") -> dict:
    """target×source の as-of 結合を再現し strictly-prior 全件集計を返す（性能は見ない）。

    返す manifest キー:
      target_rows / target_valid_rows(ketto&date 有) / feature_rows(参照が付いた行) /
      history_source_references_checked(=feature_rows) / future_reference_count(=0 が要件) /
      same_day_reference_count(=0) / exact_target_reference_count(=0) / max_source_date /
      target_key_duplicates(同一 ketto×date 重複) / source_rows / leak_safe(bool)。
    """
    trep: dict = {"target_rows": int(len(target)), "source_rows": int(len(source))}
    if (source is None or source.empty or ketto_col not in target.columns
            or target_date_col not in target.columns):
        trep.update(target_valid_rows=0, feature_rows=0, history_source_references_checked=0,
                    future_reference_count=0, same_day_reference_count=0,
                    exact_target_reference_count=0, max_source_date=None,
                    target_key_duplicates=0, leak_safe=True)
        return trep

    t = pd.DataFrame({
        "_k": target[ketto_col].astype("string"),
        "_today": _to_race_datetime(target[target_date_col]),
    })
    t = t.dropna(subset=["_k", "_today"])
    trep["target_valid_rows"] = int(len(t))
    trep["target_key_duplicates"] = int(t.duplicated(["_k", "_today"]).sum())

    s = pd.DataFrame({
        "_k": source[ketto_col].astype("string"),
        "_sdate": pd.to_datetime(source[source_date_col], errors="coerce"),
    }).dropna(subset=["_k", "_sdate"])
    trep["max_source_date"] = (s["_sdate"].max().strftime("%Y-%m-%d")
                               if len(s) else None)

    t = t.sort_values("_today")
    s = s.sort_values("_sdate")
    m = pd.merge_asof(t, s, by="_k", left_on="_today", right_on="_sdate",
                      direction="backward", allow_exact_matches=False)
    matched = m["_sdate"].notna()
    trep["feature_rows"] = int(matched.sum())
    trep["history_source_references_checked"] = int(matched.sum())
    sd = m.loc[matched, "_sdate"]
    td = m.loc[matched, "_today"]
    trep["future_reference_count"] = int((sd > td).sum())
    trep["same_day_reference_count"] = int((sd == td).sum())
    trep["exact_target_reference_count"] = int((sd == td).sum())
    trep["leak_safe"] = bool(trep["future_reference_count"] == 0
                             and trep["same_day_reference_count"] == 0)
    return trep


def assert_strictly_prior(report: dict, *, label: str = "history") -> None:
    """manifest が strictly-prior でなければ RuntimeError（未来/同日参照は leak）。fail-closed。"""
    if not report.get("leak_safe", False):
        raise RuntimeError(
            f"{label} as-of が strictly-prior でない: "
            f"future={report.get('future_reference_count')} "
            f"same_day={report.get('same_day_reference_count')}"
            "（direction=backward/allow_exact_matches=False と date 正規化を確認）。")
