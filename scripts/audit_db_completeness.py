"""生DB完全性監査＝「行が在る」の先の整合を機械検査する（取り込み認定の最終層）。

featured/特徴量の充足は audit_feature_coverage が見る。本スクリプトはその**上流**＝raw テーブル
そのものの整合を検査する:
  ① 主キー重複ゼロ           ② 必須列 NULL 率
  ③ 孤児キー率（results↔race_info↔horse_results の race_id / horse_id）
  ④ 日付レンジと欠落年        ⑤ 年別行数
  ⑥ 着順/頭数/勝ち馬 の整合（rank∈1..頭数・各レースに rank==1 が1頭）
元 ZIP/TXT/CSV の件数×DB件数の 1:1 照合はソースファイルが要る（ローカル・別途）。ここは DB 側の
自己整合と相互整合を機械判定する。純関数はテスト済。read は best-effort（無いテーブルはスキップ）。

使い方: python scripts/audit_db_completeness.py
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def pk_duplicate_count(df, keys) -> int:
    """主キー keys の重複行数。keys が揃っていなければ -1（判定不能）。"""
    if df is None or len(df) == 0 or not all(k in df.columns for k in keys):
        return -1
    return int(df.duplicated(subset=list(keys)).sum())


def null_rate(df, col) -> float:
    """列 col の NULL 率（列が無ければ 1.0＝全欠測扱い）。"""
    import pandas as pd
    if df is None or len(df) == 0 or col not in df.columns:
        return 1.0
    return float(pd.to_numeric(df[col], errors="coerce").isna().mean()
                 if df[col].dtype.kind in "biufc" else df[col].isna().mean())


def orphan_rate(child_keys, parent_keys) -> dict:
    """child のキーのうち parent に存在しない割合（孤児率）。返す {n_child, n_orphan, rate}。"""
    c = {str(x) for x in child_keys if x is not None and str(x) != "nan"}
    p = {str(x) for x in parent_keys if x is not None and str(x) != "nan"}
    orph = c - p
    return {"n_child": len(c), "n_orphan": len(orph),
            "rate": (len(orph) / len(c)) if c else 0.0}


def year_span(years) -> dict:
    """年(YYYY int/str)集合 → {min, max, n_present, missing}（min..max の欠落年）。"""
    ys = sorted({int(y) for y in years if str(y).isdigit()})
    if not ys:
        return {"min": None, "max": None, "n_present": 0, "missing": []}
    full = set(range(ys[0], ys[-1] + 1))
    return {"min": ys[0], "max": ys[-1], "n_present": len(ys),
            "missing": sorted(full - set(ys))}


def rank_consistency(df, *, race_col="race_id", rank_col="着順", n_col="頭数") -> dict:
    """着順の整合: 各レースに rank==1 が1頭か・rank が 1..頭数 に収まるか。返す率の dict。"""
    import pandas as pd
    if df is None or len(df) == 0 or race_col not in df.columns or rank_col not in df.columns:
        return {"n_races": 0}
    r = pd.to_numeric(df[rank_col], errors="coerce")
    g = df.assign(_r=r).groupby(race_col)
    n_races = g.ngroups
    winners = g["_r"].apply(lambda s: (s == 1).sum())
    one_winner = float((winners == 1).mean())
    if n_col in df.columns:
        nn = pd.to_numeric(df[n_col], errors="coerce")
        in_range = float(((r >= 1) & (r <= nn)).mean())
    else:
        in_range = float("nan")
    return {"n_races": int(n_races), "one_winner_rate": one_winner,
            "rank_in_range_rate": in_range}


def _fmt_pct(x):
    return "n/a" if x != x else f"{x:.2%}"


def main() -> int:
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.pipeline._ingestion import load_raw

    ap = argparse.ArgumentParser(description="生DB完全性監査（自己整合・相互整合）")
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    def _jrdb(rt):
        try:
            from src.jrdb._store import JrdbStore
            return JrdbStore(args.db).read(rt)
        except Exception:  # noqa: BLE001
            return None

    print("=== 生DB完全性監査 ===")

    # JRDB 主要テーブルの PK 重複・行数・年レンジ
    jrdb_pk = {"SED": ["race_id", "umaban"], "KYI": ["race_id", "umaban"],
               "HJC": ["race_id"], "TYB": ["race_id", "umaban"]}
    print("\n[JRDB テーブル: 行数 / PK重複 / 年レンジ]")
    sed = None
    for rt, pk in jrdb_pk.items():
        d = _jrdb(rt)
        if rt == "SED":
            sed = d
        if d is None or len(d) == 0:
            print(f"  {rt:<5} 読込不可/空")
            continue
        dup = pk_duplicate_count(d, pk)
        yr = year_span(d["race_id"].astype(str).str[:4]) if "race_id" in d.columns else {}
        print(f"  {rt:<5} 行={len(d):>8,}  PK重複={dup:>6,}  "
              f"年={yr.get('min')}-{yr.get('max')} 欠落{yr.get('missing')}")

    # netkeiba raw の相互整合（孤児キー）＋着順整合
    res = load_raw(LocalPaths.RAW_RESULTS_PATH)
    ri = load_raw(LocalPaths.RAW_RACE_INFO_PATH)
    hr = load_raw(LocalPaths.RAW_HORSE_RESULTS_PATH)

    def _ids(df, col):
        return df[col].astype(str) if (df is not None and col in df.columns) else []

    print("\n[相互整合: 孤児キー率]")
    if res is not None and ri is not None:
        o = orphan_rate(_ids(res, "race_id"), _ids(ri, "race_id"))
        print(f"  results.race_id ∉ race_info: {o['n_orphan']:,}/{o['n_child']:,} = {_fmt_pct(o['rate'])}")
    if res is not None and hr is not None:
        o = orphan_rate(_ids(hr, "horse_id"), _ids(res, "horse_id"))
        print(f"  horse_results.horse_id ∉ results: {o['n_orphan']:,}/{o['n_child']:,} = {_fmt_pct(o['rate'])}"
              "（別ID体系なら高率＝既知）")

    print("\n[必須列 NULL 率]（results）")
    if res is not None:
        for c in ("race_id", "馬番", "着順", "単勝", "horse_id"):
            print(f"  {c:<10} {_fmt_pct(null_rate(res, c))}")

    print("\n[着順整合]（各レースに1着1頭・rank∈1..頭数）")
    rc = rank_consistency(res) if res is not None else {"n_races": 0}
    if rc.get("n_races"):
        print(f"  results: races={rc['n_races']:,}  1着1頭率={_fmt_pct(rc['one_winner_rate'])}  "
              f"rank範囲内率={_fmt_pct(rc.get('rank_in_range_rate'))}")

    if ri is not None and "race_id" in ri.columns:
        ys = year_span(ri["race_id"].astype(str).str[:4])
        print(f"\n[race_info 年レンジ] {ys['min']}-{ys['max']}  欠落年={ys['missing']}  行={len(ri):,}")

    print("\n※ これは DB 側の自己/相互整合。元ZIP/TXT/CSV 件数との 1:1 照合はソースファイルが要る（別途ローカル）。"
          "owner/血統/guide/race_class 派生の充足は audit_feature_coverage を参照。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
