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


def norm_rank_series(s):
    """着順を頑健に整数化する（NFKC で全角→半角、先頭連続数字を採用）。純関数。

    '1'/'01'/'１'/'1着'→1、'取消'/'中止'/''→NaN。SQLite の無条件 CAST(→0) や pd.to_numeric
    （'1着'→NaN, '１'→NaN）が異常を隠す/誤検出するのを避ける（ユーザ指摘の年別表現差対策）。
    """
    import re
    import unicodedata

    import numpy as np
    import pandas as pd

    def _one(v):
        if v is None or (isinstance(v, float) and v != v):
            return np.nan
        m = re.match(r"^(\d+)", unicodedata.normalize("NFKC", str(v)).strip())
        return int(m.group(1)) if m else np.nan
    idx = s.index if isinstance(s, pd.Series) else None
    return pd.Series([_one(v) for v in s], index=idx)


def rank_consistency(df, *, race_col="race_id", rank_col="着順", n_col="頭数") -> dict:
    """着順の整合: 各レースに rank==1 が1頭か・rank が 1..頭数 に収まるか。返す率の dict。"""
    import pandas as pd
    if df is None or len(df) == 0 or race_col not in df.columns or rank_col not in df.columns:
        return {"n_races": 0}
    r = norm_rank_series(df[rank_col])
    r.index = df.index
    g = df.assign(_r=r).groupby(race_col)
    n_races = g.ngroups
    winners = g["_r"].apply(lambda s: (s == 1).sum())
    one_winner = float((winners == 1).mean())
    if n_col in df.columns:
        nn = pd.to_numeric(df[n_col], errors="coerce").values
        fin = r.notna().values
        in_range = float(((r.values[fin] >= 1) & (r.values[fin] <= nn[fin])).mean()) if fin.any() else float("nan")
    else:
        in_range = float("nan")
    return {"n_races": int(n_races), "one_winner_rate": one_winner,
            "rank_in_range_rate": in_range}


def race_stats_by_year(df, *, race_col="race_id", rank_col="着順") -> dict:
    """年別（race_id[:4]）に {n_races, rows_per_race, one_winner_rate} を返す純関数。

    1着1頭率が年で割れるかを見る＝古年 netkeiba の結果疎か・全年構造異常かを切り分ける
    （近年が高率なら学習対象は健全・古年疎は benign）。
    """
    if df is None or len(df) == 0 or race_col not in df.columns or rank_col not in df.columns:
        return {}
    rid = df[race_col].astype(str)
    r = norm_rank_series(df[rank_col]); r.index = df.index
    work = df.assign(_y=rid.str[:4].values, _r=r.values)
    out: dict = {}
    for y, sub in work.groupby("_y"):
        g = sub.groupby(race_col)["_r"]
        n_races = g.ngroups
        one = float((g.apply(lambda s: (s == 1).sum()) == 1).mean())
        out[str(y)] = {"n_races": int(n_races),
                       "rows_per_race": round(len(sub) / max(n_races, 1), 1),
                       "one_winner_rate": one}
    return out


def race_id_structure_by_year(df, *, race_col="race_id", years=None) -> dict:
    """年別に race_id の 長さ分布・場コード分布・サンプル値 を返す純関数（分裂/形式変化の特定用）。

    2025 で 1実レースが複数 race_id に分裂している疑いに対し、race_id 文字列そのものの構造
    （長さ・場コード[4:6]・末尾・例）を年で比べ、どの桁が変わったかを見る。
    """
    import pandas as pd
    if df is None or len(df) == 0 or race_col not in df.columns:
        return {}
    rid = df[race_col].astype(str)
    yr = rid.str[:4]
    out: dict = {}
    for y in (years or sorted(yr.unique())):
        r = rid[yr == y]
        if r.empty:
            continue
        uniq = r.drop_duplicates()
        lens = uniq.str.len().value_counts().to_dict()
        places = uniq.str[4:6].value_counts().head(6).to_dict()
        out[str(y)] = {
            "n_rows": int(len(r)), "n_unique_race_id": int(uniq.nunique()),
            "len_dist": {int(k): int(v) for k, v in lens.items()},
            "place_top": {k: int(v) for k, v in places.items()},
            "samples": uniq.head(4).tolist(),
        }
    return out


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

    # JRDB テーブル間の小差を anti-join で説明（TYB=SED+18 の18行はどちら片側か）
    tyb = _jrdb("TYB")
    if sed is not None and tyb is not None and all(
            "race_id" in d.columns and "umaban" in d.columns for d in (sed, tyb)):
        skey = set(zip(sed["race_id"].astype(str), sed["umaban"].astype(str), strict=False))
        tkey = set(zip(tyb["race_id"].astype(str), tyb["umaban"].astype(str), strict=False))
        print(f"\n[JRDB anti-join] TYBのみ(∉SED)={len(tkey - skey):,}  SEDのみ(∉TYB)={len(skey - tkey):,}"
              "（取消/発売対象外/提供差で説明できれば正常）")

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

    print("\n[着順整合]（頑健正規化 NFKC＋先頭数字＝'01'/'１'/'1着'も1着と判定）")
    rc = rank_consistency(res) if res is not None else {"n_races": 0}
    if rc.get("n_races"):
        print(f"  results: races={rc['n_races']:,}  1着1頭率={_fmt_pct(rc['one_winner_rate'])}  "
              f"rank範囲内率={_fmt_pct(rc.get('rank_in_range_rate'))}")
    # 着順の生表現分布（年で型が変わる＝24.6%の主因かを見る）
    if res is not None and "着順" in res.columns:
        vc = res["着順"].astype(str).value_counts().head(12)
        print("  着順 生値 上位:", {k: int(v) for k, v in vc.items()})
    # 年別 1着1頭率＋rows/race（全race_info）と、JRA限定（場コード01-10＝モデル対象）の両方。
    # NAR/出馬表stub が混じると全体は下がるが、JRA限定が高率なら学習対象は健全と確定できる。
    ys = race_stats_by_year(res) if res is not None else {}
    jra = None
    if res is not None and "race_id" in res.columns:
        pc = res["race_id"].astype(str).str[4:6]
        jra = race_stats_by_year(res[pc.isin({f"{i:02d}" for i in range(1, 11)})])
    if ys:
        print("  [年別] 全race_info（JRA+NAR+stub） / JRA限定(場01-10)  1着1頭率・rows/race")
        for y in sorted(k for k in ys if k >= "2015"):   # 近年（学習対象域）を全表示
            s = ys[y]
            j = (jra or {}).get(y, {})
            jtxt = (f"JRA: races={j['n_races']:>5,} rows/race={j['rows_per_race']:>4} "
                    f"1着1頭率={_fmt_pct(j['one_winner_rate'])}") if j else "JRA: —"
            print(f"    {y}: 全 races={s['n_races']:>6,} rows/race={s['rows_per_race']:>4} "
                  f"1着1頭率={_fmt_pct(s['one_winner_rate'])}  |  {jtxt}")
        # 古年 stub の確認（1行/race＝horse結果なし）
        old = {y: ys[y] for y in ys if y < "2015"}
        if old:
            stub = sum(1 for v in old.values() if v["rows_per_race"] <= 1.1)
            print(f"    [pre-2015] {len(old)}年中 {stub}年が rows/race≈1（＝horse結果なしのstub・JRDB前）")

    # race_id 構造の年比較（2023 正常年 vs 2025/2026 異常年）＝分裂の桁を特定
    st = race_id_structure_by_year(res, years=["2023", "2024", "2025", "2026"]) if res is not None else {}
    if st:
        print("\n[race_id 構造 年比較]（長さ分布/場コード上位/例＝分裂した桁の特定）")
        for y, s in st.items():
            print(f"  {y}: rows={s['n_rows']:,} 一意race_id={s['n_unique_race_id']:,} "
                  f"長さ={s['len_dist']} 場上位={s['place_top']}")
            print(f"       例: {s['samples']}")

    if ri is not None and "race_id" in ri.columns:
        ys = year_span(ri["race_id"].astype(str).str[:4])
        print(f"\n[race_info 年レンジ] {ys['min']}-{ys['max']}  欠落年={ys['missing']}  行={len(ri):,}")

    print("\n※ これは DB 側の自己/相互整合。元ZIP/TXT/CSV 件数との 1:1 照合はソースファイルが要る（別途ローカル）。"
          "owner/血統/guide/race_class 派生の充足は audit_feature_coverage を参照。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
