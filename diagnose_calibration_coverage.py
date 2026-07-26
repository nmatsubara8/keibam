"""較正のカバレッジ診断: なぜ単勝レースが少ないのかを切り分ける。

calibrate-takeout で「単勝 N レースのみ」となる原因が
  (A) raw_results 自体が少ない（データ量不足）か
  (B) 12154 行あるが '単勝' 列が非数値で tansho_odds_by_race_from_table に弾かれる（dtype）か
を判定する。

実行: python diagnose_calibration_coverage.py
"""

from __future__ import annotations

import pandas as pd

from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols
from src.policies._takeout_calibration import payout_lookup_from_return_processor
from src.policies._takeout_calibration import tansho_odds_by_race_from_table


def _read_db(alias: str) -> pd.DataFrame:
    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        if repo.has_rows(alias):
            return repo.read(alias)
    except Exception as e:  # noqa: BLE001
        print(f"  [DB読込失敗 {alias}] {e}")
    return pd.DataFrame()


def main() -> None:
    print("=" * 72)
    print("較正カバレッジ診断")
    print("=" * 72)

    # ---- raw_results（単勝の元）----
    print("\n■ raw_results（単勝の元）")
    db = _read_db("raw_results")
    pk = pd.DataFrame()
    import os

    if os.path.exists(LocalPaths.RAW_RESULTS_PATH):
        pk = pd.read_pickle(LocalPaths.RAW_RESULTS_PATH)

    for name, df in [("DB", db), ("pickle", pk)]:
        if df.empty:
            print(f"  [{name}] 空")
            continue
        n_rows = len(df)
        n_races = df.index.nunique() if df.index.name else df.get("race_id", pd.Series()).nunique()
        print(f"  [{name}] 行数={n_rows}  ユニークrace_id={n_races}")
        umc, odc = ResultsCols.UMABAN, ResultsCols.TANSHO_ODDS
        has_cols = umc in df.columns and odc in df.columns
        print(f"        列 '{umc}' 有={umc in df.columns} / '{odc}' 有={odc in df.columns}")
        if has_cols:
            odds = df[odc]
            num = pd.to_numeric(odds, errors="coerce")
            print(f"        '{odc}' dtype={odds.dtype}  非NaN={odds.notna().sum()}  "
                  f"数値化可={num.notna().sum()}  数値化不可={(odds.notna() & num.isna()).sum()}")
            print(f"        '{odc}' サンプル: {list(odds.dropna().astype(str).head(5))}")
            tmap = tansho_odds_by_race_from_table(df, umc, odc)
            print(f"        → tansho_odds_by_race_from_table: {len(tmap)} レース（≥2頭の正の単勝）")
        # race_id（index）の構造診断
        idx = df.index
        print(f"        index.name={idx.name!r}  dtype={idx.dtype}")
        print(f"        index サンプル: {list(idx.astype(str)[:5])}")
        rows_per = df.groupby(level=0).size()
        dist = rows_per.value_counts().sort_index()
        print(f"        1レースあたり行数の分布(行数:レース数, 上位): "
              f"{dict(list(dist.items())[:6])}")
        print(f"        ≥2行のrace_id数={int((rows_per >= 2).sum())} / "
              f"最大頭数={int(rows_per.max())}")

    # ---- raw_return_tables（払戻）----
    print("\n■ raw_return_tables（払戻）")
    from src.preprocessing._return_processor import ReturnProcessor
    import tempfile

    rdb = _read_db("raw_return_tables")
    rp = None
    if not rdb.empty:
        with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
            tmp = tf.name
        rdb.to_pickle(tmp)
        rp = ReturnProcessor(tmp)
        os.unlink(tmp)
    elif os.path.exists(LocalPaths.RAW_RETURN_TABLES_PATH):
        rp = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)

    if rp is not None:
        payout = payout_lookup_from_return_processor(rp)
        per_bt: dict[str, set] = {}
        for (race_id, bt, _combo) in payout:
            per_bt.setdefault(bt, set()).add(race_id)
        print(f"  払戻エントリ総数={len(payout)}  ユニークrace_id={len({k[0] for k in payout})}")
        print("  券種別の払戻レース数:")
        for bt, races in sorted(per_bt.items()):
            print(f"    {bt:<12} {len(races)} レース")
        # 払戻 race_id のサンプル（results との形式比較用）
        sample_payout_races = sorted({k[0] for k in payout})[:5]
        print(f"  払戻 race_id サンプル: {sample_payout_races}")
        # results の race_id サンプルと桁数を比較
        res_df = db if not db.empty else pk
        if not res_df.empty:
            res_ids = list(res_df.index.astype(str)[:5])
            print(f"  results race_id サンプル: {res_ids}")
            import collections
            res_len = collections.Counter(len(s) for s in res_df.index.astype(str))
            pay_len = collections.Counter(len(s) for s in {k[0] for k in payout})
            print(f"  results race_id 桁数分布: {dict(res_len)}")
            print(f"  払戻    race_id 桁数分布: {dict(pay_len)}")

        # ---- 重なり ----
        tmap = tansho_odds_by_race_from_table(
            db if not db.empty else pk, ResultsCols.UMABAN, ResultsCols.TANSHO_ODDS
        )
        tansho_races = set(tmap)
        print(f"\n■ 重なり（単勝 {len(tansho_races)} レース × 券種別払戻）")
        for bt, races in sorted(per_bt.items()):
            print(f"    {bt:<12} 重なり {len(tansho_races & races)} レース")

    print("\n" + "=" * 72)
    print("判定の見方:")
    print(" - raw_results の『ユニークrace_id』が小さい → データ量不足（ingest/復元が必要）")
    print(" - 行数は多いが tansho 構築が少ない → '単勝' の dtype/欠損問題")
    print(" - 単勝×券種の重なりが各券種で十分(>=20) → 較正できる")
    print("=" * 72)


if __name__ == "__main__":
    main()
