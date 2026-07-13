"""旧年代 walk-forward で「買い目0」になる原因＝単勝オッズ欠損を年代別に切り分ける。

walk_forward の EV 候補は ExpectedValueScorePolicy が付ける current_odds(=単勝) から
選ぶ（kelly_backtest._candidates_by_race）。単勝が NaN の馬は EV=prob×odds が NaN で
候補にならない。旧年代 fold(1993-2017)が全 fold で買い目0だったのは、featured の単勝が
その年代で欠損している疑い。これを:
  (1) featured 側の単勝 非NULL率（＝walk_forward が実際に使う値）
  (2) raw_results 側の単勝 非NULL率（＝netkeiba アーカイブに元から有ったか）
の2層で年代別に測り、「データ源の限界（raw に無い）」か「パイプラインで落とした（raw に有るのに
featured で消えた）」かを確定する。

実行例: python diag_tansho_odds.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# 年代バケット（walk_forward の 5分割 fold 境界に対応づけて読みやすく）
ERA_BUCKETS = [
    ("1986-1992", 1986, 1992),
    ("1993-2000", 1993, 2000),
    ("2001-2008", 2001, 2008),
    ("2009-2016", 2009, 2016),
    ("2017-2026", 2017, 2026),
]


def _report(odds, year, race_id, label, dtype):
    """odds/year/race_id は同じ長さの Series（位置対応）。年代別に非NULL率等を出す。"""
    import numpy as np
    import pandas as pd

    odds = pd.to_numeric(pd.Series(np.asarray(odds)), errors="coerce")
    year = pd.Series(np.asarray(year))
    race_id = pd.Series(np.asarray(race_id).astype(str))
    print(f"\n[{label}] 単勝 dtype={dtype}")
    print(f"  {'年代':<12}{'行数':>10}{'非NULL%':>10}{'>0%':>8}{'中央値':>8}"
          f"{'全馬NaNレース%':>15}")
    for lab, y0, y1 in ERA_BUCKETS:
        m = ((year >= y0) & (year <= y1)).to_numpy()
        n = int(m.sum())
        if n == 0:
            continue
        o = odds[m]
        rid = race_id[m]
        nn = float(o.notna().mean()) * 100
        pos = float((o > 0).mean()) * 100
        med = o.median()
        # レース単位で「単勝を1頭も持たない」割合（＝EV 候補が作れないレース）
        has_any = o.notna().groupby(rid.to_numpy()).any()
        all_nan_pct = float((~has_any).mean()) * 100
        med_txt = f"{med:.1f}" if med == med else "NaN"
        print(f"  {lab:<12}{n:>10,}{nn:>9.1f}%{pos:>7.1f}%{med_txt:>8}"
              f"{all_nan_pct:>14.1f}%")


def _race_id_series(df):
    """race_id を Series で取り出す（列 or index どちらでも）。"""
    import pandas as pd
    if "race_id" in df.columns:
        return df["race_id"]
    return pd.Series(df.index, index=df.index)


def _year_from_race_id(rid):
    import pandas as pd
    txt = pd.Series(rid).astype(str).str.replace(r"\.0$", "", regex=True)
    return pd.to_numeric(txt.str[:4], errors="coerce")


def main():
    import os

    import pandas as pd

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols

    odds_col = ResultsCols.TANSHO_ODDS  # '単勝'

    # (1) featured 側（walk_forward が実際に使う値）
    from app._model_eval import load_featured_data
    feat = load_featured_data()
    if feat is None or feat.empty:
        print("featured_data がありません"); return
    if odds_col not in feat.columns:
        cand = [c for c in feat.columns if "単" in str(c) or "odds" in str(c).lower()]
        print(f"featured に単勝列 '{odds_col}' が無い（似た列: {cand[:10]}）")
    else:
        fyear = pd.to_datetime(feat["date"], errors="coerce").dt.year
        _report(feat[odds_col], fyear, _race_id_series(feat), "featured_data（walk_forward が使う値）",
                feat[odds_col].dtype)

    # (2) raw_results 側（netkeiba アーカイブに元から有ったか）
    rpath = LocalPaths.RAW_RESULTS_PATH
    if not os.path.exists(rpath):
        print(f"\nraw_results.pkl が無い（{rpath}）— featured 側のみで判定"); return
    raw = pd.read_pickle(rpath)
    if odds_col not in raw.columns:
        print(f"\nraw_results に単勝列 '{odds_col}' が無い（列: {list(raw.columns)[:20]}）"); return
    rid = _race_id_series(raw)
    ryear = _year_from_race_id(rid)
    _report(raw[odds_col], ryear, rid, "raw_results（アーカイブ原本）", raw[odds_col].dtype)

    # (3) 払戻テーブル（return_tables）の年代別カバレッジ
    #     walk_forward の買い目は Simulator が単勝払戻テーブルで決済する。race_id が
    #     払戻テーブルに無いと bet_tansho が (0,0,0) を返し n_bets=0 で除外される
    #     （_betting_tickets._SingleStrategy.place）。単勝オッズが有っても払戻が無ければ買い目0。
    if odds_col in feat.columns:
        _return_coverage(feat, fyear)

    print("\n判定の読み方:")
    print("  ・単勝オッズは有る(featured 100%)のに買い目0 → 原因は EV でなく払戻テーブル欠損の疑い。")
    print("  ・(3)で旧年代の払戻カバレッジ%が低い → return_tables 未取得が原因＝買い目0の正体。")
    print("    対処: 旧年代の return_tables を ingest（払戻ページ取得）すれば旧 fold も決済可能になる。")


def _return_coverage(feat, fyear):
    """featured の各レースが単勝払戻テーブルに存在する割合を年代別に出す。"""
    import os

    import pandas as pd

    from src.constants._bet_types import BetType
    from src.constants._local_paths import LocalPaths
    from src.preprocessing._return_processor import ReturnProcessor

    rpath = LocalPaths.RAW_RETURN_TABLES_PATH
    if not os.path.exists(rpath):
        print(f"\nreturn_tables.pkl が無い（{rpath}）— 払戻カバレッジ判定不可"); return
    try:
        tables = ReturnProcessor(rpath).preprocessed_data
        tansho = tables[BetType.TANSHO]
    except Exception as e:  # noqa: BLE001
        print(f"\n払戻テーブルの読込/前処理に失敗: {e}"); return
    payout_ids = set(tansho.index.astype(str)) if tansho is not None and not tansho.empty else set()
    print(f"\n[払戻テーブル(単勝) カバレッジ] 払戻あり race_id 数={len(payout_ids):,}")
    print(f"  {'年代':<12}{'レース数':>10}{'払戻あり%':>12}")
    # featured のレース単位（race_id ×1）で判定
    rid = _race_id_series(feat).astype(str)
    df = pd.DataFrame({"race_id": rid.to_numpy(), "year": pd.Series(fyear).to_numpy()})
    df = df.drop_duplicates("race_id")
    for lab, y0, y1 in ERA_BUCKETS:
        m = (df["year"] >= y0) & (df["year"] <= y1)
        n = int(m.sum())
        if n == 0:
            continue
        cov = float(df.loc[m, "race_id"].isin(payout_ids).mean()) * 100
        print(f"  {lab:<12}{n:>10,}{cov:>11.1f}%")


if __name__ == "__main__":
    main()
