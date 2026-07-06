"""seed_*.pkl（CSV変換済み）だけから featured_data を組む（スタンドアロン検証用）。

seed には horse_results / horse_info / peds が無いため:
  - horse_results は KEIBA_FORM_FROM_RESULTS=1 で results 自己結合から再構成（率/適性/距離/馬場系）。
  - horse_info / peds は「正しい列を持つ空フレーム」を供給し、Processor を no-op で通す
    （馬齢・血統系の特徴は NaN になるが MVP では許容。form/person_te/Elo の疎通を確認するのが目的）。

出力: data/raw/seed_featured_data.pkl（既存 featured_data.pkl は無改変）。
この featured で 35年モデルを学習し、既存モデルと logloss/ECE/AUC を比較して
「35年分が効くか」を統合前に測る。

使い方:
    python build_seed_featured.py                 # data/raw/seed_*.pkl を読む
    python build_seed_featured.py --results <p> --race-info <p> --out <p>
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile

import pandas as pd

# form-from-results を既定 ON（seed は馬ページ非取得）。
os.environ.setdefault("KEIBA_FORM_FROM_RESULTS", "1")


def _empty_horse_results(path: str) -> None:
    """HorseResultsProcessor が no-op で通る空フレーム（全列 + horse_id index）を書く。"""
    from src.constants._horse_results_cols import HorseResultsCols as HR

    # HorseResultsProcessor は末尾で set_index("horse_id") するため horse_id は **列**で持つ。
    cols = ["horse_id", HR.DATE, HR.PLACE, HR.WEATHER, HR.R, HR.RACE_NAME, HR.N_HORSES,
            HR.WAKUBAN, HR.UMABAN, HR.TANSHO_ODDS, HR.POPULARITY, HR.RANK, HR.JOCKEY, HR.KINRYO,
            HR.RACE_TYPE_COURSE_LEN, HR.GROUND_STATE, HR.TIME, HR.RANK_DIFF, HR.CORNER,
            HR.PACE, HR.NOBORI, HR.WEIGHT_AND_DIFF, HR.PRIZE]
    pd.DataFrame(columns=cols).to_pickle(path)


def _empty_horse_info(path: str) -> None:
    """HorseInfoProcessor が no-op で通る空フレーム（horse_id 列 + 生年月日/owner_id/breeder_id）。"""
    from src.constants._horse_info_cols import HorseInfoCols as HI

    df = pd.DataFrame(columns=["horse_id", HI.BIRTHDAY, "owner_id", "breeder_id"])
    df.to_pickle(path)


def _empty_peds(path: str) -> None:
    """PedsProcessor が no-op で通る空フレーム（horse_id + peds_0..2）。"""
    df = pd.DataFrame(columns=["horse_id", "peds_0", "peds_1", "peds_2"])
    df.to_pickle(path)


def run(args) -> int:
    for p in (args.results, args.race_info):
        if not os.path.isfile(p):
            print(f"[NG] 見つかりません: {p}（先に seed_from_csv.py を実行）")
            return 2

    from src.pipeline._ingestion import IngestConfig, save_raw
    from src.pipeline.commands._ingest import _build_featured_data

    tmp = tempfile.mkdtemp(prefix="seed_featured_")
    hr = os.path.join(tmp, "horse_results.pkl")
    hi = os.path.join(tmp, "horse_info.pkl")
    pe = os.path.join(tmp, "peds.pkl")
    _empty_horse_results(hr)
    _empty_horse_info(hi)
    _empty_peds(pe)

    cfg = IngestConfig(
        raw_results_path=args.results,
        raw_race_info_path=args.race_info,
        raw_horse_results_path=hr,
        raw_horse_info_path=hi,
        raw_peds_path=pe,
        featured_data_path=args.out,
    )
    print("=" * 78)
    print("seed スタンドアロン featured 生成")
    print(f"  results={args.results}  race_info={args.race_info}")
    print(f"  form-from-results={os.environ.get('KEIBA_FORM_FROM_RESULTS')}  out={args.out}")
    print("=" * 78)

    featured = _build_featured_data(cfg)
    if featured is None or featured.empty:
        print("[NG] featured_data が空でした。")
        return 2

    save_raw(featured, args.out)
    n_races = featured.index.nunique() if hasattr(featured, "index") else 0
    print(f"[OK] featured_data: {len(featured):,} 行 / {n_races:,} レース / {featured.shape[1]} 列")
    print(f"     → {args.out}")
    # 主要特徴の非null率（form/person_te/elo が乗ったか）
    probe = [c for c in featured.columns if any(
        k in c for k in ("win_rate", "place_rate", "_te", "elo", "rating", "単勝_log", "interval"))]
    if probe:
        print("     主要特徴の非null率（先頭20）:")
        for c in probe[:20]:
            print(f"       {c:<34} {featured[c].notna().mean()*100:5.1f}%")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="seed だけから featured_data を組む（検証用）")
    ap.add_argument("--results", default="data/raw/seed_results.pkl")
    ap.add_argument("--race-info", default="data/raw/seed_race_info.pkl")
    ap.add_argument("--out", default="data/raw/seed_featured_data.pkl")
    return run(ap.parse_args())


if __name__ == "__main__":
    sys.exit(main())
