"""JRDB で netkeiba raw を補完/上書きする（dry-run 既定）。

raw_jrdb_sed → adapters → netkeiba raw スキーマへ整形し、既存 netkeiba raw pickle
（results/race_info/horse_results）へ反映する。2 モード:

  fill      : 既存に無い race_id だけ追加（NAR/既存年 非改変）。当初の欠損補完用。
  overwrite : 対象年の JRA race_id を JRDB 行で置換。netkeiba の JRA スクレイプが
              壊れている年（1レース数頭しか無い 2023-2025 等）を JRDB 完全データで
              修復し、目的変数が空の死年（2015-2020 等）を蘇生する。JRDB に無い
              race_id（NAR）は置換対象外なので保護される。

horse_results は両モードとも「既存に無い (horse_id,日付) を追加」で過去走履歴を包括補完。
連結後に主キー keep-last dedup を掛けるので既存の重複も掃除される。

使い方:
  # 補完（従来）: 欠損 race_id だけ追加
  python scripts/jrdb_fill_netkeiba.py --year 2021 --year 2022
  # 上書き（推奨・壊れJRA修復＋死年蘇生）: まず dry-run で置換件数を確認
  python scripts/jrdb_fill_netkeiba.py --mode overwrite \
      --year 2015 --year 2016 ... --year 2026
  # 問題なければ --apply（元 pickle は .bak にバックアップ）
  python scripts/jrdb_fill_netkeiba.py --mode overwrite --year 2015 ... --apply
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._local_paths import LocalPaths  # noqa: E402
from src.jrdb._crosswalk import read_crosswalk  # noqa: E402
from src.jrdb._fill import (  # noqa: E402
    build_fill_tables,
    drop_race_ids,
    filter_years,
    new_by_race_id,
    new_horse_results,
    race_ids_of,
    to_raw_shape,
)
from src.storage._db import get_engine  # noqa: E402

_PATHS = {
    "results": LocalPaths.RAW_RESULTS_PATH,
    "race_info": LocalPaths.RAW_RACE_INFO_PATH,
    "horse_results": LocalPaths.RAW_HORSE_RESULTS_PATH,
}
# 連結後の防御的 keep-last dedup 用（主キー）。既存の重複も掃除される。
_PK = {"results": ["race_id", "馬番"], "race_info": ["race_id"],
       "horse_results": ["horse_id", "日付"]}


def _parse_args(argv):
    ap = argparse.ArgumentParser(description="JRDB で netkeiba 欠損年を補完（dry-run 既定）")
    ap.add_argument("--year", action="append", default=None, metavar="YYYY",
                    help="対象年（results/race_info）。複数可。例 --year 2021 --year 2022")
    ap.add_argument("--mode", choices=["fill", "overwrite"], default="fill",
                    help="fill=既存に無い race_id だけ追加（既定・NAR/既存年 非改変）。"
                         "overwrite=対象年の JRA race_id を JRDB 行で置換（netkeiba の壊れた"
                         "JRA を修復・死年を蘇生。JRDB に無い race_id=NAR は保護）")
    ap.add_argument("--db", default=None, help="SQLite パス（既定 LocalPaths.DB_PATH）")
    ap.add_argument("--apply", action="store_true",
                    help="実際に pickle へ書込（既定は dry-run で書き込まない）")
    return ap.parse_args(argv)


def _dedup_keep_last(df, name):
    """連結後の主キー重複を keep-last で掃除する。(df, 除去数) を返す。"""
    pk = [c for c in _PK[name] if c in df.columns]
    if not pk:
        return df, 0
    before = len(df)
    out = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    return out, before - len(out)


def _load_existing(name):
    """既存 netkeiba raw pickle を読む（無ければ None）。"""
    p = Path(_PATHS[name])
    if not p.exists():
        return None
    return pd.read_pickle(p)


def _load_kyi(engine):
    """raw_jrdb_kyi から 枠番・性齢 補完用の列を読む（無い列はスキップ・失敗時 None）。

    以前は固定 SELECT が 1 列でも欠けると例外→握り潰しで無言スキップしていた
    （sex_code 未取込の DB で 枠番まで補完されない事故）。存在する列だけを選び、
    race_id/umaban/wakuban が揃えば 枠番を補完する。sex_code が無ければ 性齢だけスキップ。
    """
    try:
        have = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), engine).columns.tolist()
    except Exception as e:  # noqa: BLE001 — 未取込なら補完なしで続行
        print(f"[fill] raw_jrdb_kyi を読めません（{e}）→ 枠番/性齢は欠損のまま", file=sys.stderr)
        return None
    cols = [c for c in ("race_id", "umaban", "wakuban", "sex_code") if c in have]
    if not {"race_id", "umaban", "wakuban"} <= set(cols):
        print(f"[fill] raw_jrdb_kyi に必要列が不足（有: {cols}）→ 枠番/性齢は欠損のまま",
              file=sys.stderr)
        return None
    if "sex_code" not in cols:
        print("[fill] ⚠ raw_jrdb_kyi に sex_code 列が無く 性齢 は補完できません（枠番のみ補完）。"
              "KYI を再取込すると sex_code が入ります。", file=sys.stderr)
    return pd.read_sql(text(f"SELECT {','.join(cols)} FROM raw_jrdb_kyi"), engine)


def _existing_race_ids(df):
    if df is None:
        return set()
    if df.index.name == "race_id":
        return set(df.index.astype(str))
    if "race_id" in df.columns:
        return set(df["race_id"].astype(str))
    return set()


def _existing_hr_keys(df):
    if df is None or "日付" not in df.columns:
        return set()
    hid = df.index.astype(str) if df.index.name == "horse_id" else df.get("horse_id")
    if hid is None:
        return set()
    return set(zip(pd.Series(hid).astype(str), df["日付"].astype(str), strict=False))


def main(argv=None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    years = args.year or ["2021", "2022"]
    engine = get_engine(args.db)

    print(f"[fill] 対象年 {years}（{'APPLY' if args.apply else 'DRY-RUN'}）")
    sed = pd.read_sql(text("SELECT * FROM raw_jrdb_sed"), engine)
    print(f"[fill] raw_jrdb_sed 読込 {len(sed):,} 行")
    # KYI（出馬表）から 枠番・性別 を補う（race_id×馬番）。無くても動く（枠番/性齢は欠損のまま）。
    kyi = _load_kyi(engine)
    if kyi is not None:
        print(f"[fill] raw_jrdb_kyi 読込 {len(kyi):,} 行（枠番・性齢の補完に使用）")
    built = build_fill_tables(sed, jockey_xwalk=read_crosswalk("jockey", db_path=args.db),
                              trainer_xwalk=read_crosswalk("trainer", db_path=args.db), kyi=kyi)

    existing = {n: _load_existing(n) for n in _PATHS}
    ex_hr_keys = _existing_hr_keys(existing["horse_results"])
    overwrite = args.mode == "overwrite"

    # results / race_info の対象行と、置換で消す既存 race_id を mode 別に決める。
    #  fill      : 既存に無い race_id だけ追加（NAR/既存年 非改変）
    #  overwrite : 対象年の JRDB 行“全て”で、その race_id の既存行を置換（壊れJRA修復・死年蘇生）
    jr_res = filter_years(built["results"], years)
    jr_ri = filter_years(built["race_info"], years)
    if overwrite:
        res_new, ri_new = jr_res, jr_ri
        drop_ids = {"results": set(race_ids_of(jr_res)), "race_info": set(race_ids_of(jr_ri))}
    else:
        res_new = new_by_race_id(jr_res, _existing_race_ids(existing["results"]))
        ri_new = new_by_race_id(jr_ri, _existing_race_ids(existing["race_info"]))
        drop_ids = {"results": set(), "race_info": set()}
    # horse_results は両モードとも「既存に無い (horse_id,日付) を追加」（過去走履歴を包括補完）。
    hr_new = new_horse_results(built["horse_results"], ex_hr_keys)

    print(f"\n[fill] mode={args.mode} 対象:")
    if overwrite:
        print(f"  results:       置換 {len(drop_ids['results']):,} race → JRDB {len(res_new):,} 行")
        print(f"  race_info:     置換 {len(drop_ids['race_info']):,} race → JRDB {len(ri_new):,} 行")
    else:
        print(f"  results:       追加 {len(res_new):,} 行（新規 race_id のみ）")
        print(f"  race_info:     追加 {len(ri_new):,} 行")
    print(f"  horse_results: 追加 {len(hr_new):,} 行（全年・既存 {len(ex_hr_keys):,} と重複除外）")

    # 列整合チェック（既存 netkeiba と生成側の列差）
    for name, new_df in (("results", res_new), ("race_info", ri_new),
                         ("horse_results", hr_new)):
        ex = existing[name]
        if ex is not None and new_df is not None and not new_df.empty:
            excols = set(ex.columns) | ({ex.index.name} if ex.index.name else set())
            newcols = set(new_df.columns) | ({new_df.index.name} if new_df.index.name else set())
            only_nk = sorted(excols - newcols)
            only_jr = sorted(newcols - excols)
            print(f"\n  [{name}] 列差: netkeiba のみ={only_nk[:12]}"
                  + (" …" if len(only_nk) > 12 else ""))
            if only_jr:
                print(f"            JRDB のみ={only_jr}")

    if not args.apply:
        print(f"\n[fill] DRY-RUN 完了（書き込みなし）。問題なければ --apply で {args.mode}。")
        return 0

    for name, new_df in (("results", res_new), ("race_info", ri_new),
                         ("horse_results", hr_new)):
        if new_df is None or new_df.empty:
            continue
        p = Path(_PATHS[name])
        # overwrite: 対象 race_id の既存行を消してから連結（NAR 等は drop 対象外＝保持）。
        ex = drop_race_ids(existing[name], drop_ids[name]) if overwrite else existing[name]
        # netkeiba raw は RangeIndex＋キー列。既存/生成側とも named index（race_id 等）は列へ
        # 戻してから連結する（ignore_index の列アライン崩れを防ぐ）。
        ex = to_raw_shape(ex)
        new_df = to_raw_shape(new_df)
        if p.exists():
            shutil.copy2(p, str(p) + ".bak")   # 元をバックアップ
        merged = (pd.concat([ex, new_df], ignore_index=True) if ex is not None
                  else new_df.reset_index(drop=True))
        merged, n_dup = _dedup_keep_last(merged, name)   # 防御的 keep-last（既存重複も掃除）
        p.parent.mkdir(parents=True, exist_ok=True)
        merged.to_pickle(p)
        dupmsg = f" / 重複除去 {n_dup:,}" if n_dup else ""
        print(f"[fill] {name}: → {len(merged):,} 行 保存{dupmsg}（{p.name}, 元は .bak）")
    print(f"[fill] APPLY 完了（mode={args.mode}）。featured を再生成すると反映されます。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
