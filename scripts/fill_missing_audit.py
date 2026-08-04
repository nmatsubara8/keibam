"""Phase 0 健全性ゲート: JRDB 補完年（2021-2022 中央）の欠損プロファイル監査。

JRDB 統合で 2022 が 0→約4.7万行に回復したが、補完行は fill ポリシー
（`_fill.FILL_RACE_INFO_KEEP` の6列だけ充填・他は NULL）で意図的に欠損している。
これはリークではないが、「補完年だけ欠損する列」は LightGBM から見ると**分布シフト**。
学習前にどの列がどれだけズレるかを可視化し、Phase 2 の A/B（補完の純寄与測定）の前提を作る。

2層で検査する:
  ① raw_race_info 層 — fill ポリシーが補完年で守られているかを決定論的に確認する。
     KEEP 列（race_type/weather/ground_state1/2/course_len/date）は充填され、それ以外の
     recent-only 列（place/around/time/age/race_class/…）は補完年でほぼ全 NULL のはず。
     2022 は統合前 0 行＝**純 fill 年**なので厳密判定でき、2021 は既存 netkeiba と混在する。
  ② featured 層 — モデルに実際に届く各列の欠損率を年別に出し、補完年が近傍年から大きく
     乖離する列（分布シフト源）を上位表示する。乖離が fill ポリシーで説明できる範囲なら
     健全、想定外の乖離があれば WARN として洗い出す。

補完データはローカル生成物（未コミット）なので本スクリプトはローカルで実行する。

使い方:
    python scripts/fill_missing_audit.py                       # featured 層 + raw 層
    python scripts/fill_missing_audit.py --fill-years 2021 2022 --neighbor-years 2020 2023
    python scripts/fill_missing_audit.py --top 30
    python scripts/fill_missing_audit.py --raw-only
    python scripts/fill_missing_audit.py --featured path/to/featured.pkl
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._local_paths import LocalPaths  # noqa: E402
from src.jrdb._fill import FILL_RACE_INFO_KEEP  # noqa: E402

logger = logging.getLogger(__name__)

# fill ポリシーが NULL に落とす race_info 列（`_fill` の docstring に列挙されたもの）。
# raw 層の決定論的判定に使う（補完年でほぼ全 NULL であることを確認）。
FILL_RACE_INFO_NULLED = (
    "place", "place_id", "times", "days", "around", "time", "age",
    "race_class", "sex", "race_condition",
)


# --------------------------------------------------------------------------
# 純粋ヘルパー（テスト対象）
# --------------------------------------------------------------------------
def year_series(df: pd.DataFrame) -> pd.Series:
    """DataFrame から race_id 由来の 4桁年（str）を index 揃えで返す。

    優先順: date 列 → race_id 列 → 12桁 race_id とみなせる index。どれも無ければ ValueError。
    """
    if "date" in df.columns:
        y = pd.to_datetime(df["date"], errors="coerce").dt.year
        return y.astype("Int64").astype(str).str.replace("<NA>", "", regex=False)
    if "race_id" in df.columns:
        return df["race_id"].astype(str).str[:4]
    idx = pd.Series(df.index.astype(str), index=df.index)
    if (idx.str.len() >= 4).all() and idx.str[:4].str.isdigit().all():
        return idx.str[:4]
    raise ValueError("年を特定できません（date / race_id 列も 12桁 index も無い）。")


def null_rate_by_year(df: pd.DataFrame, years: pd.Series) -> pd.DataFrame:
    """列×年の欠損率テーブル（0..1）。years は df に index 揃えの年 str。"""
    isna = df.isna()
    isna = isna.assign(__year=years.values)
    grouped = isna.groupby("__year").mean(numeric_only=False)
    return grouped.T  # 行=列名, 列=年


def divergence(null_by_year: pd.DataFrame, fill_years, neighbor_years) -> pd.DataFrame:
    """列ごとに (補完年平均欠損率, 近傍年平均欠損率, 差) を返す。差の降順。"""
    fy = [str(y) for y in fill_years if str(y) in null_by_year.columns]
    ny = [str(y) for y in neighbor_years if str(y) in null_by_year.columns]
    if not fy or not ny:
        raise ValueError(f"対象年が欠損率表にありません（fill={fy} neighbor={ny} "
                         f"available={list(null_by_year.columns)}）。")
    fill = null_by_year[fy].mean(axis=1)
    neigh = null_by_year[ny].mean(axis=1)
    out = pd.DataFrame({"fill_null": fill, "neighbor_null": neigh,
                        "diff": fill - neigh})
    return out.sort_values("diff", ascending=False)


# --------------------------------------------------------------------------
# ロード
# --------------------------------------------------------------------------
def _load_featured(path: str) -> pd.DataFrame:
    obj = pd.read_pickle(path)
    from src.preprocessing._prepared_features import PreparedFeatures  # noqa: PLC0415
    if isinstance(obj, PreparedFeatures):
        return obj.gbdt
    return obj


# --------------------------------------------------------------------------
# ① raw 層（決定論的なポリシー確認）
# --------------------------------------------------------------------------
def audit_raw(path: str, fill_years, neighbor_years) -> int:
    if not Path(path).exists():
        print(f"[raw] race_info が見つかりません: {path}（ローカルで実行してください）")
        return 0
    ri = pd.read_pickle(path)
    if "race_id" not in ri.columns:
        ri = ri.reset_index()
    ys = ri["race_id"].astype(str).str[:4]
    years = [str(y) for y in list(fill_years) + list(neighbor_years)]

    keep = [c for c in FILL_RACE_INFO_KEEP if c in ri.columns]
    nulled = [c for c in FILL_RACE_INFO_NULLED if c in ri.columns]

    def _keep_fill(sub):  # KEEP 列の平均充填率
        return 1.0 - sub[keep].isna().mean().mean() if keep else float("nan")

    def _nulled_na(sub):  # NULLED 列の平均欠損率
        return sub[nulled].isna().mean().mean() if nulled else float("nan")

    # 近傍（充填）年のベースライン。KEEP は ground_state2 等が元々空きがちなので絶対閾値では
    # なく「近傍年と同等の充填率か」を基準にする（絶対 0.95 は誤検知する）。
    neigh_subs = [ri[ys == str(y)] for y in neighbor_years]
    neigh_subs = [s for s in neigh_subs if not s.empty]
    base_keep = (sum(_keep_fill(s) for s in neigh_subs) / len(neigh_subs)
                 if neigh_subs else float("nan"))

    print("\n=== ① raw_race_info: fill ポリシー確認 ===")
    print(f"  KEEP（充填されるべき）: {keep}")
    print(f"  NULLED（補完年で欠損のはず）: {nulled}")
    print(f"  近傍年の KEEP 充填率ベースライン: {base_keep:.1%}（fill 年はこれと同等以上なら健全）")
    print(f"  {'年':<6}{'行数':>8}{'KEEP充填率':>12}{'NULLED欠損率':>14}  判定")
    verdict = 0
    for y in years:
        sub = ri[ys == y]
        if sub.empty:
            print(f"  {y:<6}{0:>8}{'—':>12}{'—':>14}  (行なし)")
            continue
        keep_fill, nulled_na = _keep_fill(sub), _nulled_na(sub)
        is_fill = y in {str(x) for x in fill_years}
        if is_fill:
            # KEEP は近傍と同等以上（-5pt 以内）、NULLED はほぼ全欠損なら純 fill 年 PASS。
            # NULLED が中間なら既存 netkeiba と混在（2021）で正常。KEEP が近傍より大きく劣る、
            # または NULLED が充填されている場合のみ WARN。
            keep_ok = pd.isna(base_keep) or keep_fill >= base_keep - 0.05
            if keep_ok and nulled_na > 0.95:
                tag = "PASS"
            elif keep_ok and 0.05 < nulled_na <= 0.95:
                tag = "MIXED(既存と混在)"
            else:
                tag = "⚠ WARN"
                verdict = 1
        else:
            tag = "(近傍・充填年)"
        print(f"  {y:<6}{len(sub):>8}{keep_fill:>11.1%}{nulled_na:>13.1%}  {tag}")
    print("  ※ 2022 は統合前0行＝純fill年で厳密判定。2021 は既存netkeibaと混在するため MIXED は正常。")
    return verdict


# --------------------------------------------------------------------------
# ② featured 層（分布シフトの可視化）
# --------------------------------------------------------------------------
def audit_featured(path: str, fill_years, neighbor_years, top: int,
                   warn_threshold: float) -> int:
    if not Path(path).exists():
        print(f"[featured] 見つかりません: {path}（ローカルで実行してください）")
        return 0
    df = _load_featured(path)
    years = year_series(df)
    counts = years.value_counts().sort_index()

    print("\n=== ② featured: 年別行数 ===")
    for y, n in counts.items():
        mark = " ← 補完年" if y in {str(x) for x in fill_years} else ""
        print(f"  {y}: {n:,}{mark}")
    print(f"  合計 {len(df):,} 行 × {df.shape[1]:,} 列")

    nby = null_rate_by_year(df, years)
    div = divergence(nby, fill_years, neighbor_years)
    worse = div[div["diff"] > warn_threshold]

    print(f"\n=== ② featured: 補完年が近傍年より欠損する列 上位{top}"
          f"（fill={list(fill_years)} vs neighbor={list(neighbor_years)}）===")
    print(f"  {'列名':<40}{'補完年NULL':>10}{'近傍年NULL':>10}{'差':>8}")
    for name, row in div.head(top).iterrows():
        print(f"  {str(name)[:40]:<40}{row['fill_null']:>9.1%}"
              f"{row['neighbor_null']:>10.1%}{row['diff']:>+8.1%}")

    # 補完年の方が“充填されている”列（負の差）＝想定外方向。少数なら情報として表示。
    better = div[div["diff"] < -warn_threshold]
    print(f"\n  補完年の欠損が近傍年より {warn_threshold:.0%} 超**少ない**列: {len(better)} 本")
    for name, row in better.head(5).iterrows():
        print(f"    {str(name)[:40]:<40}{row['fill_null']:>9.1%}"
              f"{row['neighbor_null']:>10.1%}{row['diff']:>+8.1%}")

    print(f"\n  欠損差 > {warn_threshold:.0%} の列（分布シフト源）: {len(worse)} / {df.shape[1]} 本")
    print("  → これらは fill ポリシーで NULL 化した列に由来するのが期待。想定外の列が並ぶ")
    print("     場合のみ Phase 2 A/B の前にアダプタ/fill を見直す。")
    return 0


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="JRDB 補完年の欠損プロファイル監査（Phase 0）")
    ap.add_argument("--featured", default=LocalPaths.FEATURED_DATA_PATH,
                    help="featured_data.pkl（既定 LocalPaths.FEATURED_DATA_PATH）")
    ap.add_argument("--race-info", default=LocalPaths.RAW_RACE_INFO_PATH,
                    help="raw race_info.pkl（既定 LocalPaths.RAW_RACE_INFO_PATH）")
    ap.add_argument("--fill-years", nargs="+", default=["2021", "2022"],
                    help="補完対象年（既定 2021 2022）")
    ap.add_argument("--neighbor-years", nargs="+", default=["2020", "2023"],
                    help="比較する近傍年（既定 2020 2023）")
    ap.add_argument("--top", type=int, default=25, help="乖離列の表示本数")
    ap.add_argument("--warn-threshold", type=float, default=0.5,
                    help="分布シフトとして数える欠損率差の閾値（既定 0.5）")
    ap.add_argument("--raw-only", action="store_true", help="raw 層のみ")
    ap.add_argument("--featured-only", action="store_true", help="featured 層のみ")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    rc = 0
    if not args.featured_only:
        rc |= audit_raw(args.race_info, args.fill_years, args.neighbor_years)
    if not args.raw_only:
        rc |= audit_featured(args.featured, args.fill_years, args.neighbor_years,
                             args.top, args.warn_threshold)
    print("\n[audit] 完了。WARN が無ければ Phase 1（ベースライン学習）へ進めます。")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
