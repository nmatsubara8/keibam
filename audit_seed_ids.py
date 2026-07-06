"""seed 用 CSV の ID/キーの「正体」を実データで監査する（変換前の安全確認）。

「桁数が同じ＝同じ意味」とは限らない。このスクリプトは変換前に走らせ、レースID の
内部構造・馬番IDの構成・競馬場コードの範囲・地方海外の混入・馬名の同名別馬リスク・
race_id の一意性を実データで確認する。ここで PASS したものだけを合成IDの前提にする。

netkeiba の race_id と一致するかは **確認しない**（一致を前提にしないのが安全設計）。
確認するのは「CSV 内部で ID が自己整合しているか」「名前ベース合成 ID が安全に作れるか」。

使い方:
    python audit_seed_ids.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv"
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

# 実データの列名（analyze_seed_csv の出力で確認済み）。
C_RACE_ID = "レースID"
C_RUNNER_ID = "レース馬番ID"
C_KAI = "開催回数"
C_TRACK = "競馬場コード"
C_DAY = "開催日数"
C_RNO = "レース番号"
C_UMABAN = "馬番"
C_DATE = "レース日付"
C_NAME = "馬名"
C_SEX = "性別"
C_AGE = "馬齢"
C_REGION = "東西・外国・地方区分"

USECOLS = [C_RACE_ID, C_RUNNER_ID, C_KAI, C_TRACK, C_DAY, C_RNO, C_UMABAN,
           C_DATE, C_NAME, C_SEX, C_AGE, C_REGION]

# netkeiba/JRA 中央の競馬場コード（1-10）。これ以外は地方/海外の疑い。
JRA_TRACKS = {1: "札幌", 2: "函館", 3: "福島", 4: "新潟", 5: "東京",
              6: "中山", 7: "中京", 8: "京都", 9: "阪神", 10: "小倉"}


def _load(path: str) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            # 必要列だけ読む（471MB 全列は重い）。存在しない列は無視。
            head = pd.read_csv(path, nrows=1, encoding=enc)
            cols = [c for c in USECOLS if c in head.columns]
            return pd.read_csv(path, usecols=cols, encoding=enc, low_memory=False)
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("CSV を読めませんでした（エンコーディング）。")


def _pass(ok: bool) -> str:
    return "✅ PASS" if ok else "⚠ WARN"


def check_race_id_structure(df: pd.DataFrame) -> None:
    """レースID(12桁) = 年(4)|場(2)|回(2)|日(2)|R(2) が各列と一致するか。"""
    print("\n[1] レースID の内部構造（12桁 = 年|場|回|日|R が構成列と一致するか）")
    s = df[C_RACE_ID].astype("Int64").astype(str)
    lens = s.str.len().value_counts().to_dict()
    print(f"    桁数分布: {lens}")
    m = s.str.len() == 12
    d = df[m].copy()
    sid = d[C_RACE_ID].astype("Int64").astype(str)
    parts = {
        "年": (sid.str[0:4].astype(int), pd.to_datetime(d[C_DATE], errors="coerce").dt.year),
        "場": (sid.str[4:6].astype(int), d[C_TRACK].astype(int)),
        "回": (sid.str[6:8].astype(int), d[C_KAI].astype(int)),
        "日": (sid.str[8:10].astype(int), d[C_DAY].astype(int)),
        "R": (sid.str[10:12].astype(int), d[C_RNO].astype(int)),
    }
    all_ok = True
    for label, (embedded, col) in parts.items():
        mism = int((embedded.to_numpy() != col.to_numpy()).sum())
        all_ok = all_ok and mism == 0
        print(f"    {label}: レースID埋め込み vs {label}列  不一致 {mism:,} 行  {_pass(mism == 0)}")
    print(f"    → 全構成が一致すれば「レースID は構成列の決定的関数＝自己整合」: {_pass(all_ok)}")


def check_runner_id(df: pd.DataFrame) -> None:
    """レース馬番ID = レースID*100 + 馬番 か。"""
    print("\n[2] レース馬番ID の構成（= レースID×100 + 馬番 か）")
    expect = df[C_RACE_ID].astype("Int64") * 100 + df[C_UMABAN].astype("Int64")
    mism = int((expect.to_numpy() != df[C_RUNNER_ID].astype("Int64").to_numpy()).sum())
    print(f"    不一致 {mism:,} 行  {_pass(mism == 0)}")
    if mism:
        print("    → 別の連結規則（例: 馬番3桁）かも。実サンプルを確認要。")


def check_tracks_region(df: pd.DataFrame) -> None:
    """競馬場コードが 1-10（中央）に収まるか。地方海外区分の分布。"""
    print("\n[3] 競馬場コードの範囲と地方/海外の混入")
    vc = df[C_TRACK].value_counts().sort_index()
    outside = sorted(set(df[C_TRACK].dropna().astype(int)) - set(JRA_TRACKS))
    print(f"    競馬場コード種類: {sorted(set(df[C_TRACK].dropna().astype(int)))}")
    print(f"    中央(1-10)外のコード: {outside}  {_pass(not outside)}")
    if C_REGION in df.columns:
        print(f"    東西・外国・地方区分 の分布:\n{df[C_REGION].value_counts().to_string()}")
        print("    → 「地方」「外国」があれば、その馬の所属を表すだけか、地方開催まで含むか要判断。")


def check_name_identity(df: pd.DataFrame) -> None:
    """馬名ベース合成 horse_id の同名別馬リスク（年齢の非単調・長期ギャップ・性別分裂）。"""
    print("\n[4] 馬名→合成 horse_id の同名別馬リスク")
    d = df.dropna(subset=[C_NAME]).copy()
    d["_y"] = pd.to_datetime(d[C_DATE], errors="coerce").dt.year
    n_names = d[C_NAME].nunique()
    print(f"    ユニーク馬名: {n_names:,}")

    # (a) 同名で性別が複数 → ほぼ別馬
    sex_per_name = d.groupby(C_NAME)[C_SEX].nunique()
    multi_sex = int((sex_per_name > 1).sum())
    print(f"    (a) 同名で性別が複数の馬名: {multi_sex:,}  "
          f"{_pass(multi_sex < n_names * 0.005)}（別馬の可能性大→性別で分割）")

    # (b) 同名で出走年の span が極端に長い（>10年）→ 名前再利用の疑い
    span = d.groupby(C_NAME)["_y"].agg(lambda x: x.max() - x.min())
    long_span = int((span > 10).sum())
    print(f"    (b) 出走年 span > 10年 の馬名: {long_span:,}  "
          f"{_pass(long_span < n_names * 0.005)}（再利用の疑い→デビュー年で分割）")

    # (c) 馬齢が時系列で減少（1頭内で若返り）→ 別馬の混在
    d2 = d.dropna(subset=[C_AGE, "_y"]).sort_values([C_NAME, C_DATE])
    def _has_drop(g):
        a = pd.to_numeric(g[C_AGE], errors="coerce").to_numpy()
        return bool((a[1:] < a[:-1]).any()) if len(a) > 1 else False
    # 大きすぎると重いので、span>0 の馬名だけ検査
    suspects = span[span > 0].index
    sample = d2[d2[C_NAME].isin(suspects)]
    drop_names = sample.groupby(C_NAME, sort=False).filter(_has_drop)[C_NAME].nunique()
    print(f"    (c) 馬齢が途中で減少する馬名: {drop_names:,}  "
          f"{_pass(drop_names < n_names * 0.005)}（別馬融合の兆候→年齢連続性で分割）")
    print("    → (a)(b)(c) が僅少なら『馬名+性別+デビュー年』で安全に合成IDを作れる。")


def check_race_id_uniqueness(df: pd.DataFrame) -> None:
    """1 レースID が単一の (日付, 場) に対応するか（多対応なら ID が別物混在）。"""
    print("\n[5] レースID の一意性（1 ID ↔ 単一の日付・場か）")
    g = df.groupby(C_RACE_ID)
    multi_date = int((g[C_DATE].nunique() > 1).sum())
    multi_track = int((g[C_TRACK].nunique() > 1).sum())
    print(f"    複数日付を持つ レースID: {multi_date:,}  {_pass(multi_date == 0)}")
    print(f"    複数場を持つ レースID: {multi_track:,}  {_pass(multi_track == 0)}")


def run(path: str) -> int:
    if not os.path.isfile(path):
        print(f"[NG] ファイルが見つかりません: {path}（WSL は /mnt/c/... 形式）")
        return 2
    print("=" * 78)
    print(f"ID 監査: {os.path.basename(path)}")
    print("=" * 78)
    df = _load(path)
    print(f"読込: {len(df):,} 行 / 使用列 {list(df.columns)}")
    missing = [c for c in USECOLS if c not in df.columns]
    if missing:
        print(f"[注意] 期待列が無い: {missing}（一部チェックはスキップ）")

    if C_RACE_ID in df.columns:
        check_race_id_structure(df)
        check_race_id_uniqueness(df)
    if C_RUNNER_ID in df.columns:
        check_runner_id(df)
    if C_TRACK in df.columns:
        check_tracks_region(df)
    if C_NAME in df.columns:
        check_name_identity(df)

    print("\n" + "=" * 78)
    print("読み方: 全て ✅PASS なら、レースID は自己整合・中央のみ・合成IDも安全に作れる。")
    print("⚠WARN が出た項目は、変換スクリプトでその分割ルール（性別/デビュー年/年齢連続性）や")
    print("地方海外の除外を必ず入れる。netkeiba との race_id 一致は前提にしない（別コーパス扱い）。")
    print("=" * 78)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="seed CSV の ID 監査（変換前の安全確認）")
    ap.add_argument("path", help="CSV パス（WSL からは /mnt/c/... 形式）")
    return run(ap.parse_args().path)


if __name__ == "__main__":
    sys.exit(main())
