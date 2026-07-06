"""補助 CSV（corner_passing_order / odds / laptime 等）を profiling し、seed への結合可否を判定する。

メイン結果 CSV（seed_results）は race_id=レースID、runner=レース馬番ID(=レースID×100+馬番) を
キーに持つ。補助ファイルがこれらのキーで結合できるか、どんな列（脚質/ラップ/複勝・連系オッズ）を
足せるかを見て、form 強化や overlay 実験への活用可否を判断する。

特に odds ファイルは「複勝・連系（馬連/三連複…）の確定オッズ」を含むかを重点チェックする
（含むなら 1986-2021 の overlay=市場歪み特徴が作れ、以前カバレッジ不足で検証不能だった複勝 overlay
仮説を 35年スケールで検証できる）。

使い方（3ファイルとも）:
    python analyze_aux_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/20020615-20210731_corner_passing_order.csv"
    python analyze_aux_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_odds.csv"
    python analyze_aux_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_laptime.csv"
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

# seed への結合キー候補（結果 CSV と同じ命名を想定）。
KEY_RACE = ["レースID", "race_id", "raceid"]
KEY_RUNNER = ["レース馬番ID", "レース馬番id", "馬番id"]
KEY_UMABAN = ["馬番", "umaban"]
KEY_NAME = ["馬名", "horse_name"]
KEY_DATE = ["レース日付", "日付", "date"]

# odds ファイルで探す券種キーワード（列名 or 値に現れうる）。
BET_KEYWORDS = {
    "単勝": ["単勝", "tansho", "win"],
    "複勝": ["複勝", "fukusho", "place", "show"],
    "枠連": ["枠連", "wakuren"],
    "馬連": ["馬連", "umaren", "quinella"],
    "馬単": ["馬単", "umatan", "exacta"],
    "ワイド": ["ワイド", "wide", "quinella_place"],
    "三連複": ["三連複", "sanrenpuku", "trio"],
    "三連単": ["三連単", "sanrentan", "trifecta"],
}

# 脚質/展開に効く列キーワード（corner_passing_order 用）。
PACE_KEYWORDS = ["コーナー", "corner", "通過", "passing", "位置", "order", "脚質"]
# ラップ/タイムに効く列キーワード（laptime 用）。
LAP_KEYWORDS = ["ラップ", "lap", "タイム", "time", "ハロン", "furlong", "pace", "上り", "上がり"]


def _read(path: str, nrows: int | None):
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis", "euc-jp"):
        try:
            return pd.read_csv(path, nrows=nrows, encoding=enc, low_memory=False), enc
        except Exception:  # noqa: BLE001
            continue
    raise RuntimeError("読み込み失敗（エンコーディング）。")


def _count_rows(path: str, enc: str) -> int:
    try:
        with open(path, encoding=enc, errors="replace") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:  # noqa: BLE001
        return -1


def _find(cols_lower: dict, aliases: list[str]) -> str | None:
    for a in aliases:
        if a.lower() in cols_lower:
            return cols_lower[a.lower()]
    return None


def _keyword_cols(cols: list[str], keywords: list[str]) -> list[str]:
    out = []
    for c in cols:
        cl = str(c).lower()
        if any(k.lower() in cl for k in keywords):
            out.append(c)
    return out


def run(path: str) -> int:
    if not os.path.isfile(path):
        print(f"[NG] 見つかりません: {path}（WSL は /mnt/c/... 形式）")
        return 2
    size_mb = os.path.getsize(path) / 1e6
    sample, enc = _read(path, 5000)
    total = _count_rows(path, enc)
    cols = list(sample.columns)
    cols_lower = {str(c).lower(): c for c in cols}

    print("=" * 78)
    print(f"補助CSV分析: {os.path.basename(path)}")
    print(f"  サイズ={size_mb:,.1f}MB  enc={enc}  総行数={total:,}（推定）  列数={len(cols)}")
    print("=" * 78)
    print("列・dtype・欠損率（先頭5000行）:")
    for c in cols:
        print(f"    {str(c):<30} {str(sample[c].dtype):<10} 欠損={sample[c].isna().mean()*100:5.1f}%")
    print("\nサンプル3行:")
    with pd.option_context("display.max_columns", None, "display.width", 220):
        print(sample.head(3).to_string())

    # --- 結合キー ---
    print("\n" + "-" * 78)
    print("seed への結合キー検出:")
    kr = _find(cols_lower, KEY_RACE)
    krun = _find(cols_lower, KEY_RUNNER)
    kuma = _find(cols_lower, KEY_UMABAN)
    kname = _find(cols_lower, KEY_NAME)
    for label, hit in (("レースID(race粒度)", kr), ("レース馬番ID(runner粒度)", krun),
                       ("馬番", kuma), ("馬名", kname)):
        print(f"    {'✓' if hit else '✗'} {label:<24} {('← '+hit) if hit else '（無し）'}")

    granularity = None
    if krun:
        granularity = "runner"  # レース馬番ID があれば馬単位で厳密結合
    elif kr and kuma:
        granularity = "runner"  # レースID+馬番でも馬単位
    elif kr:
        granularity = "race"    # レース単位

    # --- 中身の種別推定 ---
    print("\n内容の推定:")
    odds_hits = {bt: _keyword_cols(cols, kw) for bt, kw in BET_KEYWORDS.items()}
    odds_hits = {k: v for k, v in odds_hits.items() if v}
    pace_cols = _keyword_cols(cols, PACE_KEYWORDS)
    lap_cols = _keyword_cols(cols, LAP_KEYWORDS)
    if odds_hits:
        print("  [オッズ] 検出した券種列:")
        for bt, cs in odds_hits.items():
            print(f"    {bt}: {cs}")
        exotic = [bt for bt in odds_hits if bt not in ("単勝",)]
        if "複勝" in odds_hits or exotic:
            print("  → ★複勝/連系の確定オッズを含む可能性。含むなら 1986-2021 の overlay(市場歪み)特徴が")
            print("     作れ、複勝 overlay 仮説を 35年スケールで検証可能（以前はカバレッジ ~2% で不能だった）。")
        else:
            print("  → 単勝のみらしい。results と重複が大きく overlay には不足（複勝確定オッズが要る）。")
    if pace_cols:
        print(f"  [脚質/展開] 通過・コーナー系の列: {pace_cols}")
        print("    → 脚質(逃げ/先行/差し/追込)・pace 特徴を form に付与できる（page専用だったもの）。")
    if lap_cols:
        print(f"  [ラップ/タイム] {lap_cols}")
        print("    → pace/speed 指標を作れる（page専用だったもの）。")
    if not (odds_hits or pace_cols or lap_cols):
        print("  → 既知キーワードに未該当。列一覧から内容を判断（サンプルを共有ください）。")

    # --- 判定 ---
    print("\n" + "=" * 78)
    print("結合判定:")
    if granularity == "runner":
        print("  ✅ 馬単位で seed_results に結合可能（レース馬番ID or レースID+馬番）。")
        print("     → 脚質/ラップ/複勝オッズを馬ごとに付与できる。")
    elif granularity == "race":
        print("  ✅ レース単位で seed_race_info に結合可能（レースID）。")
        print("     → レース属性（ラップ全体・オッズ表）として付与、馬単位は別途按分。")
    else:
        print("  ⚠ 明確な結合キー未検出。列一覧・サンプルを共有ください（キー名の対応を確定）。")
    print("=" * 78)
    print("次: 3ファイルの出力を貼ってください。結合粒度と追加特徴を確定し、")
    print("form 強化（脚質/pace/speed）と overlay(複勝確定オッズ) の実装可否を判断します。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="補助CSVの profiling と seed 結合可否判定")
    ap.add_argument("path", help="CSV パス（WSL からは /mnt/c/... 形式）")
    return run(ap.parse_args().path)


if __name__ == "__main__":
    sys.exit(main())
