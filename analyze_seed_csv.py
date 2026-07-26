"""外部 CSV（Kaggle 等の 40年分レース結果）を keibam の raw_results 互換性で分析する。

40年分の一括 seed 候補（例: takamotoki の JRA データセット 1986-2021）を、スクレイプ無しで
コーパスの土台に使えるかを判定する。keibam の raw_results は結合キーとして **race_id と
horse_id**（＋ jockey_id / trainer_id / owner_id）を持ち、horse_results / peds / person 特徴
/ form-from-results はすべてこの netkeiba 数値 ID で結合する。よって seed 可否の核心は:

  1. CSV に race_id / horse_id（netkeiba の数値 ID）があるか  → あれば直接 seed 可能
  2. 無く馬名だけか                                          → 名前→ID 解決が必要（曖昧・要検討）

本スクリプトは CSV を安全に読み（日本語エンコーディング自動判定）、行数・期間・列・欠損率・
サンプルを出し、raw_results の期待列と突き合わせて「そのまま seed 可 / 要マッピング / 要ID解決」
を判定する。プロジェクト外でも動くよう期待スキーマは自己完結で持つ（ResultsCols があれば併用）。

使い方（WSL から Windows のファイルは /mnt/c/... で参照）:
    python analyze_seed_csv.py "/mnt/c/Users/Ayaka/Downloads/archive/19860105-20210731_race_result.csv"
"""

from __future__ import annotations

import argparse
import os
import sys

import pandas as pd

# raw_results が持つ列（keibam 標準）。左=正準名、右=CSV 側に現れうる別名（日本語/ローマ字）。
# 別名は緩めに拾って「意味的に対応する列があるか」を判定する（完全一致でなくてよい）。
EXPECTED = {
    "race_id":     ["race_id", "raceid", "レースid", "レースID"],
    "horse_id":    ["horse_id", "horseid", "馬id", "馬ID", "uma_id"],
    "jockey_id":   ["jockey_id", "jockeyid", "騎手id"],
    "trainer_id":  ["trainer_id", "trainerid", "調教師id"],
    "owner_id":    ["owner_id", "ownerid", "馬主id"],
    "着順":        ["着順", "rank", "order", "finish", "finishing_position", "result"],
    "枠番":        ["枠番", "waku", "wakuban", "bracket", "post"],
    "馬番":        ["馬番", "umaban", "horse_number", "number", "gate"],
    "馬名":        ["馬名", "horse_name", "name", "horse"],
    "性齢":        ["性齢", "sex_age", "sexage", "sex", "age"],
    "斤量":        ["斤量", "kinryo", "weight_carried", "impost", "burden"],
    "騎手":        ["騎手", "jockey", "rider"],
    "タイム":      ["タイム", "time", "finish_time", "race_time"],
    "着差":        ["着差", "margin", "rank_diff", "diff"],
    "単勝":        ["単勝", "odds", "win_odds", "tansho", "tan"],
    "人気":        ["人気", "popularity", "favorite", "ninki", "fav"],
    "馬体重":      ["馬体重", "weight", "horse_weight", "body_weight"],
    "調教師":      ["調教師", "trainer"],
    "date":        ["date", "日付", "年月日", "開催日", "race_date"],
}
# seed の可否を決める必須キー（結合に不可欠）。
JOIN_KEYS = ["race_id", "horse_id"]
# あると person 特徴・Elo が効く（無くても results 自体は seed 可）。
PERSON_KEYS = ["jockey_id", "trainer_id", "owner_id"]


def _read_csv_smart(path: str, nrows: int | None = None) -> tuple[pd.DataFrame, str]:
    """日本語 CSV を複数エンコーディングで試し読みする。成功した (df, encoding) を返す。"""
    encodings = ["utf-8-sig", "utf-8", "cp932", "shift_jis", "euc-jp"]
    last_err: Exception | None = None
    for enc in encodings:
        try:
            df = pd.read_csv(path, nrows=nrows, encoding=enc, low_memory=False)
            return df, enc
        except Exception as e:  # noqa: BLE001 — 次のエンコーディングを試す
            last_err = e
    raise RuntimeError(f"どのエンコーディングでも読めませんでした: {last_err}")


def _count_rows(path: str, encoding: str) -> int:
    """ヘッダを除く行数を軽量にカウント（全体を pandas に載せない）。"""
    try:
        with open(path, encoding=encoding, errors="replace") as f:
            return max(sum(1 for _ in f) - 1, 0)
    except Exception:  # noqa: BLE001
        return -1


def _match(cols_lower: dict, aliases: list[str]) -> str | None:
    """CSV 列（小文字化）に別名のいずれかが一致 or 部分一致すれば実列名を返す。"""
    for a in aliases:
        al = a.lower()
        if al in cols_lower:
            return cols_lower[al]
    # 部分一致（例: 'finishing_position' に 'position'）は誤検出を避け、完全一致のみ採用。
    return None


def run(path: str) -> int:
    if not os.path.isfile(path):
        print(f"[NG] ファイルが見つかりません: {path}")
        print("     WSL から Windows のファイルは /mnt/c/... で参照してください。")
        return 2

    size_mb = os.path.getsize(path) / 1e6
    sample, enc = _read_csv_smart(path, nrows=5000)
    total = _count_rows(path, enc)

    print("=" * 78)
    print(f"CSV 分析: {os.path.basename(path)}")
    print(f"  サイズ={size_mb:,.1f}MB  エンコーディング={enc}  "
          f"総行数={total:,}（推定）  列数={sample.shape[1]}")
    print("=" * 78)

    # --- 列一覧・dtype・欠損率（サンプル 5000 行ベース） ---
    print("列・dtype・欠損率（先頭 5000 行）:")
    for c in sample.columns:
        na = sample[c].isna().mean() * 100
        print(f"    {str(c):<28} {str(sample[c].dtype):<10} 欠損={na:5.1f}%")

    # --- サンプル行 ---
    print("\nサンプル 3 行:")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(sample.head(3).to_string())

    # --- 期間（date 列 or ファイル名から推定） ---
    cols_lower = {str(c).lower(): c for c in sample.columns}
    date_col = _match(cols_lower, EXPECTED["date"])
    if date_col is not None:
        try:
            full = _read_csv_smart(path)[0] if total <= 2_000_000 else sample
            d = pd.to_datetime(full[date_col], errors="coerce")
            print(f"\n期間（{date_col}）: {d.min()} 〜 {d.max()}  "
                  f"（{'全体' if full is not sample else 'サンプル'}）")
        except Exception as e:  # noqa: BLE001
            print(f"\n期間: date 列の解析に失敗: {e}")
    else:
        print("\n期間: date 列を検出できず（ファイル名から 1986-2021 の可能性）。")

    # --- 互換性チェック ---
    print("\n" + "-" * 78)
    print("keibam raw_results との互換性:")
    found: dict[str, str] = {}
    missing: list[str] = []
    for canon, aliases in EXPECTED.items():
        hit = _match(cols_lower, aliases)
        if hit is not None:
            found[canon] = hit
        else:
            missing.append(canon)

    def _show(keys):
        for k in keys:
            if k in found:
                print(f"    ✓ {k:<12} ← CSV列 '{found[k]}'")
            else:
                print(f"    ✗ {k:<12} （見当たらず）")

    print("  [結合キー（seed に必須）]")
    _show(JOIN_KEYS)
    print("  [person 特徴（あると望ましい）]")
    _show(PERSON_KEYS)
    print("  [結果列]")
    _show([k for k in EXPECTED if k not in JOIN_KEYS + PERSON_KEYS + ["date"]])

    # --- 判定 ---
    print("\n" + "=" * 78)
    has_race = "race_id" in found
    has_horse = "horse_id" in found
    has_name = "馬名" in found
    core_result = sum(k in found for k in ("着順", "馬番", "単勝", "人気"))
    print("判定:")
    if has_race and has_horse and core_result >= 3:
        print("  ✅ そのまま seed 可能性が高い（race_id + horse_id + 主要結果列あり）。")
        print("     → 列名を keibam 正準名にリネームして raw_results.pkl へマージできる。")
        n_person = sum(k in found for k in PERSON_KEYS)
        print(f"     person ID {n_person}/3 検出。horse_id があるので horse_results/peds は")
        print("     別途取得（backfill）すれば form/血統も後付け可能。")
    elif has_name and not has_horse:
        print("  ⚠ 馬名はあるが horse_id（netkeiba 数値ID）が無い。")
        print("     → このままでは horse_results/peds/person特徴/form と結合できない。")
        print("     名前→horse_id の解決（同名・改名・地方馬で曖昧）が必要。results 単体の")
        print("     市場・着順統計には使えるが、keibam の主要特徴には直結しない。")
    else:
        print("  ⚠ 結合キーが不足。上の列一覧を見て、race_id/horse_id 相当の列があるか確認要。")
    if missing:
        print(f"\n  未検出の期待列: {missing}")
    print("=" * 78)
    print("次のアクション: この出力を貼ってください。列名の対応を確定し、seed 用の")
    print("変換（リネーム→raw_results へマージ）スクリプトを作るか、ID解決の要否を判断します。")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="外部レース結果 CSV の keibam 互換性分析")
    ap.add_argument("path", help="CSV パス（WSL からは /mnt/c/... 形式）")
    return run(ap.parse_args().path)


if __name__ == "__main__":
    sys.exit(main())
