"""NAR(地方)レースのデータ項目 取得可能性を1レース単位で検証する（多量取得の前段）。

中央用パーサ（db.netkeiba.com 前提）が NAR の結果ページでも「結果 / レース情報 / 払戻」の
各データ項目を取り出せるかを、**1レースだけ**取得して確認する。pkl は一切更新しない・
馬/血統/featured には触らない（安全）。多量件数を対象にする前の疎通確認用。

パーサは summary="レース結果" 等のテーブル属性に依存するため、NAR で DOM が異なると
IndexError 等で失敗する。その場合は例外内容が DOM 差の手掛かりになる。

実行:
  python verify_nar_scrape.py --race-id 202630072101            # 取得して検証
  python verify_nar_scrape.py --race-id 202630072101 --no-fetch # 既存 bin だけで検証
  python verify_nar_scrape.py --race-id 202630072101 202630072102
"""

from __future__ import annotations

import argparse
import logging
import os

logger = logging.getLogger(__name__)

_BIN_DIR = os.path.join("data", "html", "race")


def _bin_path(race_id: str) -> str:
    return os.path.join(_BIN_DIR, f"{race_id}.bin")


def _fetch_bin(race_id: str) -> None:
    import pandas as pd

    from src.preparing._scrape_html_race import scrape_html_race

    scrape_html_race(pd.DataFrame({"race_id": [race_id]}), skip=True)


def _describe(name: str, df) -> None:
    import pandas as pd

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        print(f"  ✗ {name}: 空（データ項目を取得できず）")
        return
    cols = list(df.columns)
    print(f"  ✓ {name}: {len(df)} 行 / {len(cols)} 列")
    print(f"      列: {cols}")
    # 先頭行を key:value で（長すぎる値は truncate）
    head = df.iloc[0].to_dict()
    shown = {k: (str(v)[:40]) for k, v in list(head.items())[:14]}
    print(f"      例(先頭行): {shown}")


def _verify_one(race_id: str, no_fetch: bool) -> None:
    from src.constants._model_category import organizer_of_race_id
    from src.preparing._raw_parsers import (
        create_raw_race_info,
        create_raw_race_results,
        create_raw_race_return,
    )

    org = organizer_of_race_id(race_id)
    print("=" * 74)
    print(f"race_id={race_id}  主催者={org}（{'中央/JRA' if org == 'central' else '地方/NAR'}）")
    print("=" * 74)

    bin_path = _bin_path(race_id)
    if not os.path.exists(bin_path) and not no_fetch:
        print("  … bin 未取得のため db.netkeiba.com から取得します（ポライトネス制御あり）")
        try:
            _fetch_bin(race_id)
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ 取得失敗: {e}")
            return
    if not os.path.exists(bin_path):
        print(f"  ✗ bin がありません: {bin_path}（--no-fetch を外す/確定済みレースか確認）")
        return
    print(f"  bin: {bin_path}（{os.path.getsize(bin_path):,} bytes）")

    for name, fn in (
        ("results（結果）", create_raw_race_results),
        ("race_info（レース情報）", create_raw_race_info),
        ("return（払戻）", create_raw_race_return),
    ):
        try:
            _describe(name, fn(bin_path))
        except Exception as e:  # noqa: BLE001
            print(f"  ✗ {name}: パース失敗 → {type(e).__name__}: {e}")
            print("      ↑ NAR で DOM が異なる可能性。bin の該当テーブル構造を確認して調整する。")


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="NAR レースのデータ項目 取得可能性を検証（1レース単位・非破壊）")
    ap.add_argument("--race-id", nargs="+", required=True, help="検証する race_id（複数可）")
    ap.add_argument("--no-fetch", action="store_true", help="ネット取得せず既存 bin だけで検証")
    args = ap.parse_args()

    for rid in args.race_id:
        _verify_one(str(rid), args.no_fetch)

    print("\n" + "=" * 74)
    print("読み方: 3項目すべて ✓ なら NAR 履歴系は転用完了（多量取得へ進んでよい）。")
    print("✗ が出た項目は NAR の DOM 差が原因。例外と bin を見てパーサを調整してから bulk へ。")
    print("=" * 74)


if __name__ == "__main__":
    main()
