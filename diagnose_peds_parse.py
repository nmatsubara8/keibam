"""血統(ped) bin が 6,213件あるのに 357頭しかパースできない原因を切り分ける。

recover_peds_from_html_cache.py --execute は 6,213 件の bin を解析したが、
成功は 357 頭のみ（94%が例外で黙殺）。「データ欠損」ではなく「パーサが読めない」
可能性が高い（脚質バグ・race_idバグと同型）。本スクリプトは故障 bin の中身を
分類し、原因（空DL/エラーページ/別フォーマット/文字コード/別セレクタ）を特定する。

netkeiba へは一切アクセスしない（保存済み bin の読取のみ）。

実行: python diagnose_peds_parse.py [--sample 400]
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import re
from collections import Counter

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

PED_HTML_DIR = LocalPaths.HTML_PED_DIR


def _try_parse(path: str) -> tuple[bool, str]:
    """create_raw_horse_ped で実際にパースを試み、(成功?, 失敗理由) を返す。"""
    import src.preparing.modules as _m

    try:
        df = _m.create_raw_horse_ped(path)
    except ValueError as e:
        return False, f"ValueError: {str(e)[:40]}"
    except IndexError as e:
        return False, f"IndexError: {str(e)[:40]}"
    except Exception as e:  # noqa: BLE001
        return False, f"{type(e).__name__}: {str(e)[:40]}"
    if df is None or getattr(df, "empty", True):
        return False, "empty-df（テーブル有でも行0）"
    return True, "ok"


def _inspect_bytes(html: bytes) -> dict:
    """生バイトから手掛かりを抽出（BeautifulSoup 非依存の軽量チェック）。"""
    info: dict = {"size": len(html)}
    txt = html.decode("utf-8", errors="replace")
    low = txt.lower()
    info["has_5代血統表"] = "5代血統表" in txt
    info["has_blood_table"] = "blood_table" in low
    info["has_血統"] = "血統" in txt
    # エラーページ/リダイレクトの典型語
    info["looks_error"] = any(
        k in txt for k in ("ご指定のページ", "見つかりません", "404 Not Found", "404 not found")
    )
    # charset メタ（EUC-JP なら utf-8 decode で和文が化ける）
    m = re.search(r'charset=["\']?([\w-]+)', low)
    info["charset"] = m.group(1) if m else "?"
    # <title>
    mt = re.search(r"<title>(.*?)</title>", txt, re.IGNORECASE | re.DOTALL)
    info["title"] = (mt.group(1).strip()[:40] if mt else "(no title)")
    # table の class/summary の出現（どんな表が入っているか）
    info["table_classes"] = re.findall(r'<table[^>]*class=["\']([^"\']+)["\']', low)[:3]
    return info


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="ped bin がパースできない原因を分類")
    ap.add_argument("--sample", type=int, default=400, help="抽出して詳細検査する bin 数")
    args = ap.parse_args()

    bins = sorted(glob.glob(os.path.join(PED_HTML_DIR, "*.bin")))
    print("=" * 72)
    print(f"ped bin パース診断 — 全 {len(bins)} 件 / data/html/ped")
    print("=" * 72)
    if not bins:
        print("bin が 1 件もありません。")
        return

    # 1) 全件: パース成功/失敗の理由を集計（軽い。create_raw_horse_ped を全件試す）
    reason_counter: Counter = Counter()
    ok_ids: set[str] = set()
    fail_paths: list[str] = []
    from tqdm.auto import tqdm

    for p in tqdm(bins, desc="パース試行", unit="件"):
        ok, reason = _try_parse(p)
        if ok:
            ok_ids.add(re.findall(r"\d+", os.path.basename(p))[0])
        else:
            reason_counter[reason.split(":")[0]] += 1
            fail_paths.append(p)

    print(f"\n■ パース結果: 成功 {len(ok_ids)} / 失敗 {len(fail_paths)} （全 {len(bins)}）")
    print("■ 失敗理由の内訳（上位）")
    for reason, n in reason_counter.most_common(8):
        print(f"    {reason:<32} {n:>6} 件")

    # 2) 失敗 bin をサンプリングして中身を分類（原因の特定）
    sample = fail_paths[: args.sample]
    print(f"\n■ 失敗 bin のサンプル {len(sample)} 件を中身検査")
    agg = {
        "has_5代血統表": 0, "has_blood_table": 0, "has_血統": 0,
        "looks_error": 0, "tiny(<2KB)": 0,
    }
    charset_c: Counter = Counter()
    title_c: Counter = Counter()
    tclass_c: Counter = Counter()
    for p in sample:
        with open(p, "rb") as f:
            html = f.read()
        info = _inspect_bytes(html)
        for k in ("has_5代血統表", "has_blood_table", "has_血統", "looks_error"):
            agg[k] += int(info[k])
        if info["size"] < 2048:
            agg["tiny(<2KB)"] += 1
        charset_c[info["charset"]] += 1
        title_c[info["title"]] += 1
        for tc in info["table_classes"]:
            tclass_c[tc] += 1

    n = max(1, len(sample))
    print(f"    血統テーブル文字列 '5代血統表' を含む : {agg['has_5代血統表']:>5} / {n}")
    print(f"    'blood_table' クラスを含む          : {agg['has_blood_table']:>5} / {n}")
    print(f"    どこかに '血統' を含む               : {agg['has_血統']:>5} / {n}")
    print(f"    エラーページらしい                   : {agg['looks_error']:>5} / {n}")
    print(f"    2KB 未満（空DL/途中切れ疑い）        : {agg['tiny(<2KB)']:>5} / {n}")
    print("\n  charset メタ:")
    for cs, c in charset_c.most_common(5):
        print(f"    {cs:<12} {c}")
    print("  <title> 上位:")
    for t, c in title_c.most_common(5):
        print(f"    {c:>4}  {t}")
    print("  table class 上位（パーサが探すべき実際のクラス名）:")
    for tc, c in tclass_c.most_common(8):
        print(f"    {c:>4}  {tc}")

    print("\n判定の目安:")
    print("  - '5代血統表'も'血統'も無い/エラーページ多数/2KB未満多数 → 空DL・要再DL")
    print("  - '血統'は有るが '5代血統表'/'blood_table' が別クラス名 → セレクタ修正で復旧")
    print("  - charset が euc-jp 主体 → utf-8 decode 化けが原因（decode 修正で復旧）")
    print("=" * 72)


if __name__ == "__main__":
    main()
