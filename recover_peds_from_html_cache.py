"""血統(peds)を HTML キャッシュ(data/html/ped)から再生成する。

featured の「血統/種牡馬」特徴量が重要度 0.00% なのは、信号が弱いのではなく
peds.pkl が極少数（〜357頭）しか無いデータ欠損が原因。本スクリプトは
data/html/ped に保存済みの血統ページ HTML(bin) を再パースして peds.pkl / DB(raw_peds)
を作り直す。netkeiba へは一切アクセスしない（キャッシュ再処理のみ）。

ただし peds は race_id 破損とは性質が違う:「パースの取りこぼし」ではなく
「そもそも血統ページを未ダウンロードの馬が多い」可能性がある。よって本スクリプトは
まず現状を調査し、

  - 必要な horse_id（results / horse_results に出走する全馬）
  - そのうち ped HTML(bin) が手元にある馬（= 再パースで今すぐ復旧可能）
  - bin が無い馬（= netkeiba から血統ページのダウンロードが必要）

を切り分ける。bin が有る分は --execute で即復旧し、bin が無い馬の horse_id は
ファイルに書き出す（後で scrape_html_ped でポライトネス制御つきDLするための入力）。

使い方:
  python recover_peds_from_html_cache.py            # 調査のみ（dry-run、非破壊）
  python recover_peds_from_html_cache.py --execute  # 退避→再パース→peds.pkl/DB 再構築
  python recover_peds_from_html_cache.py --execute --workers 16
"""

from __future__ import annotations

import argparse
import glob
import logging
import os
import shutil

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

PED_HTML_DIR = LocalPaths.HTML_PED_DIR  # data/html/ped
MISSING_IDS_PATH = os.path.join("data", "tmp", "peds_missing_horse_ids.txt")


# ---------------------------------------------------------------------------
# horse_id ユニバースの収集（results / horse_results に出走する全馬）
# ---------------------------------------------------------------------------

def _ids_from_frame(df: pd.DataFrame) -> set[str]:
    """DataFrame の horse_id（列 or index）を文字列集合で返す。"""
    if df is None or df.empty:
        return set()
    if "horse_id" in df.columns:
        s = df["horse_id"]
    elif df.index.name == "horse_id":
        s = df.index.to_series()
    else:
        return set()
    s = s.dropna().astype(str).str.replace(r"\.0$", "", regex=True)
    return set(s)


def _needed_horse_ids() -> set[str]:
    """学習・予測に必要な horse_id 全体（results ∪ horse_results）。"""
    ids: set[str] = set()
    for path in (LocalPaths.RAW_RESULTS_PATH, LocalPaths.RAW_HORSE_RESULTS_PATH):
        if os.path.exists(path):
            try:
                ids |= _ids_from_frame(pd.read_pickle(path))
            except Exception as e:  # noqa: BLE001
                logger.warning("[peds] %s 読込失敗: %s", path, e)
    return ids


def _ped_bin_stems() -> set[str]:
    """data/html/ped/*.bin のファイル名（=horse_id）集合。"""
    if not os.path.isdir(PED_HTML_DIR):
        return set()
    return {os.path.splitext(os.path.basename(p))[0] for p in glob.glob(os.path.join(PED_HTML_DIR, "*.bin"))}


def _peds_pkl_ids() -> set[str]:
    if not os.path.exists(LocalPaths.RAW_PEDS_PATH):
        return set()
    try:
        return _ids_from_frame(pd.read_pickle(LocalPaths.RAW_PEDS_PATH))
    except Exception as e:  # noqa: BLE001
        logger.warning("[peds] peds.pkl 読込失敗: %s", e)
        return set()


def _investigate() -> None:
    print("=" * 72)
    print("血統(peds) 復旧 — 現状調査（dry-run、非破壊）")
    print("=" * 72)

    needed = _needed_horse_ids()
    bins = _ped_bin_stems()
    have = _peds_pkl_ids()

    needed_with_bin = needed & bins
    needed_without_bin = needed - bins

    print(f"\n■ 必要 horse_id（results ∪ horse_results）   {len(needed):>8} 頭")
    print(f"■ 血統HTML(bin) 手元にある馬 (data/html/ped)  {len(bins):>8} 件")
    print(f"■ 現 peds.pkl に入っている馬                  {len(have):>8} 頭")
    print("\n── 切り分け ──")
    print(f"  ① 必要 かつ bin 有り（再パースで即復旧可）   {len(needed_with_bin):>8} 頭")
    print(f"  ② 必要 だが bin 無し（netkeiba DL が必要）    {len(needed_without_bin):>8} 頭")
    cov = (len(needed_with_bin) / len(needed) * 100) if needed else 0.0
    print(f"\n  → 再パースだけで賄える網羅率: {cov:5.1f}%（①/必要）")

    print("\n判定:")
    if len(bins) > len(have) * 1.5 and len(needed_with_bin) > len(have):
        print(f"  bin({len(bins)}) ≫ peds.pkl({len(have)}) → パース/ビルドの取りこぼし。")
        print("  --execute で今すぐ大幅に回復します（ネット非アクセス）。")
    elif len(needed_without_bin) > len(needed_with_bin):
        print("  bin が無い馬が多数 → 血統ページ自体が未ダウンロード（データ欠損）。")
        print("  --execute で bin 有り分を復旧後、不足 horse_id を書き出します。")
        print("  その後 scrape_html_ped でDL（ポライトネス制御あり）→ 再度 --execute。")
    else:
        print("  --execute で bin 有り分を復旧。残りは書き出した不足リストでDL検討。")
    print(f"\n  不足 horse_id の書き出し先: {MISSING_IDS_PATH}（--execute 時）")
    print("=" * 72)


# ---------------------------------------------------------------------------
# 並列再パース
# ---------------------------------------------------------------------------

def _parse_one_ped(path: str):
    """1つの ped bin → DataFrame(horse_id index, peds_*) or None。"""
    import src.preparing.modules as _m

    try:
        df = _m.create_raw_horse_ped(path)
    except (ValueError, IndexError):
        return None
    except Exception:  # noqa: BLE001 — 壊れた/旧式 HTML は欠損として無視
        return None
    if df is None or getattr(df, "empty", True):
        return None
    # horse_id を必ず列へ正規化（concat(ignore_index=True) で失わないため）。
    if "horse_id" not in df.columns and df.index.name == "horse_id":
        df = df.reset_index()
    return df


def _backup(path: str) -> None:
    if os.path.exists(path) and not os.path.exists(path + ".bak"):
        shutil.copy2(path, path + ".bak")
        logger.info("[peds] 退避: %s → %s", path, path + ".bak")


def _execute(workers: int = 0) -> None:
    import multiprocessing as mp

    from tqdm.auto import tqdm

    from src.pipeline._ingestion import save_raw
    from src.storage import RawDataRepo

    bins = sorted(glob.glob(os.path.join(PED_HTML_DIR, "*.bin")))
    if not bins:
        logger.error("[peds] 血統HTMLキャッシュ(%s)が空です。復旧できません", PED_HTML_DIR)
        return

    if workers <= 0:
        workers = max(1, os.cpu_count() or 4)

    _backup(LocalPaths.RAW_PEDS_PATH)
    if os.path.exists(LocalPaths.DB_PATH):
        _backup(LocalPaths.DB_PATH)

    logger.info(
        "💾 ローカル並列解析: peds を %d 件のローカル HTML から %d 並列で作成します"
        "（ネットワーク非アクセス＝ポライトネス対象外）", len(bins), workers,
    )
    frames: list = []
    if workers > 1:
        with mp.Pool(processes=workers) as pool:
            for df in tqdm(
                pool.imap_unordered(_parse_one_ped, bins, chunksize=64),
                total=len(bins), desc=f"⚡ peds 並列解析({workers}並列)", unit="件", leave=True,
            ):
                if df is not None:
                    frames.append(df)
    else:
        for p in tqdm(bins, desc="💾 peds 解析", unit="件", leave=True):
            df = _parse_one_ped(p)
            if df is not None:
                frames.append(df)

    full = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if full.empty:
        logger.error("[peds] 解析成功 0 件。peds.pkl は更新しません")
        return
    # 正本形式（RangeIndex + horse_id 列）に揃える。重複 horse_id は最後を採用。
    full = full.drop_duplicates(subset="horse_id", keep="last").reset_index(drop=True)
    logger.info("[peds] 再生成: 行=%d 馬=%d 列=%d", len(full), full["horse_id"].nunique(), full.shape[1])

    # DB を作り直してから clean pickle を投入（save_raw が pickle 保存 + DB upsert）。
    RawDataRepo().clear("raw_peds")
    save_raw(full, LocalPaths.RAW_PEDS_PATH)
    logger.info("[peds] peds.pkl / DB(raw_peds) を作り直しました")

    # 不足（必要だが bin 無し）horse_id を書き出す（後でDLする入力）。
    needed = _needed_horse_ids()
    recovered = _ids_from_frame(full)
    missing = sorted(needed - recovered)
    os.makedirs(os.path.dirname(MISSING_IDS_PATH), exist_ok=True)
    with open(MISSING_IDS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(missing))

    cov = (len(recovered & needed) / len(needed) * 100) if needed else 0.0
    print("\n" + "=" * 72)
    print(f"peds 復旧完了: {len(recovered)} 頭（必要 {len(needed)} 頭の網羅率 {cov:.1f}%）")
    print(f"不足 {len(missing)} 頭の horse_id を書き出し: {MISSING_IDS_PATH}")
    if missing:
        print("不足分をDLするには（ポライトネス制御あり・netkeiba アクセス）:")
        print("  ids = open('%s').read().split()" % MISSING_IDS_PATH)
        print("  from src.preparing._scrape_html_ped import scrape_html_ped")
        print("  scrape_html_ped(ids)  # → 再度 python recover_peds_from_html_cache.py --execute")
    print("次: rebuild-featured → feature_importance.py で 血統/種牡馬 のシェアを再確認")
    print("=" * 72)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="血統(peds)を HTML キャッシュから再生成")
    ap.add_argument("--execute", action="store_true", help="実際に再構築する（既定は調査のみ）")
    ap.add_argument(
        "--workers", type=int, default=0,
        help="並列ワーカー数（0=自動: CPU論理数、1=直列フォールバック）",
    )
    args = ap.parse_args()
    if args.execute:
        _execute(workers=args.workers)
    else:
        _investigate()


if __name__ == "__main__":
    main()
