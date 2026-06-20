"""血統(ped)を対象を絞って netkeiba からDLする（go/no-go 検証用）。

診断の結論: 手元の ped bin 6,213件のうち本物は357件のみ。残り5,856件は空DL(<2KB)の
残骸で、本物カバレッジは 0.4%。血統特徴を評価するには血統ページのDLが必須。ただし
全92,923頭を一気に落とす前に、対象レース(既定2022年以降)の馬だけ落として「血統/種牡馬が
重要度0%から動くか」の go/no-go を取る。

⚠ 落とし穴: scrape_html_ped(skip=True) は bin の存在だけで取得済み判定する。空スタブが
残っていると再DLをスキップしてしまうため、本スクリプトは対象馬の空bin(<min-bytes)を
先に削除してから取得する。

netkeiba へアクセスする処理（--execute 時のみ）。既定は調査のみで非アクセス。

使い方:
  python download_peds_targeted.py                      # 調査のみ（対象頭数/要DL数を表示）
  python download_peds_targeted.py --from-year 2022     # 対象年を変える
  python download_peds_targeted.py --execute --limit 200  # まず200頭だけ試しDL
  python download_peds_targeted.py --execute            # 対象を全DL（数時間規模）
"""

from __future__ import annotations

import argparse
import logging
import os

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

PED_HTML_DIR = LocalPaths.HTML_PED_DIR


def _target_horse_ids(from_year: int) -> set[str]:
    """results.pkl で race_id 年 >= from_year のレースに出走する horse_id 集合。"""
    path = LocalPaths.RAW_RESULTS_PATH
    if not os.path.exists(path):
        logger.error("results.pkl がありません: %s", path)
        return set()
    df = pd.read_pickle(path)
    # race_id は index か列のどちらか
    if df.index.name == "race_id":
        race_id = df.index.to_series().astype(str)
    elif "race_id" in df.columns:
        race_id = df["race_id"].astype(str)
    else:
        logger.error("results.pkl に race_id がありません")
        return set()
    year = pd.to_numeric(race_id.str[:4], errors="coerce")
    mask = (year >= from_year).to_numpy()
    sub = df.loc[mask]
    if "horse_id" not in sub.columns:
        logger.error("results.pkl に horse_id 列がありません")
        return set()
    ids = sub["horse_id"].dropna().astype(str).str.replace(r"\.0$", "", regex=True)
    return set(ids)


def _bin_path(horse_id: str) -> str:
    return os.path.join(PED_HTML_DIR, f"{horse_id}.bin")


def _classify_targets(
    targets: set[str], min_bytes: int
) -> tuple[list[str], list[str], int, list[str]]:
    """対象を (要DL, 既に有効, 空スタブ数, 空スタブid) に分類する。

    有効 = bin が存在し min_bytes 以上。空スタブ(<min_bytes)は要DL扱いで、実行時に削除する。
    """
    need_dl: list[str] = []
    valid: list[str] = []
    empty_stub_ids: list[str] = []
    for hid in targets:
        p = _bin_path(hid)
        if not os.path.exists(p):
            need_dl.append(hid)
        elif os.path.getsize(p) < min_bytes:
            empty_stub_ids.append(hid)  # 空スタブ → 要DL（先に削除）
            need_dl.append(hid)
        else:
            valid.append(hid)
    return need_dl, valid, len(empty_stub_ids), empty_stub_ids


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="対象を絞って血統ページをDL（go/no-go検証）")
    ap.add_argument("--from-year", type=int, default=2022, help="対象レースの開始年（既定2022）")
    ap.add_argument("--min-bytes", type=int, default=2048, help="これ未満のbinは空スタブ扱い")
    ap.add_argument("--limit", type=int, default=0, help="DL頭数の上限（0=無制限、まず小さく試す用）")
    ap.add_argument("--execute", action="store_true", help="実際にDLする（既定は調査のみ・非アクセス）")
    args = ap.parse_args()

    print("=" * 72)
    print(f"血統 対象限定DL — 対象年 {args.from_year} 以降")
    print("=" * 72)

    targets = _target_horse_ids(args.from_year)
    need_dl, valid, n_stub, stub_ids = _classify_targets(targets, args.min_bytes)

    print(f"\n■ 対象馬（{args.from_year}年以降のレース出走）  {len(targets):>7} 頭")
    print(f"■ うち既に有効な血統bin有り                 {len(valid):>7} 頭")
    print(f"■ うち空スタブ(<{args.min_bytes}B、要再DL)    {n_stub:>7} 頭")
    print(f"■ 要DL（bin無し＋空スタブ）                  {len(need_dl):>7} 頭")

    if not args.execute:
        est_min = len(need_dl) * 1.5 / 60.0  # 1.5s/頁 目安
        print(f"\n  概算DL時間（1.5秒/頁）: 約 {est_min:.0f} 分（= {est_min / 60:.1f} 時間）")
        print("  → 実行するなら: python download_peds_targeted.py --execute [--limit 200]")
        print("  （まず --limit 200 で小さく試し、binが本物で埋まるか確認を推奨）")
        print("=" * 72)
        return

    # --- 実行: 空スタブを削除（skip=True の再DL阻害を解消）してからDL ---
    if stub_ids:
        for hid in stub_ids:
            try:
                os.remove(_bin_path(hid))
            except OSError:
                pass
        logger.info("[peds-dl] 空スタブ %d 件を削除（再DLを可能にする）", len(stub_ids))

    dl_ids = need_dl[: args.limit] if args.limit > 0 else need_dl
    if not dl_ids:
        print("\n要DLの馬がいません（対象は全て有効binを保有）。")
        print("=" * 72)
        return

    print(f"\n🌐 netkeiba から {len(dl_ids)} 頭の血統ページをDLします（ポライトネス制御あり）…")
    from src.preparing._scrape_html_ped import scrape_html_ped

    scrape_html_ped(dl_ids, skip=True)

    print("\n" + "=" * 72)
    print(f"DL試行完了: {len(dl_ids)} 頭")
    print("次の手順:")
    print("  1) python diagnose_peds_parse.py        # DL分が本物bin(>2KB)で埋まったか確認")
    print("  2) python recover_peds_from_html_cache.py --execute  # peds.pkl/DB を再構築")
    print("  3) python -m src.pipeline.run_pipeline rebuild-featured")
    print("  4) python feature_importance.py         # 血統/種牡馬 が0%から動くか（go/no-go）")
    print("=" * 72)


if __name__ == "__main__":
    main()
