"""馬ページ HTML のスクレイピング（差分ダウンロード対応）。

scrape_html_horse_with_master は horse_id_list を受け取り、
既存 bin を持つ horse_id をスキップして未取得分だけダウンロードする。
"""

import logging
import os
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def scrape_html_horse_with_master(horse_id_list, skip: bool = True):
    """horse_id_list の馬ページ HTML を scrape して bin ファイルに保存する。

    Parameters
    ----------
    horse_id_list : array-like
        results_new['horse_id'].unique() 等で得た horse_id の配列。
    skip : bool
        True の場合、既存 bin がある horse_id をスキップ（差分ダウンロード）。

    Returns
    -------
    list[Path]
        保存済み bin ファイルのパスリスト（ソート済み）。
    """
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    hh = url_paths.HORSE_HTML

    save_dir = Path(hh[4])
    save_dir.mkdir(parents=True, exist_ok=True)

    # 未取得 horse_id を絞り込む
    ids = [str(h) for h in horse_id_list] if horse_id_list is not None else []
    if skip and ids:
        existing = {p.stem for p in save_dir.glob("*.bin")}
        to_scrape_ids = [h for h in ids if h not in existing]
        if not to_scrape_ids:
            logger.info("scrape_html_horse: 全 horse_id 取得済みのためスキップ (%d 件)", len(ids))
            return sorted(save_dir.glob("*.bin"))
        logger.info(
            "scrape_html_horse: %d 件中 %d 件取得済み → %d 件をダウンロード",
            len(ids), len(ids) - len(to_scrape_ids), len(to_scrape_ids),
        )
    else:
        to_scrape_ids = ids

    # 入力 pkl として保存（ローダーが参照する）
    input_dir = hh[7]
    input_file = hh[8]
    if to_scrape_ids:
        os.makedirs(input_dir, exist_ok=True)
        df_ids = pd.DataFrame({"horse_id": to_scrape_ids})
        df_ids.to_pickle(os.path.join(input_dir, input_file))

    loader = KaisaiDateLoader(
        alias=hh[0],
        from_location=hh[1],
        to_temp_location=hh[2],
        temp_save_file_name=hh[3],
        to_location=hh[4],
        save_file_name=hh[5],
        batch_size=hh[6],
        from_local_location=input_dir,
        from_local_file_name=input_file,
    )
    loader.scrape_html_horse()

    return sorted(save_dir.glob("*.bin"))
