def scrape_race_id_list(kaisai_date_list=None):
    """開催日リストからレースIDリストを取得して返す。

    Playwright でスクレイピングし、pkl に保存後、DataFrame を返す。
    kaisai_date_list: scrape_kaisai_date() の戻り値（使用しない、内部で pkl を参照）
    """
    import os
    import pandas as pd
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    rl = url_paths.RACE_LIST_URL
    loader = KaisaiDateLoader(
        alias=rl[0],
        from_location=rl[1],
        to_temp_location=rl[2],
        temp_save_file_name=rl[3],
        to_location=rl[4],
        save_file_name=rl[5],
        batch_size=rl[6],
        from_local_location=rl[7],
        from_local_file_name=rl[8],
    )
    loader.scrape_race_id_list()
    save_path = os.path.join(rl[4], rl[5])
    return pd.read_pickle(save_path)
