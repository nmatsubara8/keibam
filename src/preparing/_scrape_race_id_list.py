def scrape_race_id_list(kaisai_date_list=None):
    """開催日リストからレースIDリストを取得して返す。

    Playwright でスクレイピングし、pkl に保存後、DataFrame を返す。
    kaisai_date_list: scrape_kaisai_date() の戻り値。指定するとそのデータを
    入力 pkl として保存してからスクレイピングするため、年が混在しない。
    """
    import os
    import pandas as pd
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    rl = url_paths.RACE_LIST_URL

    # 呼び出し元が kaisai_date_list を渡した場合、それを入力 pkl として保存する。
    # これにより、ディスク上の古い pkl（別年のデータ混在）による誤スクレイピングを防ぐ。
    if kaisai_date_list is not None:
        input_pkl_path = os.path.join(rl[7], rl[8])
        os.makedirs(rl[7], exist_ok=True)
        kaisai_date_list.to_pickle(input_pkl_path)

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
    df = pd.read_pickle(save_path)

    # kaisai_date_list が指定されている場合、その年集合に含まれない race_id を除外する。
    # scraping 中のフォールバック応答や古い pkl の残骸が混入しても確実に取り除く。
    if kaisai_date_list is not None:
        valid_years = set(
            kaisai_date_list.iloc[:, -1].astype(str).str[:4].unique()
        )
        race_id_col = df.columns[-1]
        mask = df[race_id_col].astype(str).str[:4].isin(valid_years)
        df = df[mask].reset_index(drop=True)
        # ディスク上の pkl も更新する（後続スクレイパーが正しいリストを読む）
        df.to_pickle(save_path)

    return df
