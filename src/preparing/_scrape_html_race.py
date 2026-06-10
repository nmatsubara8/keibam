def scrape_html_race(race_id_list=None, skip=False):
    """各 race_id の HTML を db.netkeiba.com からスクレイピングして bin ファイルに保存する。

    Parameters
    ----------
    race_id_list : DataFrame, optional
        scrape_race_id_list() の戻り値。指定するとそのデータを入力 pkl として保存する。
    skip : bool
        True の場合、既存の bin ファイルがある race_id をスキップする。

    Returns
    -------
    list[Path]
        保存済み bin ファイルのパスリスト（ソート済み）。
    """
    import os
    from pathlib import Path
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    rh = url_paths.RACE_HTML

    save_dir = Path(rh[4])
    save_dir.mkdir(parents=True, exist_ok=True)

    if race_id_list is not None:
        input_pkl_path = os.path.join(rh[7], rh[8])
        os.makedirs(rh[7], exist_ok=True)

        if skip:
            # 既存 bin ファイルの race_id を除外してから入力 pkl を保存
            existing = {p.stem for p in save_dir.glob("*.bin")}
            race_id_col = race_id_list.columns[-1]
            to_scrape = race_id_list[
                ~race_id_list[race_id_col].astype(str).isin(existing)
            ].reset_index(drop=True)
            if to_scrape.empty:
                return sorted(save_dir.glob("*.bin"))
            to_scrape.to_pickle(input_pkl_path)
        else:
            race_id_list.to_pickle(input_pkl_path)

    loader = KaisaiDateLoader(
        alias=rh[0],
        from_location=rh[1],
        to_temp_location=rh[2],
        temp_save_file_name=rh[3],
        to_location=rh[4],
        save_file_name=rh[5],
        batch_size=rh[6],
        from_local_location=rh[7],
        from_local_file_name=rh[8],
    )
    loader.scrape_html_race()

    return sorted(save_dir.glob("*.bin"))
