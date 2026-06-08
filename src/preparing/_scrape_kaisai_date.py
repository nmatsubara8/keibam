def scrape_kaisai_date(from_date: str, to_date: str, skip: bool = False):
    """指定期間の開催日リストを取得して返す。

    Playwright でスクレイピングし、pkl に保存後、DataFrame を返す。

    Parameters
    ----------
    from_date, to_date : str
        期間指定（例: "2020-01-01", "2021-01-01"）。
    skip : bool
        True の場合、既存の pkl がある場合はスクレイピングを省略して返す。
    """
    import os
    import pandas as pd
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    cal = url_paths.CALENDAR_URL
    save_path = os.path.join(cal[4], cal[5])

    if skip and os.path.exists(save_path):
        return pd.read_pickle(save_path)

    loader = KaisaiDateLoader(
        alias=cal[0],
        from_location=cal[1],
        to_temp_location=cal[2],
        temp_save_file_name=cal[3],
        to_location=cal[4],
        save_file_name=cal[5],
        batch_size=cal[6],
        from_date=from_date,
        to_date=to_date,
    )
    loader.scrape_kaisai_date()
    return pd.read_pickle(save_path)
