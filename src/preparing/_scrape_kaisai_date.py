def scrape_kaisai_date(from_date: str | None = None, to_date: str | None = None, skip: bool = False,
                       from_: str | None = None, to_: str | None = None):
    """指定期間の開催日リストを取得して返す。

    Playwright でスクレイピングし、pkl に保存後、DataFrame を返す。

    Parameters
    ----------
    from_date, to_date : str
        期間指定（例: "2020-01-01", "2021-01-01"）。
        後方互換のため from_ / to_ という別名でも指定できる。
    skip : bool
        True の場合、既存の pkl がある場合はスクレイピングを省略して返す。
    """
    # 後方互換: from_ / to_ エイリアスを受け付ける
    from_date = from_date if from_date is not None else from_
    to_date = to_date if to_date is not None else to_
    if from_date is None or to_date is None:
        raise TypeError("scrape_kaisai_date() は from_date と to_date が必要です")

    import os
    import pandas as pd
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    cal = url_paths.CALENDAR_URL
    save_path = os.path.join(cal[4], cal[5])

    if skip and os.path.exists(save_path):
        cached = pd.read_pickle(save_path)
        # from_date/to_date の範囲内の日付だけ返す（他年のキャッシュを混入させない）
        date_col = cached.columns[-1]
        from_8 = from_date.replace("-", "")[:8]
        to_8 = to_date.replace("-", "")[:8]
        mask = (cached[date_col].astype(str) >= from_8) & (cached[date_col].astype(str) < to_8)
        filtered = cached[mask]
        if not filtered.empty:
            return filtered
        # キャッシュに該当範囲がなければ再スクレイピング

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
