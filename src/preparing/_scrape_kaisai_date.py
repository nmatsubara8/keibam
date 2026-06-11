def scrape_kaisai_date(from_date: str = None, to_date: str = None, skip: bool = False,
                       from_: str = None, to_: str = None):
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

    from_8 = from_date.replace("-", "")[:8]
    to_8 = to_date.replace("-", "")[:8]

    def _filter_range(df):
        """from_date/to_date の範囲内の日付だけに絞る（他年のキャッシュ混入を防ぐ）。"""
        if df is None or df.empty:
            return df
        date_col = df.columns[-1]
        dates = df[date_col].astype(str)
        mask = (dates >= from_8) & (dates < to_8)
        return df[mask]

    if skip and os.path.exists(save_path):
        cached = pd.read_pickle(save_path)
        filtered = _filter_range(cached)
        if filtered is not None and not filtered.empty:
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
    # スクレイプ後も全件 pkl から要求範囲だけ返す（過去年キャッシュの混入を防ぐ）
    return _filter_range(pd.read_pickle(save_path))
