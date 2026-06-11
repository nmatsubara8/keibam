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

    def _date_column(df):
        """8桁日付（YYYYMMDD）を多く含む列を日付列として特定する。

        CSV 往復で末尾に "Unnamed: 0"（行番号）が付くと columns[-1] では
        取り違えるため、列名ではなく内容で判定する。
        """
        best_col, best_ratio = None, 0.0
        for col in df.columns:
            if str(col).startswith("Unnamed"):
                continue
            vals = df[col].astype(str).str.replace(r"\.0$", "", regex=True)
            ratio = vals.str.match(r"^\d{8}$").mean()
            if ratio > best_ratio:
                best_col, best_ratio = col, ratio
        # 8桁日付らしい列が見つからなければ "kaisai_data" 名、無ければ最終列
        if best_col is not None and best_ratio > 0.5:
            return best_col
        if "kaisai_data" in df.columns:
            return "kaisai_data"
        return df.columns[-1]

    def _filter_range(df):
        """from_date/to_date の範囲内の日付だけに絞る（他年のキャッシュ混入を防ぐ）。"""
        if df is None or df.empty:
            return df
        date_col = _date_column(df)
        dates = df[date_col].astype(str).str.replace(r"\.0$", "", regex=True)
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
