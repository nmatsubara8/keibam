def scrape_kaisai_date(from_date: str, to_date: str) -> int:
    """Scrape kaisai date list for the given date range using Selenium.

    Requires selenium and network access; intended for live data collection only.
    Returns 0 on success.
    """
    from src.constants._url_paths import UrlPaths
    from src.preparing.url_loader import KaisaiDateLoader

    url_paths = UrlPaths()
    cal = url_paths.CALENDAR_URL
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
    return 0
