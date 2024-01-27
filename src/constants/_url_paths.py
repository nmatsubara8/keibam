import dataclasses


@dataclasses.dataclass(frozen=True)
class UrlPaths:
    DB_DOMAIN: tuple = ("kaisai_date_list", "https://db.netkeiba.com/")
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RACE_URL: tuple = ("race_list", DB_DOMAIN[1] + "race/")
    # 馬の過去成績テーブルが含まれるページ
    HORSE_URL: tuple = ("horse_list", DB_DOMAIN[1] + "horse/")
    # 血統テーブルが含まれるページ
    PED_URL: tuple = ("ped_list", HORSE_URL[1] + "ped/")

    TOP_URL: tuple = ("top_page", "https://race.netkeiba.com/top/")
    # 開催日程ページ
    CALENDAR_URL: tuple = ("horse_result_list", TOP_URL[1] + "calendar.html")
    # レース一覧ページ
    RACE_LIST_URL: tuple = ("race_result_list", TOP_URL[1] + "race_list.html")

    # 出馬表ページ
    SHUTUBA_TABLE: tuple = ("shutuba_table", "https://race.netkeiba.com/race/shutuba.html")
