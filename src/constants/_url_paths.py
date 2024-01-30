import dataclasses


@dataclasses.dataclass(frozen=True)
class UrlPaths:
    # 属性名
    # Tuple構造：
    # 0 エイリアス（データ取得時の呼称）
    # 1 エイリアス対象データの入手先URL
    # 2 エイリアス対象データの一時保存先
    # 3 エイリアス対象データの一時保存ファイル名称
    # 4 エイリアス対象データの保存先 #2のtmpフォルダから移動するので、フォルダ構造は同じにしておく
    # 5 エイリアス対象データの保存ファイル名称
    # 6 スクレイピング時のバッチサイズ

    DB_DOMAIN: tuple = ("db_dmain", "https://db.netkeiba.com/", "./data/tmp/", None, "./data/html/", None)
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RACE_URL: tuple = (
        "race_results_list",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "race_results/",  # 2
        "temp_race_results_list",  # 3
        DB_DOMAIN[4] + "race_results/",  # 4
        "race_results_list",  # 5
        100,  # 6
    )
    # 馬の過去成績テーブルが含まれるページ
    HORSE_URL: tuple = (
        "horse_results_list",  # 0
        DB_DOMAIN[1] + "horse/",  # 1
        DB_DOMAIN[2] + "horse_results/",  # 2
        "temp_horse_results_list",  # 3
        DB_DOMAIN[4] + "horse_results/",  # 4
        "horse_results_list",  # 5
        100,  # 6
    )
    # 血統テーブルが含まれるページ
    PED_URL: tuple = (
        "ped_list",  # 0
        HORSE_URL[1] + "ped/",  # 1
        DB_DOMAIN[2] + "ped/",  # 2
        "temp_ped_list.csv",  # 3
        DB_DOMAIN[4] + "ped/",  # 4
        "ped_list",  # 5
        100,  # 6
    )

    TOP_URL: tuple = ("top_page", "https://race.netkeiba.com/top/", "./data/tmp/", None, "./data/html/", None)
    # 開催日程ページ
    CALENDAR_URL: tuple = (
        "kaisai_date_list",  # 0
        TOP_URL[1] + "calendar.html",  # 1
        TOP_URL[2] + "kaisai_date/",  # 2
        "temp_kaisai_date_list",  # 3
        TOP_URL[2] + "kaisai_date/",  # 4
        "kaisai_date_list",  # 5
        50,  # 6
    )

    # レース一覧ページ
    RACE_LIST_URL: tuple = (
        "race_id_list",  # 0
        TOP_URL[1] + "race_id_list.html",  # 1
        TOP_URL[2] + "race_list/",  # 2
        "temp_race_id__list",  # 3
        TOP_URL[4] + "race_id_list/",  # 4
        "race_id_list",  # 5
        100,  # 6
    )

    # 出馬表ページ
    SHUTUBA_TABLE: tuple = (
        "shutuba_table",  # 0
        "https://race.netkeiba.com/race/shutuba.html",  # 1
        TOP_URL[2] + "shutuba_table/",  # 2
        "temp_shutuba_table",  # 3
        TOP_URL[4] + "shutuba_table/",  # 4
        "shutuba_table",  # 5
        100,  # 6
    )
