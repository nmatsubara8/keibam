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
    # 7 外部キーを保有するローカルファイルへのパス
    # 8 外部キーを保有するローカルファイル名称
    # 9 skip処理するかどうかのフラグ(デフォルトはFalse)

    TOP_URL: tuple = ("top_page", "https://race.netkeiba.com/top/", "./data/tmp/", None, "./data/html/", None)
    DB_DOMAIN: tuple = ("db_dmain", "https://db.netkeiba.com/", "./data/tmp/", None, "./data/html/", None)

    # 開催日程ページ
    CALENDAR_URL: tuple = (
        "kaisai_date_list",  # 0
        TOP_URL[1] + "calendar.html",  # 1
        TOP_URL[2] + "kaisai_date/",  # 2
        "temp_kaisai_date_list.txt",  # 3
        TOP_URL[4] + "kaisai_date/",  # 4
        "kaisai_date_list.pkl",  # 5
        100,  # 6
        "",  # 7
        "",  # 8
        "",  # 9
        False,  # 10
    )

    # レース一覧ページ
    RACE_LIST_URL: tuple = (
        "race_id_list",  # 0
        TOP_URL[1] + "race_list.html",  # 1
        TOP_URL[2] + "race_id_list/",  # 2
        "temp_race_id_list.txt",  # 3
        TOP_URL[4] + "race_id_list/",  # 4
        "race_id_list.pkl",  # 5
        50,  # 6
        DB_DOMAIN[4],  # 7
        DB_DOMAIN[5],  # 8
        "",  # 9
        False,  # 10
    )
    # 馬一覧ページ
    HORSE_LIST_URL: tuple = (
        "horse_id_list",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "horse_id_list/",  # 2
        "temp_horse_id_list.txt",  # 3
        DB_DOMAIN[4] + "horse_id_list/",  # 4
        "horse_id_list.pkl",  # 5
        5,  # 6
        TOP_URL[4] + "race_id_list/",  # 7
        "race_id_list.pkl",
        "",  # 9
        False,  # 10
    )

    # レースhtmlページ
    RACE_HTML: tuple = (
        "race_html",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "race/",  # 2
        "temp_race_html",  # 3
        DB_DOMAIN[4] + "race/",  # 4
        "race_html",  # 5  #
        1,  # 6
        DB_DOMAIN[4] + "race_id_list/",  # 7
        "race_id_list.pkl",  # 8
        "",  # 9
        False,  # 10
    )
    # レースhtmlページ
    HORSE_HTML: tuple = (
        "horse_html",  # 0
        DB_DOMAIN[1] + "horse/",  # 1
        DB_DOMAIN[2] + "horse/",  # 2
        "temp_horse_html",  # 3
        DB_DOMAIN[4] + "horse/",  # 4
        "horse_html",  # 5  #
        1,  # 6
        DB_DOMAIN[4] + "horse_id_list/",  # 7
        "horse_id_list.pkl",  # 8
        "",  # 9
        False,  # 10
    )
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RACE_URL: tuple = (
        "race_results_table",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "race_results/",  # 2
        "temp_race_results_table.csv",  # 3
        DB_DOMAIN[4] + "race_results/",  # 4
        "race_results_table.h5",  # 5
        100,  # 6
        DB_DOMAIN[4] + "race/",  # 7
        "*.bin",  # 8 "race_results_table"においては、使わないこととする
        "",  # 9
        False,  # 10
    )
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RACE_INFO: tuple = (
        "race_info_table",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "race_info/",  # 2
        "temp_race_info_table.csv",  # 3
        DB_DOMAIN[4] + "race_info/",  # 4
        "race_info_table.h5",  # 5
        100,  # 6
        DB_DOMAIN[4] + "race/",  # 7
        "*.bin",  # 8 "race_info_table"においては、使わないこととする
        "",  # 9
        False,  # 10
    )
    # レース結果テーブル、レース情報テーブル、払い戻しテーブルが含まれるページ
    RETURN_INFO: tuple = (
        "race_return_table",  # 0
        DB_DOMAIN[1] + "race/",  # 1
        DB_DOMAIN[2] + "race_return/",  # 2
        "temp_race_return_table.csv",  # 3
        DB_DOMAIN[4] + "race_return/",  # 4
        "race_return_table.h5",  # 5
        100,  # 6
        DB_DOMAIN[4] + "race/",  # 7
        "*.bin",  # 8 "race_info_table"においては、使わないこととする
        "",  # 9
        False,  # 10
    )
    # 馬の過去成績テーブルが含まれるページ
    HORSE_URL: tuple = (
        "horse_results_list",  # 0
        DB_DOMAIN[1] + "horse/",  # 1
        DB_DOMAIN[2] + "horse_results/",  # 2
        "temp_horse_results_tabel",  # 3
        DB_DOMAIN[4] + "horse_results/",  # 4
        "horse_results_table",  # 5
        100,  # 6
        "",  # 7
        "",  # 8
        "",  # 9
        False,  # 10
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
        "",  # 7
        "",  # 8
        "",  # 9
        False,  # 10
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
        "",  # 7
        "",  # 8
        "",  # 9
        False,  # 10
    )
