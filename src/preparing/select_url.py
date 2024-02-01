from src.preparing.url_loader import KaisaiDateLoader


def url_loader(alias):
    data_request = KaisaiDateLoader()

    data_request.set_args(alias)
    data_request.pre_process_display()

    if alias == "kaisai_date_list":
        data_request.scrape_kaisai_date()

    elif alias == "race_id_list":
        ############################################
        # ここに、kaisai_date_listの取得ロジックを追加する。今は仮に前処理からのデータをそのまま、引き継ぐ
        data_request.scrape_race_id_list()

    elif alias == "horse_results_list":
        pass
    elif alias == "horse_list":
        pass
    elif alias == "pede_list":
        pass
    elif alias == "race_results_list":
        pass
    elif alias == "shutuba_table":
        pass
    else:
        pass

    data_request.post_process_display()


# kaisai_date_list = url_loader(alias="kaisai_date_list")

race_id_list = url_loader(alias="race_id_list")
