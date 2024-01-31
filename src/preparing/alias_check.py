from src.preparing.URL_loader import KaisaiDateLoader


def url_loader(alias):
    data_request = KaisaiDateLoader()

    data_request.set_args(alias)

    if alias == "kaisai_date_list":
        # print("temp_save_file_name : ", temp_save_file_name)
        # print("save_file_name : ", save_file_name)
        # print("kaisai_date_list will be obtained from {} and stored in {}".format(from_location, to_temp_location))
        target_data = data_request.scrape_kaisai_date()
        print("len:", len(target_data))
        print("batch_size:", data_request.batch_size)
        print("kaisai_date_list[:5]:", target_data[-5:])

    elif alias == "race_id_list":
        ############################################
        # ここに、kaisai_date_listの取得ロジックを追加する。今は仮に前処理からのデータをそのまま、引き継ぐ
        race_id_list = data_request.scrape_race_id_date(kaisai_date_list)
        print("race_id_list[:5]:", race_id_list[-5:])

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
    elif alias == "race_results_list":
        pass


kaisai_date_list = url_loader(alias="kaisai_date_list")

# race_id_list = url_loader(alias="race_id_list")
