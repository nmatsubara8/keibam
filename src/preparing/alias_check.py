from src.constants._url_paths import UrlPaths
from src.preparing.URL_loader import KaisaiDateLoader


def reuest_by_alias(alias: str):
    # クラスの属性を取得
    url_paths = UrlPaths()
    attributes = [attr for attr in dir(url_paths) if not attr.startswith("_")]
    # クラスの属性をたどって、alias_listを作成
    alias_list = [getattr(url_paths, attr)[0] for attr in attributes if isinstance(getattr(url_paths, attr), tuple)]

    # タプルの[0]の中にaliasと等しいものがあれば
    if alias in alias_list:
        # 該当する属性のタプルを取得
        attr = [attr for attr in attributes if getattr(url_paths, attr)[0] == alias][0]

        # エイリアスが表すデータの取得先をセット
        from_location = getattr(url_paths, attr)[1]
        # エイリアスが表すデータの一時保存先をセット
        to_temp_location = getattr(url_paths, attr)[2]
        # エイリアスが表すデータの一時保存先ファイル名をセット
        temp_save_file_name = getattr(url_paths, attr)[3]
        # エイリアスが表すデータの正本保存先をセット
        to_location = getattr(url_paths, attr)[4]
        # エイリアスが表すデータの正本ファイル名をセット
        save_file_name = getattr(url_paths, attr)[5]
        batch_size = getattr(url_paths, attr)[6]

        data_request = KaisaiDateLoader(
            alias=alias,
            from_location=from_location,
            to_temp_location=to_temp_location,
            temp_save_file_name=temp_save_file_name,
            to_location=to_location,
            save_file_name=save_file_name,
            batch_size=batch_size,
        )
        if alias == "kaisai_date_list":
            # print("temp_save_file_name : ", temp_save_file_name)
            # print("save_file_name : ", save_file_name)
            # print("kaisai_date_list will be obtained from {} and stored in {}".format(from_location, to_temp_location))
            kaisai_date_list = data_request.scrape_kaisai_date()
            print("len:", len(kaisai_date_list))
            print("batch_size:", batch_size)
            print("kaisai_date_list[:5]:", kaisai_date_list[-5:])

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
    else:
        print("No such data")


kaisai_date_list = reuest_by_alias(alias="kaisai_date_list")

race_id_list = reuest_by_alias(alias="race_id_list")
