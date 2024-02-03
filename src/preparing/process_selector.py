from src.preparing.table_creator import TableCreator
from src.preparing.url_loader import KaisaiDateLoader


def select_process(alias):
    if alias == "kaisai_date_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.scrape_kaisai_date()

    elif alias == "race_id_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.scrape_race_id_list()

    elif alias == "horse_id_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.scrape_horse_id_list()

    elif alias == "race_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.scrape_html_race()

    elif alias == "horse_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.scrape_html_horse()

    elif alias == "race_results_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.create_race_results_table()

    elif alias == "race_info_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.create_race_info_table()

    elif alias == "race_return_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.pre_process_display()
        data_request.create_race_return_table()

    elif alias == "horse_results_list":
        pass
    elif alias == "horse_list":
        pass
    elif alias == "pede_list":
        pass

    elif alias == "shutuba_table":
        pass
    else:
        pass

    data_request.post_process_display()


# kaisai_date_list = select_process(alias="kaisai_date_list")
# race_id_list = select_process(alias="race_id_list")
# race_html = select_process(alias="race_html")
horse_html = select_process(alias="horse_id_list")
# horse_html = select_process(alias="horse_html")
# race_results_table = select_process(alias="race_results_table")
# race_info_table = select_process(alias="race_info_table")
# race_retunr_table = select_process(alias="race_return_table")
