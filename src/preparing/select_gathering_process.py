from src.preparing.url_loader import KaisaiDateLoader


def select_process(alias):
    data_request = KaisaiDateLoader()
    data_request.set_args(alias)
    data_request.pre_process_display()

    if alias == "kaisai_date_list":
        data_request.scrape_kaisai_date()

    elif alias == "race_id_list":
        data_request.scrape_race_id_list()

    elif alias == "race_html":
        data_request.scrape_html_race()

    elif alias == "race_html_horse":
        data_request.scrape_html_horse()

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


# kaisai_date_list = select_process(alias="kaisai_date_list")
# race_id_list = select_process(alias="race_id_list")
race_html = select_process(alias="race_html")
