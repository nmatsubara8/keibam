from src.preparing.table_creator import TableCreator
from src.preparing.url_loader import KaisaiDateLoader


def select_process(alias):
    ################################# Done ####################################
    if alias == "kaisai_date_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_kaisai_date()
    ################################# Done ####################################
    elif alias == "race_id_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_race_id_list()
    ################################# Done ####################################
    elif alias == "horse_id_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_horse_id_list()

    elif alias == "race_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_html_race()

    elif alias == "horse_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_html_horse()

    elif alias == "race_results_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.create_race_results_table()

    elif alias == "tmp_for_race_info":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.create_tmp_for_race_info()

    elif alias == "race_info_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.create_race_info_table()

    elif alias == "race_return_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.create_race_return_table()

    elif alias == "horse_results_table":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.create_horse_results_table()

    elif alias == "horse_info_table":
        data_request = TableCreator()
        data_request.set_args(alias)
        data_request.create_horse_info_table()

    elif alias == "ped_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_html_ped()

    elif alias == "peds_list":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_peds_list()

    elif alias == "schedule":  #
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_schedule()

    elif alias == "scheduled_race_html":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_scheduled_race_html()

    elif alias == "tentative_info":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_latest_info()

    elif alias == "actual_info":
        data_request = KaisaiDateLoader()
        data_request.set_args(alias)
        data_request.scrape_latest_info()

    else:
        pass

    data_request.post_process_display()


# kaisai_date_list = select_process(alias="kaisai_date_list")
# race_id_list = select_process(alias="race_id_list")
# horse_id_list = select_process(alias="horse_id_list")  #
# race_html = select_process(alias="race_html")
# horse_html = select_process(alias="horse_html")
# ped_html = select_process(alias="ped_html")
race_results_table = select_process(alias="race_results_table")
# horse_results_table = select_process(alias="horse_results_table")
# race_return_table = select_process(alias="race_return_table")
# peds_list = select_process(alias="peds_list")
# horse_info_table = select_process(alias="horse_info_table")
# race_info_table = select_process(alias="race_info_table")


# tmp_for_race_info = select_process(alias="tmp_for_race_info")

#############
#############
#############
#############


# scheduled_race = select_process(alias="scheduled_race_html")

# schedule = select_process(alias="schedule")
# scheduled_horse = select_process(alias="schedule")

# tentative_info = select_process(alias="tentative_info")

# actual_info = select_process(alias="actual_info")
