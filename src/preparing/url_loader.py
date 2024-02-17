from src.preparing.DataLoader import DataLoader
from src.preparing.modules import create_raw_horse_info
from src.preparing.modules import create_raw_horse_ped
from src.preparing.modules import create_raw_horse_results
from src.preparing.modules import create_raw_race_return
from src.preparing.modules import get_kaisai_date_list
from src.preparing.modules import get_raw_horse_id_list
from src.preparing.modules import process_bin_file
from src.preparing.modules import process_pkl_file
from src.preparing.modules import scrape_html_horse
from src.preparing.modules import scrape_html_ped
from src.preparing.modules import scrape_html_race
from src.preparing.modules import scrape_race_id_list


class KaisaiDateLoader(DataLoader):
    def __init__(
        self,
        alias="",
        from_location="",
        to_temp_location="",
        temp_save_file_name="",
        to_location="",
        save_file_name="",
        batch_size="",
        from_local_location="",
        from_local_file_name="",
        processing_id="",
        obtained_last_key="",
        target_data=None,
        skip=False,
        from_date="2008-01-01",
        to_date="2024-02-13",
    ):
        super().__init__(
            alias,
            from_location,
            to_temp_location,
            temp_save_file_name,
            to_location,
            save_file_name,
            batch_size,
            from_local_location,
            from_local_file_name,
            processing_id,
            obtained_last_key,
            target_data,
            skip,
            from_date,
            to_date,
        )
        self.target_data = []

    def scrape_kaisai_date(self):
        process_pkl_file(self, get_kaisai_date_list)

    ################################# Done ####################################
    def scrape_race_id_list(self):
        process_pkl_file(self, scrape_race_id_list)

    ################################# Done ####################################
    def scrape_horse_id_list(self):
        process_pkl_file(self, get_raw_horse_id_list)

    ################################# Done ####################################
    def scrape_html_race(self):
        process_pkl_file(self, scrape_html_race)

    ################################# Done ####################################
    def scrape_html_horse(self):
        process_pkl_file(self, scrape_html_horse)

    ################################# Done ####################################
    def scrape_html_ped(self):
        process_pkl_file(self, scrape_html_ped)

    ################################# Done ####################################
    def create_horse_results_table(self):
        process_bin_file(self, create_raw_horse_results)

    ################################# Done ####################################
    def create_horse_info_table(self):
        process_bin_file(self, create_raw_horse_info)

    ################################# Done ####################################
    def create_race_return_table(self):
        process_bin_file(self, create_raw_race_return)

    ################################# Done ####################################
    def scrape_peds_list(self):
        process_bin_file(self, create_raw_horse_ped)
