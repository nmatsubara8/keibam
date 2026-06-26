import logging

from src.preparing._data_loader import DataLoader
from src.preparing.modules import create_raw_horse_info
from src.preparing.modules import create_raw_horse_ped
from src.preparing.modules import create_raw_horse_results
from src.preparing.modules import create_raw_race_info
from src.preparing.modules import create_raw_race_results
from src.preparing.modules import create_raw_race_return
from src.preparing.modules import process_bin_file

logger = logging.getLogger(__name__)


class TableCreator(DataLoader):
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
        from_date="2020-01-01",
        to_date="2021-01-01",
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
        )
        self.target_data = []
        # 増分処理用: 指定時はこの id（race_id/horse_id）の bin だけを処理する
        # （process_bin_file が getattr で参照）。None は全件処理（従来挙動）。
        self.only_ids: list[str] | None = None

    def create_race_results_table(self):
        """
        race_html binファイルを受け取って、レース結果テーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_results)

    def create_tmp_for_race_info(self):
        """
        raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_info)

    def create_race_info_table(self):
        """
        raceページのhtmlを受け取って、レース情報テーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_info)

    def create_race_return_table(self):
        """
        raceページのhtmlを受け取って、払い戻しテーブルに変換する関数。
        """
        process_bin_file(self, create_raw_race_return)

    def create_horse_results_table(self):
        """
        # horseページのhtmlを受け取って、馬の過去成績のDataFrameに変換する関数。
        """
        process_bin_file(self, create_raw_horse_results)

    def create_horse_info_table(self):
        """
        horseページのhtmlを受け取って、馬の基本情報のDataFrameに変換する関数。
        """

        process_bin_file(self, create_raw_horse_info)

    def scrape_peds_list(self):
        """
        horse/pedページのhtmlを受け取って、血統のDataFrameに変換する関数。
        """
        process_bin_file(self, create_raw_horse_ped)

    def create_table_for_predict(self):
        """予測用テーブル作成（Playwright 全面移行後に実装予定）。"""
        raise NotImplementedError

    def update_horse_table(self):
        """馬の結果テーブル更新（未実装）。"""
        raise NotImplementedError
