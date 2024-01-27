# import shutil
# import os
# import re

# import datetime
# import time

# import urllib.request
# from urllib.request import urlopen

# import pandas as pd
# from bs4 import BeautifulSoup
# from selenium.webdriver.common.by import By
# from tqdm.auto import tqdm

# from src.constants import _url_paths
# from src.preparing.prepare_chrome_driver import prepare_chrome_driver
from src.constants._url_paths import UrlPaths


class DataLoader_URL:
    def __init__(
        self,
        alias="kaisai_date_list",
        from_location="",
        save_location="",
        save_file_name="",
        rerun=False,
    ):
        self.alias = alias
        self.from_location = from_location
        self.save_location = save_location
        self.save_file_name = save_file_name
        self.rerun = rerun

    def alias_existence_check(self):
        url_paths = UrlPaths()

        # クラスの属性を取得
        attributes = [attr for attr in url_paths.__annotations__ if not attr.startswith("_")]
        # 順番にアクセス
        for attr in attributes:
            if self.alias == getattr(url_paths, attr)[0]:
                self.from_location = getattr(url_paths, attr)[1]
                print(f"OK:{self.from_location}")
            else:
                print("NG")
