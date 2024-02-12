import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class LocalPaths:
    # パス
    ## プロジェクトルートの絶対パス
    BASE_DIR: str = os.path.abspath("./")
    ## dataディレクトリまでの絶対パス
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    ### HTMLディレクトリのパス
    HTML_DIR: str = os.path.join(DATA_DIR, "html")
    HTML_RACE_DIR: str = os.path.join(HTML_DIR, "race")
    HTML_HORSE_DIR: str = os.path.join(HTML_DIR, "horse")
    HTML_PED_DIR: str = os.path.join(HTML_DIR, "ped")

    ### rawディレクトリのパス
    RAW_DIR: str = os.path.join(DATA_DIR, "raw")
    RAW_RESULTS_PATH: str = os.path.join(RAW_DIR, "results.pkl")
    RAW_RACE_INFO_PATH: str = os.path.join(RAW_DIR, "race_info.pkl")
    RAW_RETURN_TABLES_PATH: str = os.path.join(RAW_DIR, "return_tables.pkl")
    RAW_HORSE_RESULTS_PATH: str = os.path.join(RAW_DIR, "horse_results.pkl")
    RAW_HORSE_INFO_PATH: str = os.path.join(RAW_DIR, "horse_info.pkl")
    RAW_PEDS_PATH: str = os.path.join(RAW_DIR, "peds.pkl")

    ### masterディレクトリのパス
    MASTER_DIR: str = os.path.join(DATA_DIR, "master")
    MASTER_RAW_HORSE_RESULTS_PATH: str = os.path.join(MASTER_DIR, "horse_results_updated_at.csv")
