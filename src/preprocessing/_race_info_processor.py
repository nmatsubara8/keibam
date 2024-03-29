import pandas as pd

from src.preprocessing._abstract_data_processor import AbstractDataProcessor


class RaceInfoProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        """
        初期処理
        """
        super().__init__(filepath)

    def _preprocess(self):
        """
        前処理
        """
        df = self.raw_data
        # 距離は10の位を切り捨てる
        df["course_len"] = df["course_len"].astype(float) // 100
        # 日付型に変更
        df["date"] = pd.to_datetime(df["date"], format="%Y年%m月%d日")
        # 開催場所
        # df["開催"] = df.index.map(lambda x: str(x)[4:6])
        df["開催"] = df["place_id"].astype(int)
        df.drop(columns=["place_id"], inplace=True)
        df.drop(columns=["age"], inplace=True)
        df.drop(columns=["sex"], inplace=True)
        df.set_index("race_id", inplace=True)

        return df
