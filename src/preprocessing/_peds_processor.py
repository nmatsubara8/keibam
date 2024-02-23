from sklearn.preprocessing import LabelEncoder

from src.preprocessing._abstract_data_processor import AbstractDataProcessor


class PedsProcessor(AbstractDataProcessor):
    """
    初期処理
    """

    def __init__(self, filepath):
        super().__init__(filepath)

    """
    前処理
    """

    def _preprocess(self):
        df = self.raw_data.copy()
        df["horse_id"] = df["horse_id"].astype(int)  # horse_id 列を整数型に変換

        # horse_id 列を除く列をカテゴリ型に変換する
        for column in df.columns[:-1]:
            if column != "horse_id":
                df[column] = LabelEncoder().fit_transform(df[column].fillna("Na"))
                df[column] = df[column].astype("category")
        # horse_id 列をインデックスに設定する
        df.set_index("horse_id", inplace=True)
        return df
