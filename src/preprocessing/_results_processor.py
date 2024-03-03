import pandas as pd

from src.constants._results_cols import ResultsCols as Cols
from src.preprocessing._abstract_data_processor import AbstractDataProcessor


class ResultsProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        """
        初期処理
        """
        super().__init__(filepath)

    def _preprocess(self):
        """
        前処理
        """
        df = self.raw_data.copy()
        # print(f"type df:{type(df)}")

        # print(f"df.head:{df.head()}")
        # 着順の前処理
        df = self._preprocess_rank(df)

        # 性齢を性と年齢に分ける
        # サイト上のテーブルに存在する列名は、ResultsColsクラスで定数化している。
        df["性"] = df[Cols.SEX_AGE].map(lambda x: str(x)[0])
        df["年齢"] = df[Cols.SEX_AGE].map(lambda x: str(x)[1:]).astype(int)

        # print(f"type df:{type(df)}")

        # print(f"df.head:{df.head()}")

        # print(f"Cols.WEIGHT_AND_DIFF:{Cols.WEIGHT_AND_DIFF}")
        # print(f"Cols.WEIGHT_AND_DIFF.head():{df[Cols.WEIGHT_AND_DIFF].head()}")
        # 馬体重を体重と体重変化に分ける
        df["体重"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[0]
        df["体重変化"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[1].str[:-1]

        # errors='coerce'で、"計不"など変換できない時に欠損値にする
        df["体重"] = pd.to_numeric(df["体重"], errors="coerce")
        df["体重変化"] = pd.to_numeric(df["体重変化"], errors="coerce")

        # 各列を数値型に変換
        df[Cols.TANSHO_ODDS] = df[Cols.TANSHO_ODDS].astype(float)
        df[Cols.KINRYO] = df[Cols.KINRYO].astype(float)
        df[Cols.WAKUBAN] = df[Cols.WAKUBAN].astype(int)
        df[Cols.UMABAN] = df[Cols.UMABAN].astype(int)

        # 6/6出走数追加
        df["n_horses"] = df.index.map(df.index.value_counts())

        # カラム抽出
        df = self._select_columns(df)

        return df

    def _preprocess_rank(self, raw):
        """
        着順の前処理
        """
        df = raw.copy()

        # 着順に数字以外の文字列が含まれているものを取り除く

        df[Cols.RANK] = pd.to_numeric(df[Cols.RANK], errors="coerce")
        df.dropna(subset=[Cols.RANK], inplace=True)
        df[Cols.RANK] = df[Cols.RANK].astype(int)
        df["rank"] = df[Cols.RANK].map(lambda x: 1 if x < 4 else 0)
        return df

    def _select_columns(self, raw):
        """
        カラム抽出
        """
        df = raw.copy()[
            [
                "race_id",
                # Cols.RANK, # 着順
                Cols.WAKUBAN,  # 枠番
                Cols.UMABAN,  # 馬番
                # Cols.HORSE_NAME, # 馬名
                # Cols.SEX_AGE, # 性齢
                Cols.KINRYO,  # 斤量
                # Cols.JOCKEY, # 騎手
                # Cols.TIME # タイム
                # Cols.RANK_DIFF # 着差
                Cols.TANSHO_ODDS,  # 単勝
                # Cols.POPULARITY, # 人気
                # Cols.WEIGHT_AND_DIFF, # 馬体重
                # Cols.TRAINER, # 調教師
                "horse_id",
                "jockey_id",
                "trainer_id",
                "owner_id",
                "性",
                "年齢",
                "体重",
                "体重変化",
                "n_horses",
                "rank",
            ]
        ]
        df.set_index("race_id", inplace=True)
        return df
