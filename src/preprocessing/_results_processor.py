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
        # 着順の前処理
        df = self._preprocess_rank(df)

        # 性齢を性と年齢に分ける
        # サイト上のテーブルに存在する列名は、ResultsColsクラスで定数化している。
        df["性"] = df[Cols.SEX_AGE].map(lambda x: str(x)[0] if pd.notna(x) else None)
        df["年齢"] = pd.to_numeric(
            df[Cols.SEX_AGE].map(lambda x: str(x)[1:] if pd.notna(x) else None),
            errors="coerce",
        ).astype("Int64")

        # 馬体重を体重と体重変化に分ける
        df["体重"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[0]
        df["体重変化"] = df[Cols.WEIGHT_AND_DIFF].str.split("(", expand=True)[1].str[:-1]

        # errors='coerce'で、"計不"など変換できない時に欠損値にする
        df["体重"] = pd.to_numeric(df["体重"], errors="coerce")
        df["体重変化"] = pd.to_numeric(df["体重変化"], errors="coerce")

        # 各列を数値型に変換
        df[Cols.TANSHO_ODDS] = df[Cols.TANSHO_ODDS].astype(float)
        df[Cols.KINRYO] = df[Cols.KINRYO].astype(float)
        df[Cols.WAKUBAN] = pd.to_numeric(df[Cols.WAKUBAN], errors="coerce").astype("Int64")
        df[Cols.UMABAN] = pd.to_numeric(df[Cols.UMABAN], errors="coerce").astype("Int64")

        # 6/6出走数追加
        df = self._add_n_horses(df)

        # カラム抽出
        df = self._select_columns(df)

        return df

    def _add_n_horses(self, raw):
        """出走頭数 n_horses を「同一 race_id の実出走数」で付与する。

        旧実装 ``df.index.map(df.index.value_counts())`` は生 pickle が
        RangeIndex（race_id は通常列）の形状だと各行 index が一意になり、
        n_horses が全馬 1 に縮退した。この縮退列は featured_data 本流の
        _rel_rank（着順/頭数）を壊す。race_id を明示的に取り出し groupby
        サイズで実頭数を数える（_horse_features.py の頭数算出と同規約）。
        """
        df = raw.copy()
        if "race_id" in df.columns:
            race_ids = df["race_id"]
        else:  # race_id がインデックス側（processor 往復後の pickle 等）
            race_ids = df.index.to_series()
        df["n_horses"] = race_ids.groupby(race_ids).transform("size").to_numpy()
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
        # Win ヘッド用の第二ラベル（1着=1）。Place ヘッド(rank=top3) と別目的変数。
        # 学習入力からは rank と同様に必ず除外する（_DROP_FOR_TRAIN / _DROP_FOR_PREDICT）。
        df["rank_win"] = df[Cols.RANK].map(lambda x: 1 if x == 1 else 0)
        return df

    def _select_columns(self, raw):
        """
        カラム抽出
        """
        df = raw.copy()
        # race_id がインデックスにあり列にない場合（pickle 再保存後の状態）は列に復元する
        if "race_id" not in df.columns and df.index.name == "race_id":
            df = df.reset_index()
        cols = [
            "race_id",
            Cols.RANK,  # 着順 (actual finishing position; used for jockey/trainer/sire stats)
            Cols.WAKUBAN,  # 枠番
            Cols.UMABAN,  # 馬番
            Cols.KINRYO,  # 斤量
            Cols.TANSHO_ODDS,  # 単勝
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
            "rank_win",
        ]
        # 通過（コーナー順）はアーカイブ取込 results に有り、脚質(leg_type)算出の入力になる。
        # 旧スクレイプ data に無い場合もあるため、存在するときだけ保持する（欠落で KeyError しない）。
        if "通過" in df.columns:
            cols.append("通過")
        df = df[cols]
        df.set_index("race_id", inplace=True)
        return df
