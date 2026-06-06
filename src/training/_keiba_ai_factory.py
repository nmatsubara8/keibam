import datetime
import os
from typing import Optional

import dill

from src.training._data_splitter import DataSplitter
from src.training._keiba_ai import KeibaAI


class KeibaAIFactory:
    """
    KeibaAIのインスタンスを作成するためのクラス
    """

    @staticmethod
    def create(featured_data, peds_processor=None, test_size=0.3, valid_size=0.3) -> KeibaAI:
        """
        featured_data: PreparedFeatures DTO または plain pd.DataFrame。
            PreparedFeatures を渡すと NN ストリーム (nn_scaler / X_nn_*) が有効になる。
        peds_processor: 学習済み LabelEncoder を持つ PedsProcessor。
            渡すと KeibaAI に紐付けられ、dill.dump 時に自動同梱される。
            推論時は ai.peds_processor.encoders_ で復元する。
        nn_scaler は DataSplitter が内部で生成し datasets.nn_scaler 経由で取得する。
        """
        datasets = DataSplitter(featured_data, test_size, valid_size)
        ai = KeibaAI(datasets)
        ai.peds_processor = peds_processor
        ai.nn_scaler = datasets.nn_scaler  # None if plain DataFrame was passed
        return ai

    @staticmethod
    def save(keibaAI: KeibaAI, version_name: str) -> None:
        """
        日付やバージョン、パラメータ、データなどを保存。
        保存先はmodels/(yyyymmdd)/(version_name).pickle。
        """
        yyyymmdd = datetime.date.today().strftime("%Y%m%d")
        # ディレクトリ作成
        os.makedirs(os.path.join("models", yyyymmdd), exist_ok=True)
        filepath_pickle = os.path.join("models", yyyymmdd, "{}.pickle".format(version_name))
        with open(filepath_pickle, mode="wb") as f:
            dill.dump(keibaAI, f)

    @staticmethod
    def load(filepath: str) -> KeibaAI:
        with open(filepath, mode="rb") as f:
            return dill.load(f)
