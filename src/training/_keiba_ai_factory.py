import datetime
import os

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
    def save(
        keibaAI: KeibaAI,
        version_name: str,
        category: str | None = None,
        models_dir: str = "models",
    ) -> str:
        """
        日付やバージョン、パラメータ、データなどを保存する。

        保存先は models/(yyyymmdd)/(version_name)[__category].pickle。
        category を指定すると `__{category}` サフィックス付きで保存し、6 分割
        （全国/地方 × 芝/ダート/障害）のカテゴリ別モデルを区別する。
        category が None または "combined" の場合は従来どおりサフィックス無し
        （統合モデル）で保存する。保存した pickle の絶対/相対パスを返す。
        """
        from src.constants._model_category import COMBINED

        yyyymmdd = datetime.date.today().strftime("%Y%m%d")
        outdir = os.path.join(models_dir, yyyymmdd)
        os.makedirs(outdir, exist_ok=True)
        suffix = f"__{category}" if category and category != COMBINED else ""
        filepath_pickle = os.path.join(outdir, "{}{}.pickle".format(version_name, suffix))
        with open(filepath_pickle, mode="wb") as f:
            dill.dump(keibaAI, f)
        return filepath_pickle

    @staticmethod
    def load(filepath: str) -> KeibaAI:
        with open(filepath, mode="rb") as f:
            return dill.load(f)
