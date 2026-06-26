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
    def create(
        featured_data, test_size=0.3, valid_size=0.3, target_col="rank", *, peds_processor=None
    ) -> KeibaAI:
        """
        featured_data: PreparedFeatures DTO または plain pd.DataFrame。
            PreparedFeatures を渡すと NN ストリーム (nn_scaler / X_nn_*) が有効になる。
        peds_processor: 学習済み LabelEncoder を持つ PedsProcessor。
            渡すと KeibaAI に紐付けられ、dill.dump 時に自動同梱される。
            推論時は ai.peds_processor.encoders_ で復元する。
        target_col: 目的変数列。"rank"=複勝(top3,既定) / "rank_win"=単勝(1着, Win ヘッド)。
        nn_scaler は DataSplitter が内部で生成し datasets.nn_scaler 経由で取得する。
        """
        datasets = DataSplitter(featured_data, test_size, valid_size, target_col=target_col)
        ai = KeibaAI(datasets)
        ai.peds_processor = peds_processor
        ai.nn_scaler = datasets.nn_scaler  # None if plain DataFrame was passed
        return ai

    @staticmethod
    def save(keibaAI: KeibaAI, version_name: str, suffix: str = "") -> None:
        """
        日付やバージョン、パラメータ、データなどを保存。
        保存先はmodels/(yyyymmdd)/(version_name)(suffix).pickle。

        suffix は Win ヘッド等の併存モデル用（例 "__win"）。空なら従来通り Place ヘッド。

        学習に使った DataSplitter は featured_data 全体（数十万行）を抱えており、
        そのまま dill.dump するとモデル 1 個で数百 MB になり、読込時に OOM の
        原因となる。推論に必要なのは effective_model / feature_names_ /
        peds_processor / nn_scaler のみなので、保存中だけ __datasets を切り離す。
        """
        yyyymmdd = datetime.date.today().strftime("%Y%m%d")
        # ディレクトリ作成
        os.makedirs(os.path.join("models", yyyymmdd), exist_ok=True)
        filepath_pickle = os.path.join("models", yyyymmdd, "{}{}.pickle".format(version_name, suffix))

        # 保存中だけ重い datasets を退避（dump 後に復元）
        datasets_attr = "_KeibaAI__datasets"
        saved_datasets = getattr(keibaAI, datasets_attr, None)
        try:
            setattr(keibaAI, datasets_attr, None)
            with open(filepath_pickle, mode="wb") as f:
                dill.dump(keibaAI, f)
        finally:
            setattr(keibaAI, datasets_attr, saved_datasets)

    @staticmethod
    def load(filepath: str) -> KeibaAI:
        with open(filepath, mode="rb") as f:
            return dill.load(f)
