"""2系統特徴量 DataFrame を後続レイヤへ型安全に受け渡す DTO。"""

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class PreparedFeatures:
    """前処理パイプラインの出口 DTO。

    gbdt: LightGBM (base①) 用。全 ~480 特徴量・スケーリングなし・One-Hot 含む。
    nn:   NN (base②) 用。entity_cols + numeric_cols のみ列選択（スケーリング未適用）。
          DataSplitter が訓練データのみで NnFeatureScaler を fit し各スプリットを transform する。

    両 DataFrame は同一の race_id インデックスを持ち、DataSplitter が GBDT 側の
    日付ソートで分割した index を NN 側にも適用することで時系列整合を保つ。
    """

    gbdt: pd.DataFrame
    nn: pd.DataFrame


def prepared_from_gbdt(gbdt: pd.DataFrame) -> "PreparedFeatures":
    """gbdt DataFrame から NN ストリームを列選択のみで導出して PreparedFeatures を作る。

    Entity 列（ID 系 category 型）と numeric 列は gbdt DataFrame 内に共存しているため、
    特徴量エンジニアリングを再実行せずに、キャッシュ済み featured_data からも 2 系統を
    構成できる（FeatureEngineering.build と同一の列選択ロジック）。
    """
    from src.constants._nn_cols import NN_DROP_COLS
    from src.constants._nn_cols import NN_ENTITY_COLS

    entity_cols = [c for c in NN_ENTITY_COLS if c in gbdt.columns]
    exclude = set(NN_DROP_COLS) | set(entity_cols)
    numeric_cols = [
        c
        for c in gbdt.select_dtypes(include=["float64", "int64", "int32", "float32"]).columns
        if c not in exclude
    ]
    nn_df = gbdt[entity_cols + numeric_cols].copy()
    return PreparedFeatures(gbdt=gbdt, nn=nn_df)
