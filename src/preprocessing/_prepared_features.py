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
