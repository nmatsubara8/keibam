from typing import Dict, Optional

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder

from src.preprocessing._abstract_data_processor import AbstractDataProcessor

_UNKNOWN = "__unknown__"


class PedsProcessor(AbstractDataProcessor):
    """
    血統データの前処理。

    encoders=None (training mode): 各列の LabelEncoder を fit し self.encoders_ に保存。
    encoders=dict (inference mode): 保存済みエンコーダで transform のみ実行。
                                    未知カテゴリは "__unknown__" にマップ。
    """

    def __init__(self, filepath: str, encoders: Optional[Dict[str, LabelEncoder]] = None):
        # encoders_ は _preprocess() 内で populate される（train）か、引数をコピーする（inference）
        self.encoders_: Dict[str, LabelEncoder] = {}
        self._provided_encoders = encoders  # None = training mode
        super().__init__(filepath)

    def _preprocess(self) -> pd.DataFrame:
        df = self.raw_data.copy()
        df["horse_id"] = pd.to_numeric(df["horse_id"], errors="coerce").astype("Int64")

        for column in df.columns:
            if column != "horse_id":
                filled = df[column].fillna("Na")
                if self._provided_encoders is None:
                    # Training mode: fit encoder, include __unknown__ for future unseen values
                    le = LabelEncoder()
                    unique_vals = list(filled.unique()) + [_UNKNOWN]
                    le.fit(unique_vals)
                    df[column] = le.transform(filled)
                    self.encoders_[column] = le
                else:
                    # Inference mode: use saved encoder, map unknown categories to __unknown__
                    le = self._provided_encoders[column]
                    df[column] = self._transform_safe(le, filled)
                    self.encoders_[column] = le
                df[column] = df[column].astype("category")

        df.set_index("horse_id", inplace=True)
        return df

    @staticmethod
    def _transform_safe(le: LabelEncoder, series: pd.Series) -> np.ndarray:
        """Unknown categories are mapped to __unknown__ before transforming."""
        known = set(le.classes_)
        mapped = series.map(lambda x: x if x in known else _UNKNOWN)
        return le.transform(mapped)
