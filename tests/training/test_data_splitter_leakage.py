"""DataSplitter の目的変数リーク防止テスト（§2c/2j で着順を追加した際の回帰防止）。

ResultsProcessor は §2c/2j 集計のため当該レースの実着順 '着順'(ResultsCols.RANK) を
選択するが、rank = (着順 < 4) の元データであるため学習入力に残すとリークになる。
本テストは X_train / X_test / スタッキング分割の全特徴量行列から '着順' が
除外されることを保証する。
"""

import sys
import types

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

# optuna スタブ
_optuna_stub = types.ModuleType("optuna")
_lgb_stub = types.ModuleType("optuna.integration.lightgbm")


class _DatasetStub:
    def __init__(self, data, label):
        self.data = data
        self.label = label


_lgb_stub.Dataset = _DatasetStub
_optuna_stub.integration = types.SimpleNamespace(lightgbm=_lgb_stub)
sys.modules.setdefault("optuna", _optuna_stub)
sys.modules.setdefault("optuna.integration", _optuna_stub.integration)
sys.modules.setdefault("optuna.integration.lightgbm", _lgb_stub)

from src.training._data_splitter import DataSplitter  # noqa: E402


def _make_featured_with_chakujun(n_races=60, horses=8, seed=0):
    """'着順'(実着順) を含む featured_data。rank は (着順 < 4)。"""
    rng = np.random.default_rng(seed)
    rows = []
    base = pd.Timestamp("2020-01-01")
    for i in range(n_races):
        race_id = f"r{i:04d}"
        date = base + pd.Timedelta(days=i)
        for h in range(horses):
            chakujun = h + 1  # 1..horses (実着順)
            rows.append(
                {
                    "race_id": race_id,
                    "date": date,
                    ResultsCols.RANK: chakujun,  # '着順'
                    "rank": 1 if chakujun < 4 else 0,
                    ResultsCols.TANSHO_ODDS: float(rng.uniform(1.5, 20.0)),
                    "feat_a": float(rng.normal()),
                }
            )
    return pd.DataFrame(rows).set_index("race_id")


class TestChakujunNotLeaked:
    def test_chakujun_dropped_from_x_train(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert ResultsCols.RANK not in ds.X_train.columns

    def test_chakujun_dropped_from_x_test(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert ResultsCols.RANK not in ds.X_test.columns

    def test_tansho_odds_kept_in_x_test(self):
        """X_test は EV 計算のため単勝オッズを保持する（着順だけ落とす）。"""
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert ResultsCols.TANSHO_ODDS in ds.X_test.columns

    def test_binary_rank_not_in_features(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert "rank" not in ds.X_train.columns
        assert "rank" not in ds.X_test.columns

    def test_chakujun_dropped_from_stacking_splits(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        ds.make_stacking_splits(meta_ratio=0.3)
        assert ResultsCols.RANK not in ds.X_base_train.columns
        assert ResultsCols.RANK not in ds.X_meta_train.columns
        assert ResultsCols.RANK not in ds.X_calib.columns

    def test_feature_retained(self):
        """通常の特徴量は残ること（過剰 drop の確認）。"""
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert "feat_a" in ds.X_train.columns

    def test_works_without_chakujun_column(self):
        """'着順' 列が無い DataFrame でも errors='ignore' で動作すること（後方互換）。"""
        df = _make_featured_with_chakujun().drop(columns=[ResultsCols.RANK])
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        assert "feat_a" in ds.X_train.columns
        assert ResultsCols.RANK not in ds.X_train.columns
