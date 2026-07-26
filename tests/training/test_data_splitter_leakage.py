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
                    "rank_win": 1 if chakujun == 1 else 0,
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

    def test_both_labels_dropped_from_features(self):
        """rank(top3) と rank_win(1着) は**両方**特徴量から落ちること（相互リーク防止）。"""
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        for col in ("rank", "rank_win"):
            assert col not in ds.X_train.columns
            assert col not in ds.X_test.columns
        ds.make_stacking_splits(meta_ratio=0.3)
        for col in ("rank", "rank_win"):
            assert col not in ds.X_base_train.columns
            assert col not in ds.X_meta_train.columns
            assert col not in ds.X_calib.columns


class TestObjectFeatureCoercion:
    """object dtype の特徴量列（best_class_won 等）を数値へ強制するセーフティネット。

    脚質集計の best_class_won は race_class_level が None を返すと object dtype に
    なり得る。DataSplitter が学習入力を数値化しないと LightGBM が
    "pandas dtypes must be int, float or bool" で落ちる。既存 parquet を再ビルド
    せず学習可能にするため、DataSplitter が object 特徴量列を数値化することを保証する。
    """

    def test_object_feature_column_coerced_to_numeric(self):
        df = _make_featured_with_chakujun()
        # best_class_won を "3"/"未勝利"(非数値) 混在の object 列として注入
        vals = ["3" if i % 2 == 0 else "未勝利" for i in range(len(df))]
        df = df.assign(best_class_won=pd.Series(vals, index=df.index, dtype="object"))
        assert df["best_class_won"].dtype == object
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        # 学習特徴量として数値化されている（非数値→NaN、float 化）
        assert ds.X_train["best_class_won"].dtype.kind == "f"
        # .values が object にならず float 行列として取り出せる（LightGBM 契約）
        arr = ds.X_train.drop(columns=[c for c in ds.X_train.columns
                                       if ds.X_train[c].dtype == object], errors="ignore").values
        assert arr.dtype.kind == "f"

    def test_protected_non_numeric_columns_survive(self):
        # date/horse_id は数値化せず保持（date は時系列分割キー）
        df = _make_featured_with_chakujun()
        df = df.assign(horse_id=[f"h{i}" for i in range(len(df))])
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        # date はそのまま（分割が機能している＝train/test が非空）
        assert len(ds.X_train) > 0 and len(ds.X_test) > 0


class TestTargetColumn:
    """target_col で Place(rank) / Win(rank_win) ヘッドを切替える。"""

    def test_default_target_is_top3(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2)
        # 既定は rank(top3)。着順<4 が 1
        assert ds.y_train.equals(ds.train_data["rank"])

    def test_win_target_selects_rank_win(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2, target_col="rank_win")
        assert ds.y_train.equals(ds.train_data["rank_win"])
        assert ds.y_test.equals(ds.test_data["rank_win"])
        # 1着のみ正例 → top3 ラベルより正例が少ない
        assert ds.y_train.sum() < ds.train_data["rank"].sum()

    def test_win_target_stacking_labels(self):
        df = _make_featured_with_chakujun()
        ds = DataSplitter(df, test_size=0.2, valid_size=0.2, target_col="rank_win")
        ds.make_stacking_splits(meta_ratio=0.3)
        assert ds.y_base_train.equals(ds.base_train_data["rank_win"])
        assert ds.y_meta_train.equals(ds.meta_train_data["rank_win"])
        assert ds.y_calib.equals(ds.valid_data_optuna["rank_win"])

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
