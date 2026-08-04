"""特徴量契約（学習=推論の列名・列順・dtype parity）のテスト（#24）。

位置ベース predict でも「列の並び替え/追加/欠落」を検出・是正できることを固定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.training._feature_contract import (
    FeatureContract,
    FeatureContractError,
    require_present,
)


def _train_frame():
    return pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4], "c": [0.1, 0.2]})


def test_require_present_strict_raises_on_missing():
    with pytest.raises(FeatureContractError, match="不足"):
        require_present(["a", "b", "c"], ["a", "c"])


def test_require_present_lenient_returns_missing():
    assert require_present(["a", "b", "c"], ["a", "c"], lenient=True) == ["b"]


def test_require_present_ignores_extra_and_order():
    assert require_present(["a", "b"], ["extra", "b", "a"]) == []


def test_align_reorders_to_contract_order():
    c = FeatureContract.from_frame(_train_frame())
    shuffled = _train_frame()[["c", "a", "b"]]           # 推論時に列順が入れ替わっている
    out = c.align(shuffled)
    assert list(out.columns) == ["a", "b", "c"]          # 契約順へ復元
    # 位置ベース .values でも学習時と同じ並びになる（silent 誤予測を防ぐ核心）
    assert np.array_equal(out.values, _train_frame().values)


def test_align_drops_extra_columns_by_default():
    c = FeatureContract.from_frame(_train_frame())
    extra = _train_frame().assign(zzz=[9, 9])            # 契約外の余分列
    out = c.align(extra)
    assert list(out.columns) == ["a", "b", "c"]          # 余分列は drop


def test_align_extra_columns_error_when_disallowed():
    c = FeatureContract.from_frame(_train_frame())
    extra = _train_frame().assign(zzz=[9, 9])
    with pytest.raises(FeatureContractError, match="契約外"):
        c.align(extra, allow_extra=False)


def test_align_missing_column_is_fail_fast():
    c = FeatureContract.from_frame(_train_frame())
    missing = _train_frame().drop(columns=["b"])          # 学習時の列が欠落
    with pytest.raises(FeatureContractError, match="不足"):
        c.align(missing)


def test_dtype_check_and_coerce():
    c = FeatureContract.from_frame(_train_frame())         # a=float64, b=int64, c=float64
    wrong = pd.DataFrame({"a": [1.0, 2.0], "b": [3.0, 4.0], "c": [0.1, 0.2]})  # b が float
    with pytest.raises(FeatureContractError, match="dtype"):
        c.align(wrong, check_dtypes=True)
    out = c.align(wrong, coerce_dtypes=True)               # 契約 dtype へ強制変換
    assert str(out["b"].dtype) == "int64"


def test_roundtrip_dict():
    c = FeatureContract.from_frame(_train_frame())
    c2 = FeatureContract.from_dict(c.to_dict())
    assert c2 == c
    assert list(c2.align(_train_frame()[["c", "b", "a"]]).columns) == ["a", "b", "c"]


def test_non_dataframe_input_errors():
    c = FeatureContract.from_frame(_train_frame())
    with pytest.raises(FeatureContractError, match="DataFrame"):
        c.align(np.array([[1, 2, 3]]))


def test_contract_names_match_feature_names_list():
    # 学習側の不変条件: KeibaAI は feature_names_ = list(X_base_train.columns) と
    # feature_contract_ = FeatureContract.from_frame(X_base_train) を同時に採る。
    # 両者が構造的に一致する（drift しない）ことを固定する。
    df = _train_frame()
    feature_names_ = list(df.columns)  # KeibaAI._keiba_ai:253 相当
    contract = FeatureContract.from_frame(df)
    assert list(contract.names) == feature_names_


def test_keiba_ai_imports_feature_contract():
    # 学習側の配線（KeibaAI が FeatureContract を import）が壊れていないことの疎通担保。
    # 実際の学習時採録（feature_contract_ の設定）は本番学習パスで検証する。
    import src.training._keiba_ai as ka
    assert ka.FeatureContract is FeatureContract


def _bare_keiba_ai(*, contract, feature_names=None):
    """重い学習をせず calc_score の列契約だけを検証する最小オブジェクト。"""
    from types import SimpleNamespace

    from src.training._keiba_ai import KeibaAI

    ai = object.__new__(KeibaAI)
    ai._calibrated_model = object()
    ai._KeibaAI__model_wrapper = SimpleNamespace(lgb_model=object())
    ai.feature_contract_ = contract
    ai.feature_names_ = feature_names
    return ai


class _ReturningPolicy:
    def calc(self, model, X):
        return X


class _TrainingWrapper:
    def __init__(self):
        self.trained = False
        self.tuned = False

    def train(self, datasets):
        self.trained = True

    def tune_hyper_params(self, datasets, tuning_config=None):
        self.tuned = True


def _bare_training_ai():
    from types import SimpleNamespace

    from src.training._keiba_ai import KeibaAI

    ai = object.__new__(KeibaAI)
    ai._KeibaAI__datasets = SimpleNamespace(X_train=_train_frame())
    ai._KeibaAI__model_wrapper = _TrainingWrapper()
    ai.feature_names_ = None
    ai.feature_contract_ = None
    return ai


def test_train_without_tuning_captures_contract():
    ai = _bare_training_ai()

    ai.train_without_tuning()

    assert ai._KeibaAI__model_wrapper.trained is True
    assert ai.feature_contract_ == FeatureContract.from_frame(_train_frame())
    assert ai.feature_names_ == ["a", "b", "c"]


def test_train_with_tuning_captures_contract():
    ai = _bare_training_ai()

    ai.train_with_tuning(tuning_config={"n_trials": 1})

    assert ai._KeibaAI__model_wrapper.tuned is True
    assert ai._KeibaAI__model_wrapper.trained is True
    assert ai.feature_contract_ == FeatureContract.from_frame(_train_frame())


def test_calc_score_contract_reorders_and_drops_extra():
    contract = FeatureContract.from_frame(_train_frame())
    ai = _bare_keiba_ai(contract=contract)
    incoming = _train_frame()[["c", "a", "b"]].assign(extra=9)

    out = ai.calc_score(incoming, _ReturningPolicy())

    assert list(out.columns) == ["a", "b", "c"]
    assert np.array_equal(out.values, _train_frame().values)


def test_calc_score_contract_missing_is_fail_fast(monkeypatch):
    monkeypatch.delenv("KEIBA_LENIENT_FEATURES", raising=False)
    ai = _bare_keiba_ai(contract=FeatureContract.from_frame(_train_frame()))

    with pytest.raises(FeatureContractError, match="不足"):
        ai.calc_score(_train_frame().drop(columns="b"), _ReturningPolicy())


def test_calc_score_lenient_mode_zero_fills_missing(monkeypatch):
    monkeypatch.setenv("KEIBA_LENIENT_FEATURES", "1")
    ai = _bare_keiba_ai(contract=FeatureContract.from_frame(_train_frame()))

    out = ai.calc_score(_train_frame().drop(columns="b"), _ReturningPolicy())

    assert list(out.columns) == ["a", "b", "c"]
    assert np.array_equal(out["b"].to_numpy(), np.zeros(2))


def test_calc_score_legacy_model_keeps_zero_fill(monkeypatch):
    monkeypatch.delenv("KEIBA_LENIENT_FEATURES", raising=False)
    ai = _bare_keiba_ai(contract=None, feature_names=["a", "b", "c"])

    out = ai.calc_score(_train_frame().drop(columns="b"), _ReturningPolicy())

    assert np.array_equal(out["b"].to_numpy(), np.zeros(2))
