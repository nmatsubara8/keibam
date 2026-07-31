"""特徴量契約（学習=推論の列名・列順・dtype parity）のテスト（#24）。

位置ベース predict でも「列の並び替え/追加/欠落」を検出・是正できることを固定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.training._feature_contract import FeatureContract, FeatureContractError


def _train_frame():
    return pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4], "c": [0.1, 0.2]})


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
