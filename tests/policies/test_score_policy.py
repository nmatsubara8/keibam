"""ScorePolicy 各クラスの単体テスト（モデル・ファイル I/O なし）。

BasicScorePolicy / StdScorePolicy / MinMaxScorePolicy /
RelativeProbaScorePolicy / ExpectedValueScorePolicy の出力構造・変換特性を検証する。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols
from src.policies._score_policy import (
    CURRENT_ODDS,
    PROB,
    BasicScorePolicy,
    ExpectedValueScorePolicy,
    MinMaxScorePolicy,
    RelativeProbaScorePolicy,
    StdScorePolicy,
)


# ──────────────────────────────────────────────────────
# スタブ・フィクスチャ
# ──────────────────────────────────────────────────────


class _FixedModel:
    """predict_proba が固定確率列を返すスタブ（[1-p, p] 形式）。"""

    def __init__(self, probs):
        self._probs = np.asarray(probs, dtype=float)

    def predict_proba(self, X):
        return np.column_stack([1.0 - self._probs, self._probs])


def _make_X(rows, race_id: str = "R001") -> pd.DataFrame:
    """rows: list of (umaban, wakuban, tansho_odds).

    index は race_id で名前付き（_calc の groupby("race_id") に必要）。
    """
    df = pd.DataFrame(
        [
            {ResultsCols.UMABAN: u, ResultsCols.WAKUBAN: w, ResultsCols.TANSHO_ODDS: o}
            for u, w, o in rows
        ],
        index=pd.Index([race_id] * len(rows), name="race_id"),
    )
    return df


def _make_X_multi(race_rows: dict) -> pd.DataFrame:
    """race_rows: {race_id: [(umaban, wakuban, odds), ...]}"""
    frames = []
    for race_id, rows in race_rows.items():
        frames.append(_make_X(rows, race_id=race_id))
    return pd.concat(frames)


class _FloatModel:
    """predict_proba が X を float 配列化するスタブ（pd.NA だと TypeError）。"""

    def predict_proba(self, X):
        arr = np.asarray(X, dtype=float)  # pd.NA(NAType) が残っていると TypeError
        p = np.full(len(arr), 0.5)
        return np.column_stack([1.0 - p, p])


def test_expected_value_policy_coerces_pd_na():
    # nullable 拡張dtype の pd.NA を含む特徴量でも、np.nan に正規化され predict が通る
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    X["feat"] = pd.array([1, pd.NA], dtype="Int64")
    table = ExpectedValueScorePolicy.calc(_FloatModel(), X)
    assert PROB in table.columns and len(table) == 2


def test_basic_policy_coerces_object_na():
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    X["feat"] = pd.Series([1.0, pd.NA], index=X.index, dtype=object)
    result = BasicScorePolicy.calc(_FloatModel(), X)
    assert "score" in result.columns and len(result) == 2


# ──────────────────────────────────────────────────────
# BasicScorePolicy
# ──────────────────────────────────────────────────────


def test_basic_score_policy_has_score_column():
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    result = BasicScorePolicy.calc(_FixedModel([0.6, 0.4]), X)
    assert "score" in result.columns


def test_basic_score_policy_has_wakuban_flag():
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    result = BasicScorePolicy.calc(_FixedModel([0.6, 0.4]), X)
    assert "wakuban_flag" in result.columns


def test_basic_score_policy_score_matches_model_output():
    probs = [0.7, 0.3]
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    result = BasicScorePolicy.calc(_FixedModel(probs), X)
    assert list(result["score"]) == pytest.approx(probs)


def test_basic_score_policy_wakuban_flag_when_equal():
    """馬番数 == 枠番数のとき wakuban_flag = 0。"""
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    result = BasicScorePolicy.calc(_FixedModel([0.6, 0.4]), X)
    assert result["wakuban_flag"].iloc[0] == 0


def test_basic_score_policy_wakuban_flag_when_more_umaban():
    """馬番数 > 枠番数のとき wakuban_flag = 1（同じ枠に複数馬）。"""
    X = _make_X([(1, 1, 5.0), (2, 1, 3.0), (3, 2, 4.0)])
    result = BasicScorePolicy.calc(_FixedModel([0.5, 0.3, 0.2]), X)
    assert result["wakuban_flag"].iloc[0] == 1


# ──────────────────────────────────────────────────────
# StdScorePolicy
# ──────────────────────────────────────────────────────


def test_std_score_policy_mean_near_zero():
    """レース内スコアの平均が 0 に近い。"""
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0), (3, 3, 4.0)])
    result = StdScorePolicy.calc(_FixedModel([0.6, 0.3, 0.1]), X)
    race_scores = result[result.index == "R001"]["score"]
    assert abs(race_scores.mean()) < 1e-9


def test_std_score_policy_multi_race_standardized_independently():
    """複数レースがそれぞれ独立して標準化される。"""
    X = _make_X_multi({"R001": [(1, 1, 5.0), (2, 2, 3.0)], "R002": [(1, 1, 2.0), (2, 2, 8.0)]})
    probs = [0.8, 0.2, 0.6, 0.4]
    result = StdScorePolicy.calc(_FixedModel(probs), X)
    for race_id in ["R001", "R002"]:
        scores = result[result.index == race_id]["score"]
        assert abs(scores.mean()) < 1e-9


# ──────────────────────────────────────────────────────
# MinMaxScorePolicy
# ──────────────────────────────────────────────────────


def test_minmax_score_policy_global_min_zero():
    """全体の最小スコアが 0。"""
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0), (3, 3, 4.0)])
    result = MinMaxScorePolicy.calc(_FixedModel([0.6, 0.3, 0.1]), X)
    assert result["score"].min() == pytest.approx(0.0)


def test_minmax_score_policy_global_max_one():
    """全体の最大スコアが 1。"""
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0), (3, 3, 4.0)])
    result = MinMaxScorePolicy.calc(_FixedModel([0.6, 0.3, 0.1]), X)
    assert result["score"].max() == pytest.approx(1.0)


# ──────────────────────────────────────────────────────
# RelativeProbaScorePolicy
# ──────────────────────────────────────────────────────


def test_relative_proba_sums_to_one_per_race():
    """レース内スコアの合計が 1.0。"""
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0), (3, 3, 4.0)])
    result = RelativeProbaScorePolicy.calc(_FixedModel([0.6, 0.3, 0.1]), X)
    total = result[result.index == "R001"]["score"].sum()
    assert abs(total - 1.0) < 1e-9


def test_relative_proba_multi_race_each_sums_to_one():
    X = _make_X_multi({"R001": [(1, 1, 5.0), (2, 2, 3.0)], "R002": [(1, 1, 2.0), (2, 2, 8.0)]})
    result = RelativeProbaScorePolicy.calc(_FixedModel([0.7, 0.3, 0.5, 0.5]), X)
    for race_id in ["R001", "R002"]:
        total = result[result.index == race_id]["score"].sum()
        assert abs(total - 1.0) < 1e-9


# ──────────────────────────────────────────────────────
# ExpectedValueScorePolicy
# ──────────────────────────────────────────────────────


def test_ev_score_policy_has_prob_column():
    X = _make_X([(1, 1, 5.0)])
    result = ExpectedValueScorePolicy.calc(_FixedModel([0.6]), X)
    assert PROB in result.columns


def test_ev_score_policy_has_current_odds_column():
    X = _make_X([(1, 1, 5.0)])
    result = ExpectedValueScorePolicy.calc(_FixedModel([0.6]), X)
    assert CURRENT_ODDS in result.columns


def test_ev_score_policy_prob_matches_model():
    X = _make_X([(1, 1, 5.0), (2, 2, 3.0)])
    result = ExpectedValueScorePolicy.calc(_FixedModel([0.7, 0.3]), X)
    assert list(result[PROB]) == pytest.approx([0.7, 0.3])


def test_ev_score_policy_odds_matches_input():
    X = _make_X([(1, 1, 8.5)])
    result = ExpectedValueScorePolicy.calc(_FixedModel([0.5]), X)
    assert result[CURRENT_ODDS].iloc[0] == pytest.approx(8.5)


def test_apply_scaler_handles_duplicate_race_index():
    """同一 race_id(重複ラベル index) でも列代入がクラッシュしない回帰テスト。

    _apply_scaler は transform で入力順を保持し、呼び出し側は .to_numpy() で位置代入する。
    apply(group_keys=False) の並べ替え + 重複ラベル reindex 不能クラッシュのガード。
    """
    from src.policies._score_policy import _apply_scaler, _scaler_standard

    s = pd.Series([1.0, 2.0, 3.0, 10.0, 20.0], index=["r1", "r1", "r1", "r2", "r2"])
    out = _apply_scaler(s, _scaler_standard)
    assert list(out.index) == list(s.index)            # 並び順保持
    assert np.allclose(out.to_numpy()[:3], [-1.224745, 0.0, 1.224745], atol=1e-5)  # r1 内標準化
    # 重複 index の列へ位置代入が通る
    df = pd.DataFrame({"x": s})
    df["x"] = _apply_scaler(df["x"], _scaler_standard).to_numpy()
    assert df["x"].notna().all()
