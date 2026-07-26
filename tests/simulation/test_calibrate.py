"""calibrate-ev の fit ジョブ（build_calibration_inputs / fit_and_save_*）の論理テスト。

合成 featured + 偽モデル（predict_proba）で、レース単位の入力組み立てと
各アーティファクトの fit→保存→ロード往復が正しく動くことを確認する。
"""

import numpy as np
import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols
from src.policies._blend import load_blend_weights
from src.policies._calibration import load_calibrator
from src.policies._harville import load_place_exponents
from src.simulation._calibrate import build_calibration_inputs
from src.simulation._calibrate import fit_all
from src.simulation._calibrate import fit_and_save_blend
from src.simulation._calibrate import fit_and_save_calibrator
from src.simulation._calibrate import fit_and_save_place_exponents


class _FeatureModel:
    """`feat` 列をロジットとして predict_proba を返す偽モデル（位置非依存）。"""

    def predict_proba(self, X):
        z = np.asarray(X["feat"], dtype=float)
        p = 1.0 / (1.0 + np.exp(-z))
        return np.column_stack([1.0 - p, p])


def _make_featured(n_races=12, field=6, seed=0):
    rng = np.random.default_rng(seed)
    rows = []
    index = []
    for r in range(n_races):
        # feat はレースごとに馬の強さ（馬番1が強い傾向）。着順は feat 降順 + ノイズ。
        feats = rng.normal(0.0, 1.0, size=field) + np.linspace(1.0, -1.0, field)
        order = np.argsort(-(feats + rng.normal(0, 0.3, size=field)))
        rank = np.empty(field, dtype=int)
        for pos, horse_idx in enumerate(order, start=1):
            rank[horse_idx] = pos
        odds = np.clip(1.0 / (np.exp(feats) / np.exp(feats).sum()) * 0.8, 1.1, 200.0)
        for h in range(field):
            rows.append({
                ResultsCols.UMABAN: h + 1,
                ResultsCols.WAKUBAN: h + 1,
                ResultsCols.TANSHO_ODDS: float(odds[h]),
                ResultsCols.RANK: int(rank[h]),
                "feat": float(feats[h]),
            })
            index.append(f"2025{r:08d}")
    return pd.DataFrame(rows, index=index)


class TestBuildInputs:
    def test_shapes_and_orders(self):
        X = _make_featured()
        inputs = build_calibration_inputs(_FeatureModel(), X)
        assert inputs.n_races == 12
        # 全レース 6 頭で着順が揃う → place/blend とも 12 レース
        assert len(inputs.place_races) == 12
        assert len(inputs.blend_races) == 12
        # 較正標本 = 全馬
        assert inputs.raw_probs.size == 12 * 6
        assert inputs.outcomes.sum() == 12  # 各レース 1着が 1 頭

    def test_place_order_is_finishing_order(self):
        X = _make_featured(n_races=1, field=5)
        inputs = build_calibration_inputs(_FeatureModel(), X)
        wp, (first, second, third) = inputs.place_races[0]
        race = X.iloc[:5]
        want = race.sort_values(ResultsCols.RANK)[ResultsCols.UMABAN].astype(int).tolist()
        assert [first, second, third] == want[:3]
        # winner と 1着が一致
        _, _, winner = inputs.blend_races[0]
        assert winner == want[0]

    def test_public_prob_normalized(self):
        X = _make_featured(n_races=1, field=4)
        inputs = build_calibration_inputs(_FeatureModel(), X)
        _, p_public, _ = inputs.blend_races[0]
        assert sum(p_public.values()) == pytest.approx(1.0)

    def test_missing_rank_excluded_from_calibration(self):
        X = _make_featured(n_races=2, field=5)
        col = X.columns.get_loc(ResultsCols.RANK)
        X.iloc[0, col] = np.nan  # 先頭1頭の着順を欠損に（race_id は非ユニークなので位置指定）
        inputs = build_calibration_inputs(_FeatureModel(), X)
        assert inputs.raw_probs.size == 2 * 5 - 1
        assert inputs.n_races == 2  # 当該レースは他4頭が揃うので残る


class TestFitAndSave:
    def test_place_exponents_roundtrip(self, tmp_path):
        X = _make_featured()
        inputs = build_calibration_inputs(_FeatureModel(), X)
        path = str(tmp_path / "place_exponents.json")
        exp = fit_and_save_place_exponents(inputs, path)
        loaded = load_place_exponents(path)
        assert loaded is not None
        assert loaded.gamma == pytest.approx(exp.gamma)
        assert loaded.delta == pytest.approx(exp.delta)
        assert exp.gamma > 0 and exp.delta > 0

    def test_calibrator_roundtrip(self, tmp_path):
        X = _make_featured()
        inputs = build_calibration_inputs(_FeatureModel(), X)
        path = str(tmp_path / "win_calibrator.json")
        cal = fit_and_save_calibrator(inputs, path)
        loaded = load_calibrator(path)
        assert loaded is not None
        # 単調非減少
        ys = np.asarray(loaded.y)
        assert np.all(np.diff(ys) >= -1e-9)
        np.testing.assert_allclose(cal.predict([0.1, 0.5]), loaded.predict([0.1, 0.5]))

    def test_blend_roundtrip(self, tmp_path):
        X = _make_featured()
        inputs = build_calibration_inputs(_FeatureModel(), X)
        path = str(tmp_path / "blend_weights.json")
        w = fit_and_save_blend(inputs, path)
        loaded = load_blend_weights(path)
        assert loaded is not None
        assert loaded.alpha == pytest.approx(w.alpha)
        assert loaded.beta == pytest.approx(w.beta)


class TestFitAll:
    def test_fit_all_writes_selected(self, tmp_path):
        X = _make_featured()
        summary = fit_all(
            _FeatureModel(), X, models_dir=str(tmp_path), which=("exponents", "calibrator"),
        )
        assert "exponents" in summary and "calibrator" in summary
        assert "blend" not in summary
        assert (tmp_path / "place_exponents.json").exists()
        assert (tmp_path / "win_calibrator.json").exists()
        assert not (tmp_path / "blend_weights.json").exists()
        assert summary["n_races"] == 12
