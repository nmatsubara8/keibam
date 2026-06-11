"""モデル比較シミュレーション ヘルパ（app._model_compare）のテスト。"""

import pandas as pd

from app._model_compare import BET_POLICY_CHOICES
from app._model_compare import align_profit_curves
from app._model_compare import comparison_table
from app._model_compare import cumulative_profit
from app._model_compare import recent_race_slice


class TestRecentRaceSlice:
    def _featured(self):
        rows = []
        for i, rid in enumerate(["r1", "r2", "r3", "r4", "r5"]):
            for h in range(2):
                rows.append({"race_id": rid, "date": pd.Timestamp("2026-01-01") + pd.Timedelta(days=i), "x": h})
        return pd.DataFrame(rows).set_index("race_id")

    def test_takes_most_recent_fraction(self):
        sliced = recent_race_slice(self._featured(), test_frac=0.4)
        assert set(sliced.index) == {"r4", "r5"}

    def test_min_one_race(self):
        sliced = recent_race_slice(self._featured(), test_frac=0.01)
        assert sliced.index.nunique() == 1

    def test_empty_input(self):
        assert recent_race_slice(pd.DataFrame()).empty
        assert recent_race_slice(None).empty

    def test_without_date_column_uses_race_id_order(self):
        df = self._featured().drop(columns=["date"])
        sliced = recent_race_slice(df, test_frac=0.2)
        assert set(sliced.index) == {"r5"}


class TestProfitCurves:
    def test_cumulative_profit(self):
        per_race = pd.DataFrame(
            {"bet_amount": [100, 100], "return_amount": [0, 340], "n_bets": [1, 1], "hit_or_not": [0, 1]},
            index=["r1", "r2"],
        )
        curve = cumulative_profit(per_race)
        assert curve.tolist() == [-100, 140]

    def test_cumulative_profit_empty(self):
        assert cumulative_profit(pd.DataFrame()).empty
        assert cumulative_profit(None).empty

    def test_align_profit_curves_ffill(self):
        a = pd.Series([-100, 140], index=["r1", "r3"])
        b = pd.Series([50], index=["r2"])
        aligned = align_profit_curves({"A": a, "B": b})
        assert list(aligned.index) == ["r1", "r2", "r3"]
        # B は r1 で未賭け → 0、r3 は直前値を維持
        assert aligned["B"].tolist() == [0.0, 50.0, 50.0]
        assert aligned["A"].tolist() == [-100.0, -100.0, 140.0]

    def test_align_empty(self):
        assert align_profit_curves({}).empty


class TestComparisonTable:
    def test_sorted_by_return_rate(self):
        results = {
            "v_low": {"return_rate": 0.8, "hit_rate": 0.1},
            "v_high": {"return_rate": 1.2, "hit_rate": 0.2},
        }
        table = comparison_table(results)
        assert list(table.index) == ["v_high", "v_low"]

    def test_empty(self):
        assert comparison_table({}).empty


def test_bet_policy_choices_resolve():
    """UI 選択肢の全 BetPolicy クラスが policies パッケージに存在する。"""
    import src.policies as policies

    for cls_name, action_key in BET_POLICY_CHOICES.values():
        cls = getattr(policies, cls_name)
        assert hasattr(cls, "judge")
        assert isinstance(action_key, str)
