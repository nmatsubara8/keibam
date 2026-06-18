"""モデル比較シミュレーション ヘルパ（app._model_compare）のテスト。"""

import pandas as pd

from app._model_compare import BET_POLICY_CHOICES
from app._model_compare import align_profit_curves
from app._model_compare import comparison_table
from app._model_compare import cumulative_profit
from app._model_compare import recent_race_slice
from app._model_compare import simulate_model


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


def test_bet_policy_choices_cover_all_eight_bet_types():
    """単勝以外を含む全 8 券種が UI 選択肢に揃っている。"""
    action_keys = {key for _, key in BET_POLICY_CHOICES.values()}
    assert action_keys == {
        "tansho", "fukusho", "wakuren", "umaren",
        "umatan", "wide", "sanrenpuku", "sanrentan",
    }


# ---------------------------------------------------------------------------
# simulate_model の券種別 E2E（合成スコア表 + 合成払戻テーブルを DI）
# ---------------------------------------------------------------------------

from src.constants._bet_types import BetType  # noqa: E402
from src.constants._results_cols import ResultsCols  # noqa: E402


class _FakeAI:
    """calc_score が固定スコア表を返すスタブ KeibaAI。"""

    def __init__(self, score_table: pd.DataFrame) -> None:
        self._score_table = score_table

    def calc_score(self, X, policy):  # noqa: ANN001 - policy は無視
        return self._score_table


class _FakeReturnProcessor:
    """preprocessed_data だけを持つ ReturnProcessor 互換スタブ。"""

    def __init__(self, tables: dict) -> None:
        self.preprocessed_data = tables


_RID = 202601010101


def _score_table_top3() -> pd.DataFrame:
    """1 レース・5 頭。馬番1-3 が高スコア（枠も 1-3）で閾値超え。"""
    df = pd.DataFrame(
        {
            "score": [2.0, 1.8, 1.5, -1.0, -1.2],
            ResultsCols.UMABAN: [1, 2, 3, 4, 5],
            ResultsCols.WAKUBAN: [1, 2, 3, 4, 5],
            "wakuban_flag": [1, 1, 1, 1, 1],
        },
        index=[str(_RID)] * 5,
    )
    df.index.name = "race_id"
    return df


def _return_tables_123_hit() -> dict:
    """着順 1-2-3 = 馬番/枠 1-2-3 が的中する合成払戻テーブル。"""
    def _tbl(cols: dict) -> pd.DataFrame:
        return pd.DataFrame([cols], index=[_RID])

    return {
        BetType.TANSHO: _tbl({"win_0": 1, "return_0": 200}),
        BetType.FUKUSHO: _tbl({"win_0": 1, "return_0": 110, "win_1": 2, "return_1": 120,
                               "win_2": 3, "return_2": 130}),
        BetType.WAKUREN: _tbl({"win_0": (1, 2), "return_0": 600}),
        BetType.UMAREN: _tbl({"win_0": (1, 2), "return_0": 500}),
        BetType.UMATAN: _tbl({"win_0": (1, 2), "return_0": 900}),
        BetType.WIDE: _tbl({"win_0": (1, 2), "return_0": 300, "win_1": (1, 3), "return_1": 250,
                            "win_2": (2, 3), "return_2": 280}),
        BetType.SANRENPUKU: _tbl({"win_0": (1, 2, 3), "return_0": 2000}),
        BetType.SANRENTAN: _tbl({"win_0": (1, 2, 3), "return_0": 9000}),
    }


import pytest  # noqa: E402


@pytest.mark.parametrize("bet_label", list(BET_POLICY_CHOICES.keys()))
def test_simulate_model_all_bet_types_end_to_end(bet_label):
    """全 8 券種で simulate_model が成立し、的中レースで回収率>0 を返す。"""
    ai = _FakeAI(_score_table_top3())
    rp = _FakeReturnProcessor(_return_tables_123_hit())

    summary, per_race, diag = simulate_model(
        ai, pd.DataFrame(), bet_label, threshold=1.0, return_processor=rp
    )

    assert diag["n_matched_races"] == 1
    assert diag["n_covered_races"] == 1
    assert summary, f"{bet_label}: summary が空"
    assert summary["n_bets"] >= 1
    # 1-2-3 が的中する設計なので回収（払戻>0）が発生する
    assert summary["return_rate"] > 0
    assert not per_race.empty


def test_simulate_model_high_threshold_no_bets():
    """閾値が高すぎて 1 頭も選ばれない場合は summary 空・matched=0。"""
    ai = _FakeAI(_score_table_top3())
    rp = _FakeReturnProcessor(_return_tables_123_hit())
    summary, per_race, diag = simulate_model(
        ai, pd.DataFrame(), "三連複BOX", threshold=5.0, return_processor=rp
    )
    assert diag["n_matched_races"] == 0
    assert summary == {}
