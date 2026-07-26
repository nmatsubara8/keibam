"""2ヘッド+確定オッズ 券種別バックテストの UI アダプタ（app._two_head_backtest）のテスト。"""

import pandas as pd

from app import _two_head_backtest as thb
from src.constants._bet_types import BetType
from src.simulation._backtest import BetTypeStats


def _featured():
    rows = []
    for rid in ["202301010101", "202301010102", "202401010101", "202501010101"]:
        for h in range(2):
            rows.append({"race_id": rid, "x": h})
    return pd.DataFrame(rows).set_index("race_id")


class TestAvailableYears:
    def test_newest_first_and_unique(self):
        assert thb.available_years(_featured()) == ["2025", "2024", "2023"]

    def test_empty(self):
        assert thb.available_years(pd.DataFrame()) == []
        assert thb.available_years(None) == []


class TestFilterByYears:
    def test_filter_single_year(self):
        out = thb.filter_featured_by_years(_featured(), ["2023"])
        assert set(out.index.astype(str).str[:4]) == {"2023"}
        assert len(out) == 4  # 2 races × 2 horses

    def test_multiple_years(self):
        out = thb.filter_featured_by_years(_featured(), ["2024", "2025"])
        assert set(out.index.astype(str).str[:4]) == {"2024", "2025"}

    def test_empty_years_noop(self):
        df = _featured()
        assert thb.filter_featured_by_years(df, None) is df
        assert thb.filter_featured_by_years(df, []) is df


class TestSelectableBetTypes:
    def test_excludes_wakuren_and_is_ordered(self):
        bts = thb.selectable_bet_types()
        assert BetType.WAKUREN not in bts
        assert bts[0] == BetType.TANSHO
        assert BetType.SANRENTAN in bts


class TestResolveThresholds:
    def test_default_all(self):
        th = thb.resolve_thresholds(None)
        assert BetType.TANSHO in th and BetType.WAKUREN not in th

    def test_filtered_subset(self):
        th = thb.resolve_thresholds([BetType.TANSHO, BetType.WIDE])
        assert set(th.keys()) == {BetType.TANSHO, BetType.WIDE}


class TestResultToFrame:
    def _result(self):
        per = {
            BetType.TANSHO: BetTypeStats(BetType.TANSHO, n_bets=10, n_hits=3, stake=10, returned=12),
            BetType.WIDE: BetTypeStats(BetType.WIDE, n_bets=4, n_hits=1, stake=4, returned=2),
        }
        overall = BetTypeStats("ALL", n_bets=14, n_hits=4, stake=14, returned=14)
        return {"per_bet_type": per, "overall": overall, "n_races": 5, "n_candidates": 14}

    def test_rows_and_labels(self):
        frame = thb.result_to_frame(self._result())
        # 単勝・ワイド + 全体行
        assert list(frame["馬券種"]) == ["単勝", "ワイド", "全体"]
        assert list(frame.columns) == [
            "馬券種", "点数", "的中", "的中率", "投票", "払戻", "損益", "回収率",
        ]

    def test_metrics_values(self):
        frame = thb.result_to_frame(self._result())
        tansho = frame[frame["馬券種"] == "単勝"].iloc[0]
        assert tansho["回収率"] == 12 / 10
        assert tansho["的中率"] == 3 / 10
        assert tansho["損益"] == 2

    def test_empty_when_no_bets(self):
        frame = thb.result_to_frame({"per_bet_type": {}, "overall": BetTypeStats("ALL")})
        assert frame.empty


class _FakeAI:
    def __init__(self, tag):
        self.effective_model = tag


class TestRunTwoHeadBacktest:
    def test_wiring(self, monkeypatch):
        captured = {}

        def fake_run_backtest(place_model, X, rp, **kwargs):
            captured["place_model"] = place_model
            captured["X"] = X
            captured["kwargs"] = kwargs
            return {
                "per_bet_type": {
                    BetType.TANSHO: BetTypeStats(BetType.TANSHO, n_bets=2, n_hits=1, stake=2, returned=3),
                },
                "overall": BetTypeStats("ALL", n_bets=2, n_hits=1, stake=2, returned=3),
                "n_races": 1,
                "n_candidates": 2,
            }

        monkeypatch.setattr(thb, "run_backtest", fake_run_backtest)

        result = thb.run_two_head_backtest(
            _FakeAI("place"),
            _featured(),
            return_processor="rp",
            win_ai=_FakeAI("win"),
            bet_types=[BetType.TANSHO],
            years=["2023"],
        )

        # effective_model が抽出されて渡る
        assert captured["place_model"] == "place"
        assert captured["kwargs"]["win_model"] == "win"
        # years フィルタが適用されている
        assert set(captured["X"].index.astype(str).str[:4]) == {"2023"}
        # 券種絞り込みが thresholds に反映
        assert set(captured["kwargs"]["thresholds"].keys()) == {BetType.TANSHO}
        # 表示用 frame が添えられる
        assert not result["frame"].empty
        assert list(result["frame"]["馬券種"]) == ["単勝", "全体"]

    def test_no_win_head(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            thb, "run_backtest",
            lambda *a, **k: captured.update(k) or {"per_bet_type": {}, "overall": BetTypeStats("ALL")},
        )
        thb.run_two_head_backtest(_FakeAI("place"), _featured(), "rp", win_ai=None)
        assert captured["win_model"] is None
