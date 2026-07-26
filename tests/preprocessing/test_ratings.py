"""ペアワイズ Elo レーティング（preprocessing._ratings）の純粋ロジック・as-of リーク無し検証。"""

from __future__ import annotations

import pandas as pd

from src.constants._feature_cols import ELO_FEATURE_COLS
from src.preprocessing import _ratings as R


class TestExpectedScore:
    def test_equal_ratings_is_half(self):
        assert R.expected_score(1500.0, 1500.0) == 0.5

    def test_higher_rating_favored(self):
        assert R.expected_score(1600.0, 1400.0) > 0.5
        assert R.expected_score(1400.0, 1600.0) < 0.5

    def test_symmetry(self):
        a = R.expected_score(1600.0, 1450.0)
        b = R.expected_score(1450.0, 1600.0)
        assert abs((a + b) - 1.0) < 1e-9


class TestMarginMultiplier:
    def test_zero_or_negative_gap_is_one(self):
        assert R.margin_multiplier(0.0) == 1.0
        assert R.margin_multiplier(-3.0) == 1.0

    def test_monotone_increasing(self):
        assert R.margin_multiplier(0.5) < R.margin_multiplier(3.0)

    def test_capped(self):
        assert R.margin_multiplier(10_000.0) <= R.MARGIN_MULT_CAP + 1e-9


class TestFieldWinProbs:
    def test_sums_to_one(self):
        p = R.field_win_probs({1: 1500.0, 2: 1500.0, 3: 1600.0})
        assert abs(sum(p.values()) - 1.0) < 1e-9

    def test_higher_rating_higher_prob(self):
        p = R.field_win_probs({1: 1600.0, 2: 1400.0})
        assert p[1] > p[2]

    def test_empty(self):
        assert R.field_win_probs({}) == {}


class TestUpdateRace:
    def test_winner_gains_loser_loses(self):
        out = R.update_race({"a": 1500.0, "b": 1500.0}, ["a", "b"])
        assert out["a"] > 1500.0
        assert out["b"] < 1500.0

    def test_zero_sum_preserved(self):
        before = {"a": 1500.0, "b": 1500.0, "c": 1500.0}
        out = R.update_race(before, ["a", "b", "c"])
        assert abs(sum(out.values()) - sum(before.values())) < 1e-6

    def test_upset_moves_more_than_expected_win(self):
        # 格下(a=1400)が格上(b=1600)に先着＝大番狂わせ → a の上昇は大きい
        upset = R.update_race({"a": 1400.0, "b": 1600.0}, ["a", "b"])
        expected = R.update_race({"a": 1600.0, "b": 1400.0}, ["a", "b"])
        assert (upset["a"] - 1400.0) > (expected["a"] - 1600.0)

    def test_margin_amplifies(self):
        no_margin = R.update_race({"a": 1500.0, "b": 1500.0}, ["a", "b"])
        # b が大差負け（着差大）→ a の上昇が大きい
        with_margin = R.update_race(
            {"a": 1500.0, "b": 1500.0}, ["a", "b"], {"a": 0.0, "b": 8.0}
        )
        assert (with_margin["a"] - 1500.0) > (no_margin["a"] - 1500.0)

    def test_single_horse_noop(self):
        assert R.update_race({"a": 1500.0}, ["a"]) == {"a": 1500.0}

    def test_unknown_horse_starts_initial(self):
        out = R.update_race({}, ["a", "b"])
        # 両者初期値からスタート → 1着が上昇
        assert out["a"] > R.ELO_INITIAL > out["b"]


def _race(rid, date, rows):
    """rows: list of (馬番, 着順, horse_id[, 着差])。"""
    recs = []
    for r in rows:
        rec = {"race_id": rid, "date": date, "馬番": r[0], "着順": r[1], "horse_id": r[2]}
        if len(r) > 3:
            rec["着差"] = r[3]
        recs.append(rec)
    return recs


class TestBuildRatingFrame:
    def test_columns_and_first_race_is_neutral(self):
        df = pd.DataFrame(_race("R1", "2020-01-01", [(1, 1, "h1"), (2, 2, "h2")]))
        frame, snap = R.build_rating_frame(df)
        assert list(frame.columns) == ["race_id", "馬番", *ELO_FEATURE_COLS]
        # 初出走の特徴は出走前=中立（1500・対戦数0）
        assert (frame["elo_rating"] == R.ELO_INITIAL).all()
        assert (frame["elo_n_races"] == 0).all()

    def test_as_of_no_leak(self):
        # h1 は R1 で勝利。R1 の特徴は更新前(1500)、R2 の特徴は更新後(>1500) でなければならない。
        rows = _race("R1", "2020-01-01", [(1, 1, "h1"), (2, 2, "h2")])
        rows += _race("R2", "2020-02-01", [(1, 1, "h1"), (2, 2, "h3")])
        frame, snap = R.build_rating_frame(pd.DataFrame(rows))
        r1_h1 = frame[(frame["race_id"] == "R1")].iloc[0]
        r2 = frame[(frame["race_id"] == "R2")]
        r2_h1 = r2[r2["馬番"] == 1].iloc[0]
        assert r1_h1["elo_rating"] == R.ELO_INITIAL  # R1 出走前は中立
        assert r2_h1["elo_rating"] > R.ELO_INITIAL    # R1 勝利が R2 に反映
        assert r2_h1["elo_n_races"] == 1

    def test_chronological_order_independent_of_input_order(self):
        # 入力行が日付逆順でも、日付昇順で処理される（as-of 保証）
        rows = _race("R2", "2020-02-01", [(1, 1, "h1"), (2, 2, "h3")])
        rows += _race("R1", "2020-01-01", [(1, 1, "h1"), (2, 2, "h2")])
        frame, snap = R.build_rating_frame(pd.DataFrame(rows))
        r2 = frame[frame["race_id"] == "R2"]
        r2_h1 = r2[r2["馬番"] == 1].iloc[0]
        assert r2_h1["elo_rating"] > R.ELO_INITIAL

    def test_snapshot_tracks_n_races(self):
        rows = _race("R1", "2020-01-01", [(1, 1, "h1"), (2, 2, "h2")])
        rows += _race("R2", "2020-02-01", [(1, 1, "h1"), (2, 2, "h2")])
        _, snap = R.build_rating_frame(pd.DataFrame(rows))
        assert snap["h1"]["n_races"] == 2
        assert snap["h1"]["rating"] > snap["h2"]["rating"]

    def test_empty_input(self):
        frame, snap = R.build_rating_frame(pd.DataFrame())
        assert frame.empty
        assert snap == {}


class TestFeaturesFromSnapshot:
    def test_known_and_unknown_horses(self):
        snap = {"h1": {"rating": 1700.0, "n_races": 5}}
        feats = R.features_from_snapshot(["h1", "h2"], snap)
        assert feats["h1"]["elo_rating"] == 1700.0
        assert feats["h1"]["elo_n_races"] == 5.0
        assert feats["h2"]["elo_rating"] == R.ELO_INITIAL  # 初出走は中立
        assert feats["h1"]["elo_vs_field"] > 0
        assert abs(sum(f["elo_win_prob"] for f in feats.values()) - 1.0) < 1e-9
