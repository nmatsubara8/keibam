"""時系列オッズ自動再計算ウォッチャー（odds_watch）のテスト（スタブソース・tmp 永続化）。"""

import datetime as dt
import os

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _isolate_db(tmp_path, monkeypatch):
    """persist_predictions → RawDataRepo が実 DB (data/keibam.db) を汚染しないよう隔離。"""
    import src.storage
    from src.storage import RawDataRepo

    db_path = os.path.join(tmp_path, "isolated.db")

    class _TmpRepo(RawDataRepo):
        def __init__(self):
            super().__init__(db_path)

    monkeypatch.setattr(src.storage, "RawDataRepo", _TmpRepo)

from src.constants._odds_phases import OddsPhase
from src.preparing.odds_scheduler import select_checkpoint_races
from src.pipeline.odds_watch import PREDICTION_COLUMNS
from src.pipeline.odds_watch import latest_final_odds_lookup
from src.pipeline.odds_watch import load_predictions
from src.pipeline.odds_watch import persist_predictions
from src.pipeline.odds_watch import recalculate_predictions


class TestSelectCheckpointRaces:
    def test_captures_every_race_within_30min(self):
        # 発走30分前から毎ティック（cron */3 で3分おき）。30〜0分前は全て取得。
        now = dt.datetime(2026, 6, 7, 15, 0)
        pairs = [
            ("r30", dt.datetime(2026, 6, 7, 15, 30)),  # 30 分前 → thirty_min
            ("r20", dt.datetime(2026, 6, 7, 15, 20)),  # 20 分前 → thirty_min（旧仕様では対象外だった）
            ("r10", dt.datetime(2026, 6, 7, 15, 10)),  # 10 分前 → t10
            ("r05", dt.datetime(2026, 6, 7, 15, 5)),   # 5 分前 → t5
            ("r01", dt.datetime(2026, 6, 7, 15, 1)),   # 1 分前 → t0
        ]
        by_id = {rid: phase for rid, _, phase in select_checkpoint_races(pairs, now)}
        assert by_id == {
            "r30": OddsPhase.THIRTY_MIN,
            "r20": OddsPhase.THIRTY_MIN,
            "r10": OddsPhase.T10,
            "r05": OddsPhase.T5,
            "r01": OddsPhase.T0,
        }

    def test_over_30min_not_taken(self):
        now = dt.datetime(2026, 6, 7, 15, 0)
        # 31 分前は密ウィンドウ(30)外、早期 sparse も既定で無効 → 取得しない。
        assert select_checkpoint_races([("r", dt.datetime(2026, 6, 7, 15, 31))], now) == []

    def test_grace_window_continues_past_post(self):
        # 実締切の安全弁: 予定発走を過ぎても POST_GRACE_MIN(=10)分まで継続。負値の phase は T0。
        now = dt.datetime(2026, 6, 7, 15, 0)
        # 5 分前に発走済み（mtp=-5）→ 猶予内で取得継続
        t = select_checkpoint_races([("r", dt.datetime(2026, 6, 7, 14, 55))], now)
        assert len(t) == 1 and t[0][2] == OddsPhase.T0
        # ちょうど +10 分（猶予境界）→ 取得
        assert len(select_checkpoint_races([("r", dt.datetime(2026, 6, 7, 14, 50))], now)) == 1
        # +11 分（猶予超過）→ 取得しない
        assert select_checkpoint_races([("r", dt.datetime(2026, 6, 7, 14, 49))], now) == []

    def test_confirmed_races_skipped(self):
        now = dt.datetime(2026, 6, 7, 15, 0)
        pairs = [("r1", dt.datetime(2026, 6, 7, 15, 10)), ("r2", dt.datetime(2026, 6, 7, 15, 10))]
        out = select_checkpoint_races(pairs, now, confirmed={"r1"})
        assert [rid for rid, _, _ in out] == ["r2"]  # 確定済み r1 は除外


def _make_snapshots(race_id="202606070511"):
    """thirty_min と t10 の 2 時点 × 4 頭のスナップショット。"""
    from src.constants._bet_types import BetType
    from src.preparing._odds_snapshot import make_snapshot

    post = dt.datetime(2026, 6, 7, 15, 40)
    snaps = []
    for umaban, (o30, o10) in enumerate(
        [(2.0, 1.8), (4.0, 4.5), (8.0, 9.0), (12.0, 11.0)], start=1
    ):
        snaps.append(make_snapshot(race_id, BetType.TANSHO, [umaban], o30, post,
                                   dt.datetime(2026, 6, 7, 15, 10)))
        snaps.append(make_snapshot(race_id, BetType.TANSHO, [umaban], o10, post,
                                   dt.datetime(2026, 6, 7, 15, 30)))
    return snaps


class TestRecalculatePredictions:
    def test_produces_rows_for_all_models(self, tmp_path):
        snaps = _make_snapshots()
        now = dt.datetime(2026, 6, 7, 15, 30)
        preds = recalculate_predictions(snaps, ["202606070511"], now, models_dir=str(tmp_path))
        assert not preds.empty
        assert set(preds.columns) == set(PREDICTION_COLUMNS)
        assert {"identity", "dirichlet", "kalman", "particle", "ensemble"} <= set(preds["model"])
        # チェックポイントは最新観測フェーズ（t10）
        assert (preds["checkpoint"] == OddsPhase.T10).all()
        # シェアはレース・モデルごとに Σ=1
        for _, grp in preds.groupby("model"):
            assert grp["pred_final_share"].sum() == pytest.approx(1.0, abs=1e-6)

    def test_unknown_race_yields_empty(self, tmp_path):
        preds = recalculate_predictions(_make_snapshots(), ["nope"], dt.datetime.now(), models_dir=str(tmp_path))
        assert preds.empty


class TestPersistence:
    def test_roundtrip_and_replace(self, tmp_path):
        path = str(tmp_path / "odds_predictions.pkl")
        snaps = _make_snapshots()
        now = dt.datetime(2026, 6, 7, 15, 30)
        preds = recalculate_predictions(snaps, ["202606070511"], now, models_dir=str(tmp_path))

        persist_predictions(preds, path)
        loaded = load_predictions(path)
        assert len(loaded) == len(preds)

        # 同一 (race, checkpoint, model, umaban) の再計算は置き換え（重複しない）
        persist_predictions(preds, path)
        assert len(load_predictions(path)) == len(preds)

    def test_load_missing_returns_empty_frame(self, tmp_path):
        df = load_predictions(str(tmp_path / "nope.pkl"))
        assert df.empty
        assert list(df.columns) == PREDICTION_COLUMNS


class TestLatestFinalOddsLookup:
    def test_latest_per_race_horse(self):
        df = pd.DataFrame(
            [
                {"race_id": "r1", "umaban": "1", "model": "ensemble",
                 "predicted_at": "2026-06-07T15:10", "pred_final_odds": 2.5},
                {"race_id": "r1", "umaban": "1", "model": "ensemble",
                 "predicted_at": "2026-06-07T15:35", "pred_final_odds": 2.2},
                {"race_id": "r1", "umaban": "1", "model": "kalman",
                 "predicted_at": "2026-06-07T15:36", "pred_final_odds": 9.9},
            ]
        )
        lookup = latest_final_odds_lookup(df, model="ensemble")
        assert lookup == {("r1", 1): 2.2}

    def test_empty(self):
        assert latest_final_odds_lookup(pd.DataFrame()) == {}
        assert latest_final_odds_lookup(None) == {}


class TestRunOnce:
    def test_full_cycle_with_stub_source(self, tmp_path, monkeypatch):
        """スタブソースで 取得 → 永続化 → 再計算 → 予測保存 の 1 サイクルを検証。"""
        from src.constants._local_paths import LocalPaths
        from src.pipeline.odds_watch import run_once

        snap_path = str(tmp_path / "odds_snapshots.pkl")
        pred_path = str(tmp_path / "odds_predictions.pkl")
        monkeypatch.setattr(LocalPaths, "RAW_ODDS_SNAPSHOT_PATH", snap_path, raising=False)
        monkeypatch.setattr(LocalPaths, "RAW_ODDS_PREDICTIONS_PATH", pred_path, raising=False)

        now = dt.datetime(2026, 6, 7, 15, 10)  # r1 の 30 分前

        class _StubSource:
            def fetch_today_races(self, date_str):
                return [("202606070511", dt.datetime(2026, 6, 7, 15, 40))]

            def fetch_win_odds(self, race_id):
                return [(1, 2.0), (2, 4.0), (3, 8.0)]

            def close(self):
                pass

        result = run_once(_StubSource(), now=now)
        assert result["n_targets"] == 1
        assert result["n_snapshots"] == 3
        assert result["n_predictions"] > 0

        preds = load_predictions(pred_path)
        assert not preds.empty
        assert (preds["checkpoint"] == OddsPhase.THIRTY_MIN).all()

    def test_no_checkpoint_is_cheap_noop(self, tmp_path, monkeypatch):
        from src.constants._local_paths import LocalPaths
        from src.pipeline.odds_watch import run_once

        monkeypatch.setattr(LocalPaths, "RAW_ODDS_SNAPSHOT_PATH", str(tmp_path / "s.pkl"), raising=False)
        monkeypatch.setattr(LocalPaths, "RAW_ODDS_PREDICTIONS_PATH", str(tmp_path / "p.pkl"), raising=False)

        now = dt.datetime(2026, 6, 7, 14, 0)  # チェックポイント外

        class _StubSource:
            def fetch_today_races(self, date_str):
                return [("r1", dt.datetime(2026, 6, 7, 15, 40))]

            def fetch_win_odds(self, race_id):
                raise AssertionError("チェックポイント外では取得しない")

            def close(self):
                pass

        result = run_once(_StubSource(), now=now)
        assert result["n_targets"] == 0
        assert result["n_predictions"] == 0

    def test_empty_odds_past_post_marks_confirmed(self, tmp_path, monkeypatch):
        """ライブ発走を過ぎてオッズが空（撤去）→ 締切確定として confirmed に追加。"""
        from src.constants._local_paths import LocalPaths
        from src.pipeline.odds_watch import run_once

        monkeypatch.setattr(LocalPaths, "RAW_ODDS_SNAPSHOT_PATH", str(tmp_path / "s.pkl"), raising=False)
        monkeypatch.setattr(LocalPaths, "RAW_ODDS_PREDICTIONS_PATH", str(tmp_path / "p.pkl"), raising=False)
        now = dt.datetime(2026, 6, 7, 15, 42)  # 発走(15:40)+2分

        class _StubSource:
            def fetch_today_races(self, date_str):
                return [("r1", dt.datetime(2026, 6, 7, 15, 40))]

            def fetch_win_odds(self, race_id):
                return []  # 締切後＝オッズ撤去

            def close(self):
                pass

        confirmed: set[str] = set()
        run_once(_StubSource(), now=now, confirmed=confirmed)
        assert "r1" in confirmed

    def test_empty_odds_before_post_not_confirmed(self, tmp_path, monkeypatch):
        """発走前（遅延中）にオッズが一時的に空でも確定扱いしない（mtp>0）。"""
        from src.constants._local_paths import LocalPaths
        from src.pipeline.odds_watch import run_once

        monkeypatch.setattr(LocalPaths, "RAW_ODDS_SNAPSHOT_PATH", str(tmp_path / "s.pkl"), raising=False)
        monkeypatch.setattr(LocalPaths, "RAW_ODDS_PREDICTIONS_PATH", str(tmp_path / "p.pkl"), raising=False)
        now = dt.datetime(2026, 6, 7, 15, 38)  # 発走2分前（mtp=+2）

        class _StubSource:
            def fetch_today_races(self, date_str):
                return [("r1", dt.datetime(2026, 6, 7, 15, 40))]

            def fetch_win_odds(self, race_id):
                return []

            def close(self):
                pass

        confirmed: set[str] = set()
        result = run_once(_StubSource(), now=now, confirmed=confirmed)
        assert "r1" not in confirmed and result["n_targets"] == 1
