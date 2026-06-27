"""ペアワイズ Elo レーティング（src/preprocessing/_ratings.py）のユニットテスト。

純粋関数（expected_score / parse_margin / margin_k / update_pairwise）と、
as-of 履歴ウォーク（compute_rating_history）のリーク無し性・shuffle 検証を扱う。
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from src.constants._feature_cols import ELO_BASE_K
from src.constants._feature_cols import ELO_FEATURE_COLS
from src.constants._feature_cols import ELO_INITIAL_RATING
from src.preprocessing._ratings import compute_rating_history
from src.preprocessing._ratings import elo_win_probabilities
from src.preprocessing._ratings import expected_score
from src.preprocessing._ratings import field_features
from src.preprocessing._ratings import margin_k
from src.preprocessing._ratings import parse_margin
from src.preprocessing._ratings import update_pairwise


# ──────────────────────────────────────────
# expected_score
# ──────────────────────────────────────────


def test_expected_score_equal_is_half():
    assert expected_score(1500, 1500) == pytest.approx(0.5)


def test_expected_score_monotonic_in_rating():
    """レーティングが高いほど勝利期待値が大きい。"""
    assert expected_score(1600, 1500) > 0.5
    assert expected_score(1400, 1500) < 0.5


def test_expected_score_symmetry():
    assert expected_score(1600, 1500) + expected_score(1500, 1600) == pytest.approx(1.0)


def test_expected_score_400_gap_is_10to1():
    # 400 点差 → 期待値 10/11 ≈ 0.909
    assert expected_score(1900, 1500) == pytest.approx(10.0 / 11.0, abs=1e-6)


# ──────────────────────────────────────────
# parse_margin
# ──────────────────────────────────────────


@pytest.mark.parametrize(
    "value,expected",
    [
        ("", 0.0),
        ("0", 0.0),
        ("クビ", 0.3),
        ("ハナ", 0.05),
        ("アタマ", 0.2),
        ("同着", 0.0),
        ("大差", 10.0),
        ("3", 3.0),
        ("2.5", 2.5),
        ("1/2", 0.5),
        ("3/4", 0.75),
        ("1.1/2", 1.5),
        ("1.3/4", 1.75),
        ("1.1/2馬身", 1.5),
    ],
)
def test_parse_margin_known_values(value, expected):
    assert parse_margin(value) == pytest.approx(expected)


def test_parse_margin_numeric_input_abs():
    assert parse_margin(-0.3) == pytest.approx(0.3)
    assert parse_margin(2) == pytest.approx(2.0)


def test_parse_margin_unparseable_is_nan():
    assert math.isnan(parse_margin(None))
    assert math.isnan(parse_margin(float("nan")))
    assert math.isnan(parse_margin("不明"))
    assert math.isnan(parse_margin("--"))


# ──────────────────────────────────────────
# margin_k
# ──────────────────────────────────────────


def test_margin_k_nan_returns_base():
    assert margin_k(ELO_BASE_K, float("nan")) == pytest.approx(ELO_BASE_K)
    assert margin_k(ELO_BASE_K, None) == pytest.approx(ELO_BASE_K)


def test_margin_k_zero_returns_base():
    assert margin_k(ELO_BASE_K, 0.0) == pytest.approx(ELO_BASE_K)


def test_margin_k_monotonic_increasing():
    """着差が大きいほど K が大きい（更新が強くなる）。"""
    k_small = margin_k(ELO_BASE_K, 0.5)
    k_mid = margin_k(ELO_BASE_K, 3.0)
    k_big = margin_k(ELO_BASE_K, 10.0)
    assert ELO_BASE_K <= k_small < k_mid < k_big


# ──────────────────────────────────────────
# update_pairwise
# ──────────────────────────────────────────


def test_update_pairwise_winner_gains_loser_loses():
    """同レーティングの2頭。勝者が上がり敗者が下がる（ゼロサム）。"""
    new = update_pairwise([1500.0, 1500.0], [1, 2], base_k=32.0)
    assert new[0] > 1500.0
    assert new[1] < 1500.0
    assert (new[0] - 1500.0) == pytest.approx(1500.0 - new[1])


def test_update_pairwise_dead_heat_no_change():
    """同着なら期待値 0.5 同士で変化しない。"""
    new = update_pairwise([1500.0, 1500.0], [1, 1], base_k=32.0)
    assert new[0] == pytest.approx(1500.0)
    assert new[1] == pytest.approx(1500.0)


def test_update_pairwise_upset_moves_more():
    """格下が格上に勝つ（番狂わせ）と更新量が大きい。"""
    # underdog(1400) beats favorite(1600)
    upset = update_pairwise([1400.0, 1600.0], [1, 2], base_k=32.0)
    # favorite(1600) beats underdog(1400) — 期待通り
    expected = update_pairwise([1600.0, 1400.0], [1, 2], base_k=32.0)
    assert (upset[0] - 1400.0) > (expected[0] - 1600.0)


def test_update_pairwise_single_horse_noop():
    assert update_pairwise([1500.0], [1], base_k=32.0) == [1500.0]


def test_update_pairwise_field_size_invariance():
    """更新量は全対戦相手平均（÷(n-1)）のためフィールドサイズに依存しにくい。

    全頭同レーティングで勝った馬の利得が、2頭立てでも8頭立てでも同じ K で
    過大にならない（平均化されている）ことを確認する。
    """
    win2 = update_pairwise([1500.0] * 2, [1, 2], base_k=32.0)[0] - 1500.0
    field8 = [1500.0] * 8
    win8 = update_pairwise(field8, [1, 2, 3, 4, 5, 6, 7, 8], base_k=32.0)[0] - 1500.0
    # 8頭立ての勝者は全員に勝つため2頭立てより大きいが、(n-1) 平均で抑制され
    # 17倍などには膨らまない（高々数倍以内）。
    assert 0 < win8 < win2 * 3


def test_update_pairwise_margin_increases_update():
    """大差勝ちの方が小差勝ちより更新が大きい。"""
    small = update_pairwise([1500.0, 1500.0], [1, 2], margins=[0.0, 0.3], base_k=32.0)
    big = update_pairwise([1500.0, 1500.0], [1, 2], margins=[0.0, 8.0], base_k=32.0)
    assert (big[0] - 1500.0) > (small[0] - 1500.0)


def test_update_pairwise_margin_nan_falls_back():
    """着差に NaN を含む対戦は K 一定にフォールバックする（margins 無しと一致）。"""
    with_nan = update_pairwise(
        [1500.0, 1500.0], [1, 2], margins=[0.0, float("nan")], base_k=32.0
    )
    without = update_pairwise([1500.0, 1500.0], [1, 2], base_k=32.0)
    assert with_nan[0] == pytest.approx(without[0])


# ──────────────────────────────────────────
# field_features / elo_win_probabilities
# ──────────────────────────────────────────


def test_field_features():
    fm, vs = field_features([1500.0, 1600.0, 1400.0])
    assert fm == pytest.approx(1500.0)
    assert vs == pytest.approx([0.0, 100.0, -100.0])


def test_elo_win_probabilities_sum_to_one_and_ordered():
    probs = elo_win_probabilities([1600.0, 1500.0, 1400.0])
    assert sum(probs) == pytest.approx(1.0)
    assert probs[0] > probs[1] > probs[2]


def test_elo_win_probabilities_equal_uniform():
    probs = elo_win_probabilities([1500.0, 1500.0, 1500.0, 1500.0])
    assert probs == pytest.approx([0.25] * 4)


# ──────────────────────────────────────────
# compute_rating_history（as-of リーク無し）
# ──────────────────────────────────────────


def _make_races(finish_by_race: list[tuple[str, str, list[tuple[str, int]]]]) -> pd.DataFrame:
    """(race_id, date, [(horse_id, 着順), ...]) のリストから results 風 DataFrame を作る。

    馬番は着順順に 1..n を割り当てる。race_id をインデックスにする。
    """
    rows = []
    for race_id, date, entrants in finish_by_race:
        for umaban, (hid, finish) in enumerate(entrants, start=1):
            rows.append(
                {
                    "race_id": race_id,
                    "date": date,
                    "horse_id": hid,
                    "馬番": umaban,
                    "着順": finish,
                }
            )
    return pd.DataFrame(rows).set_index("race_id")


def test_compute_rating_history_columns_and_index():
    df = _make_races(
        [
            ("R1", "2020-01-01", [("A", 1), ("B", 2), ("C", 3)]),
            ("R2", "2020-01-08", [("A", 1), ("B", 2), ("C", 3)]),
        ]
    )
    feats, snapshot = compute_rating_history(df)
    assert list(feats.columns) == list(ELO_FEATURE_COLS)
    # 行順・インデックスが入力と一致する
    assert feats.index.equals(df.index)
    assert len(feats) == len(df)
    assert set(snapshot) == {"A", "B", "C"}


def test_compute_rating_history_first_race_is_initial():
    """初出走時の elo_rating は初期値、elo_n_races は 0、field 内相対は 0。"""
    df = _make_races([("R1", "2020-01-01", [("A", 1), ("B", 2)])])
    feats, _ = compute_rating_history(df)
    assert (feats["elo_rating"] == ELO_INITIAL_RATING).all()
    assert (feats["elo_n_races"] == 0).all()
    assert feats["elo_vs_field"].abs().max() == pytest.approx(0.0)


def test_compute_rating_history_consistent_winner_rises():
    """毎回勝つ馬のスナップショットが他馬より高くなる。"""
    races = [
        (f"R{i}", f"2020-01-{i:02d}", [("A", 1), ("B", 2), ("C", 3)])
        for i in range(1, 11)
    ]
    df = _make_races(races)
    _, snapshot = compute_rating_history(df)
    assert snapshot["A"]["rating"] > snapshot["B"]["rating"] > snapshot["C"]["rating"]
    assert snapshot["A"]["n_races"] == 10
    assert snapshot["A"]["last_date"] == "2020-01-10"


def test_compute_rating_history_as_of_no_leak():
    """当該レースの着順を入れ替えても、そのレースで出力される elo_rating は不変。

    as-of 特徴量は「出走前」の値なので、当該レース結果に依存してはならない。
    （後続レースの特徴量は変わりうるが、当該レース行は変わらない）。
    """
    races_a = [
        ("R1", "2020-01-01", [("A", 1), ("B", 2)]),
        ("R2", "2020-01-08", [("A", 1), ("B", 2)]),
    ]
    races_b = [
        ("R1", "2020-01-01", [("A", 2), ("B", 1)]),  # R1 の着順だけ反転
        ("R2", "2020-01-08", [("A", 1), ("B", 2)]),
    ]
    feats_a, _ = compute_rating_history(_make_races(races_a))
    feats_b, _ = compute_rating_history(_make_races(races_b))

    # R1 の as-of レーティング（出走前）は着順反転に影響されない（両者初期値）。
    r1_a = feats_a.loc["R1", "elo_rating"]
    r1_b = feats_b.loc["R1", "elo_rating"]
    assert np.allclose(r1_a.to_numpy(), r1_b.to_numpy())
    # R2 の as-of レーティングは R1 の結果に依存するため変わる（リーク無しの裏返し）。
    r2_a = feats_a.loc["R2", "elo_rating"].to_numpy()
    r2_b = feats_b.loc["R2", "elo_rating"].to_numpy()
    assert not np.allclose(r2_a, r2_b)


def test_compute_rating_history_shuffle_destroys_signal():
    """着順をレース横断でシャッフルすると、馬間のレーティング分散が縮む。

    構造のあるデータ（A が常勝）では最終レーティングの分散が大きいが、
    着順をランダム化すると地力差が消え分散が大幅に縮小する（=リーク検査の代理）。
    """
    rng = np.random.default_rng(42)
    horses = [f"H{i}" for i in range(8)]
    structured_rows = []
    shuffled_rows = []
    for r in range(60):
        date = f"2020-{1 + r // 28:02d}-{1 + r % 28:02d}"
        race_id = f"R{r}"
        # structured: H0 best ... H7 worst（地力順）
        for umaban, hid in enumerate(horses, start=1):
            finish = horses.index(hid) + 1
            structured_rows.append(
                {"race_id": race_id, "date": date, "horse_id": hid, "馬番": umaban, "着順": finish}
            )
        # shuffled: 着順をランダムに割り当て
        order = rng.permutation(len(horses)) + 1
        for umaban, (hid, finish) in enumerate(zip(horses, order, strict=True), start=1):
            shuffled_rows.append(
                {"race_id": race_id, "date": date, "horse_id": hid, "馬番": umaban,
                 "着順": int(finish)}
            )

    structured = pd.DataFrame(structured_rows).set_index("race_id")
    shuffled = pd.DataFrame(shuffled_rows).set_index("race_id")

    _, snap_struct = compute_rating_history(structured)
    _, snap_shuf = compute_rating_history(shuffled)

    spread_struct = np.std([v["rating"] for v in snap_struct.values()])
    spread_shuf = np.std([v["rating"] for v in snap_shuf.values()])
    assert spread_struct > spread_shuf * 3


def test_datamerger_merge_horse_ratings_adds_columns():
    """DataMerger._merge_horse_ratings が merged_data に ELO 列を付与し snapshot を持つ。"""
    from src.preprocessing._data_merger import DataMerger

    rows = []
    for r in range(1, 4):
        rid = f"R{r}"
        for u, (h, f) in enumerate([("A", 1), ("B", 2), ("C", 3)], start=1):
            rows.append({"race_id": rid, "date": f"2020-01-0{r}", "horse_id": h,
                         "馬番": u, "着順": f, "n_horses": 3})
    md = pd.DataFrame(rows).set_index("race_id")

    obj = object.__new__(DataMerger)
    obj._merged_data = md
    obj.horse_ratings_snapshot = {}
    obj._merge_horse_ratings()

    for col in ELO_FEATURE_COLS:
        assert col in obj._merged_data.columns
    assert set(obj.horse_ratings_snapshot) == {"A", "B", "C"}
    # 付与後も行数・インデックスは保たれる
    assert len(obj._merged_data) == len(md)


def test_datamerger_merge_horse_ratings_skips_when_missing_cols():
    """必要列が無いときは no-op（クラッシュしない）。"""
    from src.preprocessing._data_merger import DataMerger

    obj = object.__new__(DataMerger)
    obj._merged_data = pd.DataFrame({"horse_id": ["A"]})  # 着順/馬番/date 欠落
    obj.horse_ratings_snapshot = {}
    obj._merge_horse_ratings()  # should not raise
    assert "elo_rating" not in obj._merged_data.columns


def test_shutuba_merger_live_ratings_from_snapshot(tmp_path, monkeypatch):
    """ShutubaDataMerger がスナップショット JSON から現行レーティングを付与する。"""
    import json

    from src.constants import _local_paths
    from src.preprocessing._shutuba_data_merger import ShutubaDataMerger

    snap = {
        "A": {"rating": 1600.0, "n_races": 10, "last_date": "2020-01-01"},
        "B": {"rating": 1400.0, "n_races": 5, "last_date": "2020-01-01"},
    }
    snap_path = tmp_path / "horse_ratings.json"
    snap_path.write_text(json.dumps(snap))
    monkeypatch.setattr(_local_paths.LocalPaths, "HORSE_RATINGS_PATH", str(snap_path))

    md = pd.DataFrame(
        {"horse_id": ["A", "B", "C"], "馬番": [1, 2, 3]},
        index=pd.Index(["R1", "R1", "R1"], name="race_id"),
    )
    obj = object.__new__(ShutubaDataMerger)
    obj._merged_data = md
    obj._merge_horse_ratings()

    out = obj._merged_data
    assert out.loc[out["horse_id"] == "A", "elo_rating"].iloc[0] == pytest.approx(1600.0)
    assert out.loc[out["horse_id"] == "B", "elo_rating"].iloc[0] == pytest.approx(1400.0)
    # 未知の馬 C は初期レーティングへフォールバック
    assert out.loc[out["horse_id"] == "C", "elo_rating"].iloc[0] == pytest.approx(
        ELO_INITIAL_RATING
    )
    # field 内相対が正しく計算される
    assert out["elo_field_mean"].nunique() == 1
    assert out.loc[out["horse_id"] == "A", "elo_vs_field"].iloc[0] > 0


def test_compute_rating_history_with_margin_column():
    """着差列があってもクラッシュせず、特徴量が生成される。"""
    rows = []
    for r in range(1, 6):
        rid = f"R{r}"
        date = f"2020-01-{r:02d}"
        entrants = [("A", 1, "0"), ("B", 2, "クビ"), ("C", 3, "1.1/2")]
        for umaban, (hid, finish, diff) in enumerate(entrants, start=1):
            rows.append({"race_id": rid, "date": date, "horse_id": hid,
                         "馬番": umaban, "着順": finish, "着差": diff})
    df = pd.DataFrame(rows).set_index("race_id")
    feats, snapshot = compute_rating_history(df)
    assert not feats["elo_rating"].isna().any()
    assert snapshot["A"]["rating"] > snapshot["C"]["rating"]
