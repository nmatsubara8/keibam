"""オッズ帯別 ROI 選定ポリシー（_odds_band_policy）のテスト。"""

import dataclasses

from src.policies._odds_band_policy import (
    DEFAULT_ODDS_BANDS,
    OddsBandPolicy,
    filter_candidates_by_odds,
    load_odds_band_policy,
    odds_band_label,
    save_odds_band_policy,
    select_profitable_bands,
)


@dataclasses.dataclass
class _Stat:
    """BetTypeStats のダック（roi / n_hits / roi_ex_top のみ使う）。"""
    roi: float
    n_hits: int
    roi_ex_top: float = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.roi_ex_top is None:
            self.roi_ex_top = self.roi


@dataclasses.dataclass
class _Cand:
    odds: float


def test_odds_band_label():
    assert odds_band_label(1.5) == "1-3"
    assert odds_band_label(5.0) == "3-7"
    assert odds_band_label(80.0) == "50-∞"
    assert odds_band_label(0.5) is None  # どの帯にも入らない


def test_select_requires_both_periods_above_floor():
    # 3-7 帯: train も val も ROI>=1.0 かつ的中十分 → 採用。
    # 7-15 帯: train は高 ROI だが val で floor 割れ → 不採用（後付け最適化を排除）。
    train = {"3-7": _Stat(1.10, 100), "7-15": _Stat(1.50, 100)}
    val = {"3-7": _Stat(1.05, 100), "7-15": _Stat(0.80, 100)}
    allowed = select_profitable_bands(train, val, DEFAULT_ODDS_BANDS, roi_floor=1.0)
    assert (3.0, 7.0) in allowed
    assert (7.0, 15.0) not in allowed


def test_select_rejects_low_hit_bands():
    # ROI は高いが的中が少ない帯（フロック）は両期間信頼水準未満で不採用。
    train = {"50-∞": _Stat(3.0, 1)}
    val = {"50-∞": _Stat(2.5, 1)}
    assert select_profitable_bands(train, val, roi_floor=1.0) == []


def test_select_rejects_flukey_roi_ex_top():
    # 生 ROI は floor 以上だが、最大払戻 1 本除外後は floor 割れ（万馬券依存）→ 不採用。
    train = {"15-50": _Stat(1.2, 100, roi_ex_top=0.7)}
    val = {"15-50": _Stat(1.1, 100, roi_ex_top=0.6)}
    assert select_profitable_bands(train, val, roi_floor=1.0, use_roi_ex_top=True) == []
    # roi_ex_top を見なければ採用され得る
    assert (15.0, 50.0) in select_profitable_bands(train, val, roi_floor=1.0, use_roi_ex_top=False)


def test_filter_candidates_by_odds():
    cands = [_Cand(2.0), _Cand(5.0), _Cand(100.0)]
    kept = filter_candidates_by_odds(cands, [(3.0, 7.0)])
    assert [c.odds for c in kept] == [5.0]
    # 採用帯が空なら買わない
    assert filter_candidates_by_odds(cands, []) == []


def test_policy_save_load_roundtrip(tmp_path):
    path = str(tmp_path / "odds_band_policy.json")
    policy = OddsBandPolicy(
        allowed_bands=((3.0, 7.0), (50.0, float("inf"))),
        roi_floor=1.0, train_years=(2021, 2022), val_years=(2023,),
        created_at="2026-07-21T00:00:00",
    )
    save_odds_band_policy(policy, path)
    loaded = load_odds_band_policy(path)
    assert loaded is not None
    assert loaded.allowed_bands == ((3.0, 7.0), (50.0, float("inf")))  # inf 復元
    assert loaded.roi_floor == 1.0
    # 適用ヘルパも動く
    kept = loaded.filter([_Cand(5.0), _Cand(2.0), _Cand(80.0)])
    assert sorted(c.odds for c in kept) == [5.0, 80.0]


def test_load_missing_returns_none(tmp_path):
    assert load_odds_band_policy(str(tmp_path / "nope.json")) is None
