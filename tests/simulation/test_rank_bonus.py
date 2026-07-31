"""騎手＋厩舎ランク → rank_bonus と物理シム ability 加減点のテスト（③ leak承知）。"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.simulation._rank_bonus import attach_rank_bonus, build_rank_z


def test_build_rank_z_standardizes_and_maps_id():
    rank_df = pd.DataFrame({"person_code": ["1181", "1096", "1160"], "rank": [12, 2, 7]})
    code_to_id = {"1181": "5001", "1096": "5002", "1160": "5003"}
    z = build_rank_z(rank_df, code_to_id=code_to_id)
    assert set(z) == {"5001", "5002", "5003"}          # netkeiba id をキーに
    assert abs(np.mean(list(z.values()))) < 1e-9        # 平均0
    assert z["5001"] > z["5003"] > z["5002"]            # rank 12>7>2 の順（符号は rank_gain で吸収）


def test_build_rank_z_without_crosswalk_uses_person_code():
    z = build_rank_z(pd.DataFrame({"person_code": ["a", "b"], "rank": [1, 3]}))
    assert set(z) == {"a", "b"}


def test_build_rank_z_empty():
    assert build_rank_z(pd.DataFrame()) == {}


def test_attach_rank_bonus_sums_jockey_and_trainer():
    featured = pd.DataFrame({"jockey_id": ["5001", "5002", "9999"],
                             "trainer_id": ["7001", "7002", "7001"], "x": [1, 2, 3]})
    jz = {"5001": 1.0, "5002": -1.0}          # 9999 は欠損→0
    tz = {"7001": 0.5, "7002": -0.5}
    out = attach_rank_bonus(featured, jz, tz)
    assert out["rank_bonus"].tolist() == [1.5, -1.5, 0.5]   # 合算・欠損0
    assert "x" in out.columns                                 # 元列保持


def test_attach_rank_bonus_missing_columns_safe():
    out = attach_rank_bonus(pd.DataFrame({"a": [1]}), {"x": 1.0}, {"y": 2.0})
    assert out["rank_bonus"].tolist() == [0.0]                # id 列なし→全0


def test_build_live_field_uses_conservative_fixed_gain():
    # ① live 経路: 既定は RANK_GAIN_LIVE（保守的固定）。過去データで調整しない。
    from src.simulation._rank_bonus import RANK_GAIN_LIVE, build_live_field
    from src.simulation._sim_params import field_from_featured

    race = pd.DataFrame({"馬番": [1, 2, 3], "speed_figure": [1.0, 0.0, -1.0],
                         "rank_bonus": [1.5, 0.0, -1.5]})
    live = build_live_field(race)
    ref = field_from_featured(race, rank_gain=RANK_GAIN_LIVE)
    assert np.allclose(live.ability, ref.ability)          # 固定 gain と一致
    base = field_from_featured(race, rank_gain=0.0)
    assert live.ability[0] > base.ability[0]               # 正の rank_bonus は加点
    assert 0.0 < RANK_GAIN_LIVE <= 0.10                    # 保守的レンジ


def test_assert_live_only_flags_backtest_misuse():
    from src.simulation._rank_bonus import assert_live_only
    assert assert_live_only(0.0) is False                  # 0 は方針遵守（違反なし）
    assert assert_live_only(0.05, context="test") is True  # 非0 は違反として True


def test_field_from_featured_applies_rank_gain():
    from src.simulation._sim_params import field_from_featured

    race = pd.DataFrame({
        "馬番": [1, 2, 3],
        "speed_figure": [1.0, 0.0, -1.0],       # _ability_z の素（あれば）
        "rank_bonus": [1.0, 0.0, -1.0],
    })
    base = field_from_featured(race, rank_gain=0.0)
    boosted = field_from_featured(race, rank_gain=0.3)
    # rank_gain>0 で rank_bonus 正の馬は ability が上がり、負の馬は下がる（clip 内）
    assert boosted.ability[0] > base.ability[0]
    assert boosted.ability[2] < base.ability[2]
    assert boosted.ability[1] == base.ability[1]             # rank_bonus 0 は不変
