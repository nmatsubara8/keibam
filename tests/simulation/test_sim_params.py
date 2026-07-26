"""Phase2: field_from_featured（featured→RaceField）の単体テスト。

スキーマ寛容（列欠落で中立既定）・前進安全（着順/単勝を使わない）・能力の向き
（強いシグナル→高 ability）・脚質マップを固定する。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from src.simulation._agent_race import STYLE_CLOSER, STYLE_FRONT, STYLE_STALKER
from src.simulation._sim_params import field_from_featured


def test_schema_tolerant_defaults():
    # 能力シグナルが1つも無い → ability≈1.0 中立、style=stalker、stamina=1.0
    df = pd.DataFrame({"馬番": [1, 2, 3]}, index=["R", "R", "R"])
    f = field_from_featured(df)
    assert f.n == 3
    assert np.allclose(f.ability, 1.0)
    assert list(f.style) == [STYLE_STALKER] * 3
    assert np.allclose(f.stamina, 1.0)


def test_ability_follows_speed_figure():
    df = pd.DataFrame(
        {"speed_fig_best": [80.0, 60.0, 40.0], "win_rate_5R": [0.3, 0.1, 0.0]},
        index=["R", "R", "R"],
    )
    f = field_from_featured(df, ability_spread=0.3)
    assert f.ability[0] > f.ability[1] > f.ability[2]     # 強い馬ほど高能力


def test_style_from_leg_type():
    df = pd.DataFrame(
        {"leg_type_binary": [0.0, 1.0, np.nan], "speed_fig_best": [1.0, 1.0, 1.0]},
        index=["R", "R", "R"],
    )
    f = field_from_featured(df)
    assert f.style[0] == STYLE_FRONT      # 0.0<0.4 → 先行
    assert f.style[1] == STYLE_CLOSER     # 1.0>0.6 → 追込
    assert f.style[2] == STYLE_STALKER    # NaN → 差し既定


def test_does_not_use_rank_or_odds():
    # 着順・単勝を入れても ability は変わらない（前進安全＝結果を入力にしない）
    base = pd.DataFrame({"speed_fig_best": [70.0, 50.0]}, index=["R", "R"])
    leak = base.copy()
    leak["着順"] = [1, 2]
    leak["単勝"] = [1.5, 9.0]
    a = field_from_featured(base)
    b = field_from_featured(leak)
    assert np.allclose(a.ability, b.ability)


def test_end_to_end_smoke_beats_random_ordering():
    # field_from_featured → monte_carlo が能力順の勝率を返す（配線の健全性）
    from src.simulation._agent_race import monte_carlo
    df = pd.DataFrame(
        {"speed_fig_best": [90.0, 70.0, 50.0, 30.0]},
        index=["R"] * 4,
    )
    f = field_from_featured(df, ability_spread=0.35)
    r = monte_carlo(f, n_sim=1200, seed=2)
    assert r["mean_rank"][0] < r["mean_rank"][-1]        # 最強馬の平均着順 < 最弱馬
