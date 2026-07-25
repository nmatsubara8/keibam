"""Phase A: コース幾何 → per-race SimConfig 上書きの単体テスト。

固定するもの:
- スキーマ寛容: course_* 列が無い/全欠損なら CourseContext は空・cfg は base をそのまま返す（後方互換）。
- 前進安全（着順/単勝を使わない）は course_* が静的属性であることに依拠（本テストは幾何のみ与える）。
- 写像の向き: 狭い幅→有効幅↓、坂大→消耗↑、直線長→終盤到達↑、急コーナー→turn_k↑（base>0時）。
- 参照点で base を厳密再現（factor=1.0）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.simulation._agent_race import SimConfig
from src.simulation._course_env import (
    CourseContext,
    CourseEnvParams,
    course_context_from_featured,
    course_env_params_from_mapping,
    sim_config_for_course,
)


def _race(**cols) -> pd.DataFrame:
    n = len(next(iter(cols.values())))
    return pd.DataFrame(cols, index=["R"] * n)


# ---- CourseContext 解決 ----

def test_context_empty_when_no_course_columns():
    ctx = course_context_from_featured(_race(馬番=[1, 2, 3]))
    assert ctx.is_empty
    assert ctx.width is None and ctx.straight_length is None


def test_context_width_is_mean_of_min_max():
    df = _race(course_width_min=[25.0, 25.0], course_width_max=[27.0, 27.0],
               course_straight_length=[266.1, 266.1])
    ctx = course_context_from_featured(df)
    assert ctx.width == 26.0
    assert ctx.straight_length == 266.1
    assert not ctx.is_empty


def test_context_width_from_single_side():
    ctx = course_context_from_featured(_race(course_width_max=[41.0, 41.0]))
    assert ctx.width == 41.0


def test_context_first_finite_ignores_nan():
    df = _race(course_elevation_diff=[np.nan, 3.5, 3.5])
    ctx = course_context_from_featured(df)
    assert ctx.elevation_diff == 3.5


# ---- 後方互換・スキーマ寛容 ----

def test_empty_context_returns_base_unchanged():
    base = SimConfig()
    out = sim_config_for_course(base, CourseContext())
    assert out is base   # 新インスタンスすら作らない


def test_reference_course_reproduces_base():
    p = CourseEnvParams()
    ctx = CourseContext(width=p.width_ref, elevation_diff=p.elevation_ref,
                        straight_length=p.straight_ref)
    base = SimConfig()
    out = sim_config_for_course(base, ctx, p)
    assert out.course_width == pytest.approx(base.course_width)
    assert out.stamina_cost == pytest.approx(base.stamina_cost)
    assert out.closer_late == pytest.approx(base.closer_late)
    assert out.stalker_late == pytest.approx(base.stalker_late)


# ---- 写像の向き ----

def test_narrow_course_shrinks_effective_width():
    base = SimConfig()
    wide = sim_config_for_course(base, CourseContext(width=41.0))
    narrow = sim_config_for_course(base, CourseContext(width=25.0))
    assert narrow.course_width < base.course_width < wide.course_width


def test_hilly_course_raises_stamina_cost():
    base = SimConfig()
    flat = sim_config_for_course(base, CourseContext(elevation_diff=0.7))
    hill = sim_config_for_course(base, CourseContext(elevation_diff=5.3))
    assert flat.stamina_cost < base.stamina_cost < hill.stamina_cost


def test_long_straight_boosts_late_reach():
    base = SimConfig()
    short = sim_config_for_course(base, CourseContext(straight_length=266.1))
    long = sim_config_for_course(base, CourseContext(straight_length=525.9))
    assert short.closer_late < base.closer_late < long.closer_late
    # stalker は薄めに乗る（closer より変化幅が小さい）
    assert abs(long.stalker_late - base.stalker_late) < abs(long.closer_late - base.closer_late)


# ---- turn_k は opt-in（base>0 のときのみ幾何が効く）----

def test_turn_k_stays_zero_when_base_off():
    base = SimConfig()   # turn_k=0.0（既定）
    out = sim_config_for_course(base, CourseContext(corner_radius_large=0.0))
    assert out.turn_k == 0.0


def test_tight_corner_raises_turn_k_when_enabled():
    base = SimConfig(turn_k=0.01)
    tight = sim_config_for_course(base, CourseContext(corner_radius_large=0.0))
    loose = sim_config_for_course(base, CourseContext(corner_radius_large=1.0))
    assert loose.turn_k < base.turn_k < tight.turn_k


def test_spiral_reduces_turn_k_on_tight_corner():
    base = SimConfig(turn_k=0.01)
    no_spiral = sim_config_for_course(base, CourseContext(corner_radius_large=0.0, has_spiral=0.0))
    spiral = sim_config_for_course(base, CourseContext(corner_radius_large=0.0, has_spiral=1.0))
    assert spiral.turn_k < no_spiral.turn_k


# ---- clip の範囲（極端値でも物理レンジ内）----

def test_extreme_width_is_clipped():
    p = CourseEnvParams()
    base = SimConfig()
    huge = sim_config_for_course(base, CourseContext(width=1000.0), p)
    tiny = sim_config_for_course(base, CourseContext(width=0.1), p)
    assert huge.course_width == pytest.approx(base.course_width * p.width_hi)
    assert tiny.course_width == pytest.approx(base.course_width * p.width_lo)


def test_width_gain_zero_disables_width_effect():
    # width_gain=0 → 幅がどれだけ振れても有効幅は base のまま（校正で幅 knob を切れる）
    p = CourseEnvParams(width_gain=0.0)
    base = SimConfig()
    out = sim_config_for_course(base, CourseContext(width=25.0), p)
    assert out.course_width == pytest.approx(base.course_width)


def test_base_config_not_mutated():
    base = SimConfig()
    w0, sc0 = base.course_width, base.stamina_cost
    sim_config_for_course(base, CourseContext(width=25.0, elevation_diff=5.3))
    assert base.course_width == w0 and base.stamina_cost == sc0


# ---- 較正ゲイン復元（ce_* → CourseEnvParams）: calibrate_sim との共通経路 ----

def test_params_from_mapping_maps_ce_prefixed_gains():
    m = {"ce_straight_gain": 0.30, "ce_elevation_gain": 0.10, "turn_k": 0.02, "ability_sigma": 0.4}
    p = course_env_params_from_mapping(m)
    assert p is not None
    assert p.straight_gain == 0.30 and p.elevation_gain == 0.10
    assert p.width_gain == CourseEnvParams().width_gain   # 未指定は既定


def test_params_from_mapping_none_without_ce_keys():
    assert course_env_params_from_mapping({"turn_k": 0.02, "stamina_cost": 0.01}) is None


def test_calibrated_gains_change_mapping_strength():
    # ce_straight_gain を強めると同じコースでも closer_late がより伸びる（較正が効くことの担保）
    base = SimConfig()
    ctx = CourseContext(straight_length=525.9)
    weak = sim_config_for_course(base, ctx, course_env_params_from_mapping({"ce_straight_gain": 0.05}))
    strong = sim_config_for_course(base, ctx, course_env_params_from_mapping({"ce_straight_gain": 0.35}))
    assert base.closer_late < weak.closer_late < strong.closer_late
