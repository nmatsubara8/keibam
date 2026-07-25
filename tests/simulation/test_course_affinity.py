"""Phase B: 出走馬 × コース相性 → RaceField.ability 馬別補正の単体テスト。

固定するもの:
- スキーマ寛容: プロファイル未知/馬側特徴欠損なら base field をそのまま返す（後方互換）。
- 前進安全は course_*(静的) × as-of 特徴(脚質/スピード)に依拠（本テストは相性入力のみ与える）。
- 写像の向き: 前有利コース×先行馬→ability↑/×追込馬→↓、高速馬場×速い馬→↑/タフ×速い馬→↓。
- 参照点中立（bias=0 or ゲイン0 → base 厳密再現）・clip・base 非破壊・ca_* マッピング。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.simulation._agent_race import RaceField
from src.simulation._course_affinity import (
    CourseAffinityParams,
    course_affinity_params_from_mapping,
    field_for_course,
)
from src.simulation._course_env import CourseContext


def _field(n, ability=1.0):
    return RaceField(ability=np.full(n, ability), style=np.zeros(n, dtype=int),
                     stamina=np.ones(n), noise=np.full(n, 0.04))


def _race(**cols):
    n = len(next(iter(cols.values())))
    return pd.DataFrame(cols, index=["R"] * n)


# ---- 後方互換・スキーマ寛容 ----

def test_no_profile_returns_base_field():
    f = _field(3)
    out = field_for_course(f, _race(leg_type_binary=[0.0, 0.5, 1.0]), CourseContext())
    assert out is f   # 相性入力なし＝新インスタンスすら作らない


def test_zero_bias_reproduces_base():
    f = _field(3)
    df = _race(leg_type_binary=[0.0, 0.5, 1.0], speed_fig_best=[70, 60, 50])
    out = field_for_course(f, df, CourseContext(run_style_bias=0.0, time_bias=0.0))
    assert out is f


def test_missing_horse_feature_is_neutral():
    # run_style_bias はあるが leg_type 列が無い → 補正できず base のまま
    f = _field(3)
    out = field_for_course(f, _race(馬番=[1, 2, 3]), CourseContext(run_style_bias=1.0))
    assert out is f


# ---- 写像の向き: 脚質バイアス ----

def test_front_biased_course_boosts_front_penalizes_closer():
    f = _field(3)
    df = _race(leg_type_binary=[0.0, 0.5, 1.0])   # 先行 / 差し / 追込
    out = field_for_course(f, df, CourseContext(run_style_bias=1.0))
    assert out.ability[0] > f.ability[0]   # 前有利×先行 → ↑
    assert out.ability[1] == pytest.approx(f.ability[1])  # 中間脚質 → 変化なし
    assert out.ability[2] < f.ability[2]   # 前有利×追込 → ↓


def test_closer_biased_course_flips_sign():
    f = _field(2)
    df = _race(leg_type_binary=[0.0, 1.0])   # 先行 / 追込
    out = field_for_course(f, df, CourseContext(run_style_bias=-1.0))  # 差し追込有利
    assert out.ability[0] < f.ability[0]   # 差し有利×先行 → ↓
    assert out.ability[1] > f.ability[1]   # 差し有利×追込 → ↑


# ---- 写像の向き: 時計傾向 ----

def test_fast_course_boosts_speed_type():
    f = _field(3)
    df = _race(speed_fig_best=[80.0, 60.0, 40.0])   # 速い / 中 / 遅い
    fast = field_for_course(f, df, CourseContext(time_bias=1.0))
    tough = field_for_course(f, df, CourseContext(time_bias=-1.0))
    assert fast.ability[0] > f.ability[0]    # 高速×速い馬 → ↑
    assert fast.ability[2] < f.ability[2]    # 高速×遅い馬 → ↓
    assert tough.ability[0] < f.ability[0]   # タフ×速い馬 → ↓（底力型が相対優位）


def test_speed_fig_mean5_fallback():
    f = _field(2)
    df = _race(speed_fig_mean5=[80.0, 40.0])   # best 欠、mean5 で代替
    out = field_for_course(f, df, CourseContext(time_bias=1.0))
    assert out.ability[0] > out.ability[1]


# ---- clip / 定義域 / 非破壊 ----

def test_factor_is_clipped():
    f = _field(2, ability=1.0)
    df = _race(leg_type_binary=[0.0, 1.0])
    p = CourseAffinityParams(style_gain=10.0)   # 過大ゲインでも係数は [lo,hi] に収まる
    out = field_for_course(f, df, CourseContext(run_style_bias=1.0), p)
    assert out.ability[0] == pytest.approx(1.0 * p.ability_hi)
    assert out.ability[1] == pytest.approx(1.0 * p.ability_lo)


def test_ability_stays_in_domain():
    f = _field(2, ability=1.9)
    df = _race(leg_type_binary=[0.0, 1.0])
    out = field_for_course(f, df, CourseContext(run_style_bias=1.0),
                           CourseAffinityParams(style_gain=5.0))
    assert out.ability.max() <= 2.0 and out.ability.min() >= 0.3


def test_base_field_not_mutated():
    f = _field(2)
    a0 = f.ability.copy()
    field_for_course(f, _race(leg_type_binary=[0.0, 1.0]), CourseContext(run_style_bias=1.0))
    assert np.array_equal(f.ability, a0)


def test_only_ability_changes():
    f = _field(2)
    out = field_for_course(f, _race(leg_type_binary=[0.0, 1.0]), CourseContext(run_style_bias=1.0))
    assert list(out.style) == list(f.style)
    assert np.array_equal(out.stamina, f.stamina)
    assert np.array_equal(out.noise, f.noise)


# ---- 較正ゲイン復元（ca_* → CourseAffinityParams）----

def test_params_from_mapping_maps_ca_prefixed_gains():
    p = course_affinity_params_from_mapping({"ca_style_gain": 0.2, "ce_width_gain": 1.0})
    assert p is not None
    assert p.style_gain == 0.2
    assert p.time_gain == CourseAffinityParams().time_gain   # 未指定は既定


def test_params_from_mapping_none_without_ca_keys():
    assert course_affinity_params_from_mapping({"ce_width_gain": 1.0}) is None


def test_calibrated_gain_changes_strength():
    f = _field(2)
    df = _race(leg_type_binary=[0.0, 1.0])
    ctx = CourseContext(run_style_bias=1.0)
    weak = field_for_course(f, df, ctx, course_affinity_params_from_mapping({"ca_style_gain": 0.05}))
    strong = field_for_course(f, df, ctx, course_affinity_params_from_mapping({"ca_style_gain": 0.20}))
    assert strong.ability[0] > weak.ability[0] > f.ability[0]
