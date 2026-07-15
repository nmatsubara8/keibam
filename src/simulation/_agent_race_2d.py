"""Phase 1.5: 2次元・個体別物理のエージェント競馬シミュレーション（純 numpy）。

1D版(_agent_race.py)の忠実度不足——(1)ペースが実と逆相関、(3)隊列が再現弱——を狙って拡張:

  1. 発走速度 break_speed>0（全馬 v=0 発走だと序盤が加速相になり実の前半3Fと逆になる artifact を除去）。
  2. 位置ターゲット制御: 各馬は脚質×進行率に応じた「隊列内の目標位置(前/中/後)」を持ち、
     そこへ寄せる（速度でなく位置を制御）。先行は前、追込は序盤後方→終盤前へ。
  3. 2次元(横位置 y): 前が同一レーンで詰まったら「外に出す(swing)」——距離ロス(速度ペナルティ)を
     払って隣レーンへ。詰まり回避・外回しを表現（1Dの『詰まったら減速のみ』を超える）。
  4. 個体別物理: 最高速度・加速力・スタミナ容量/消費を能力・脚質から個体差として持つ。

忠実度検証(sim_fidelity)で 1D と比較し、(1)(3) が改善するかを測るための実装。予測でなく物理再現が目的。
状態は (n_sim, n_horses) 配列、干渉のみ (n_sim,n,n) のペアワイズで判定する。

**dt 不変**（1D と同規約）: 決定項・実走距離・スタミナ・swing・落馬確率は ×dt、加速度ノイズのみ
Wiener 過程で ×√dt。総時間 T·dt を保存したまま dt を細かく（steps=round(T/dt)）しても集約統計が収束する。
dt=1.0 では √dt=dt=1 なので従来値と数値一致（既存の dt=1.0 較正 `sim_calibration_2d.json` はそのまま有効）。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.simulation._agent_race import (
    STYLE_CLOSER,
    STYLE_FRONT,
    STYLE_STALKER,
    RaceField,
    _crowd_within,
)


@dataclass
class SimConfig2D:
    T: int = 100
    dt: float = 1.0
    break_speed: float = 0.55       # 発走直後の巡航寄り速度（×ability）
    accel_k: float = 0.5
    stamina_cost: float = 0.012
    stamina_floor: float = 0.35
    pos_gain: float = 0.6           # 目標隊列位置への寄せの強さ
    interf_dist: float = 2.0        # 縦の詰まり判定距離
    lane_width: float = 0.18        # 同一レーン判定（横距離）
    swing_step: float = 0.12        # 1ステップで外に出す横移動量
    interf_mult: float = 0.85       # 詰まって出せない時の減速
    # 脚質×進行率 の目標隊列位置（0=先頭 … 1=最後方）
    front_pos: float = 0.12
    stalker_early: float = 0.5
    stalker_late: float = 0.32
    closer_early: float = 0.82
    closer_late: float = 0.42
    switch: float = 0.6             # 早→遅の切替 phase
    # ── 2D化で正統物理へ格上げ（1D と同名 knob。既定は穏当な物理値／効果なし）──
    turn_k: float = 0.012           # 横レーン y による実走距離ロス: x += v·dt·(1−turn_k·y)。
                                    # 速度ペナルティ近似(旧 swing_cost)を置換＝外を回すほど距離が延びる
    lane_return: float = 0.05       # 詰まりが解けたら内ラチ(y→0)へ戻る速さ
    gate_lane: float = 1.0          # gate(枠順)→初期レーン y への写像強度（1=内枠ほど内ラチ発進）
    noise_mult: float = 1.0         # 加速度ノイズ倍率（大域ばらつき lever）
    turn_gain: float = 0.0          # 回り(右/左)適性で実効能力を微修正
    going_speed_k: float = 0.0      # 重い馬場ほど全馬を遅く
    going_stamina_k: float = 0.0    # 重い馬場ほどスタミナ消費増
    going_apt_gain: float = 0.0     # 道悪適性で能力修正
    kickback_k: float = 0.0         # 砂被り（ダートで前に馬がいる後方馬が鈍る）
    fall_base_flat: float = 0.0     # 平地の1ステップ落馬確率
    fall_base_jump: float = 0.0     # 障害の1ステップ落馬確率
    fall_congestion_k: float = 0.0  # 混雑で落馬確率上昇
    brought_down_p: float = 0.0     # 至近の落馬馬に巻き込まれる確率


def _target_position(style: np.ndarray, phase: float, cfg: SimConfig2D) -> np.ndarray:
    """脚質×進行率 → 目標隊列位置（0=先頭..1=最後方）。(n_sim,n)。"""
    tp = np.empty_like(style, dtype=float)
    is_f = style == STYLE_FRONT
    is_s = style == STYLE_STALKER
    is_c = style == STYLE_CLOSER
    tp[is_f] = cfg.front_pos
    tp[is_s] = cfg.stalker_early if phase < cfg.switch else cfg.stalker_late
    tp[is_c] = cfg.closer_early if phase < cfg.switch else cfg.closer_late
    return tp


def monte_carlo_2d(field: RaceField, n_sim: int = 800, cfg: SimConfig2D | None = None,
                   seed: int = 0, place_k: int = 3, ability_sigma: float = 0.35,
                   track_dynamics: bool = False, track_exotics: bool = False) -> dict:
    """2次元・個体別物理でモンテカルロ。返り値は _agent_race.monte_carlo と同形。

    track_dynamics=True で忠実度検証用の要約（early_pos_rank / early_speed / late_speed）も返す。
    early/late_speed は**発走直後の加速相を除いた**巡航区間で測る（実の前半3F/上がり3F に対応）。
    track_exotics=True で各試行の着順上位3頭の馬 index（top3, shape (n_sim, ≤3)）を返す
    ＝連系(馬連・3連複)の同時分布（共起依存）を実測と比較するため。
    """
    cfg = cfg or SimConfig2D()
    n = field.n
    rng = np.random.default_rng(seed)

    A = np.tile(field.ability, (n_sim, 1))
    if ability_sigma > 0:
        rel = field.noise / max(float(field.noise.mean()), 1e-6)
        A = np.clip(A + ability_sigma * np.tile(rel, (n_sim, 1)) * rng.normal(0, 1, (n_sim, n)), 0.1, None)
    # 回り(右/左)適性・馬場で実効能力を微修正（1D と同規約。中立値なら無効果）。
    if cfg.turn_gain and field.turn_apt is not None:
        A = np.clip(A * (1.0 + cfg.turn_gain * np.tile(field.turn_apt, (n_sim, 1))), 0.1, None)
    if field.going > 0.0:
        g = float(field.going)
        A = A * (1.0 - cfg.going_speed_k * g)
        if cfg.going_apt_gain and field.going_apt is not None:
            A = A * (1.0 + cfg.going_apt_gain * np.tile(field.going_apt, (n_sim, 1)) * g)
        A = np.clip(A, 0.1, None)
    stamina_cost_eff = cfg.stamina_cost * (1.0 + cfg.going_stamina_k * float(field.going))
    style = np.tile(field.style, (n_sim, 1))
    noise = np.tile(field.noise, (n_sim, 1))
    stamina = np.tile(field.stamina, (n_sim, 1)).astype(float)

    x = np.zeros((n_sim, n))
    v = cfg.break_speed * A                              # 発走速度>0
    # 初期レーン y: gate(枠順・0=内..1=外)から。gate に変動があれば枠なり、無ければ linspace で散らす。
    gate = np.asarray(field.gate, dtype=float)
    if float(np.nanstd(gate)) > 1e-6:
        y0 = cfg.gate_lane * (0.08 + 0.84 * gate) + (1.0 - cfg.gate_lane) * 0.5
    else:
        y0 = np.linspace(0.15, 0.85, n)
    y = np.tile(y0, (n_sim, 1)).astype(float)
    s = stamina.copy()
    fallen = np.zeros((n_sim, n), dtype=bool)
    _events_on = (cfg.fall_base_flat > 0 or cfg.fall_base_jump > 0
                  or cfg.fall_congestion_k > 0 or cfg.brought_down_p > 0)

    # 巡航区間（前半3F/上がり3F 相当）: 加速相(最初の10%)を除く
    warm = max(1, cfg.T // 10)
    e0, e1 = warm, max(warm + 1, cfg.T // 3)             # 序盤巡航
    l0, l1 = cfg.T - (cfg.T // 3), cfg.T                 # 終盤
    early_v = np.zeros(n_sim)
    late_v = np.zeros(n_sim)
    early_pos_rank = np.zeros((n_sim, n))

    for t in range(cfg.T):
        phase = t / cfg.T
        # 現在の隊列位置(0=先頭..1=最後方)
        pos_rank = (-x).argsort(axis=1).argsort(axis=1)
        pos_frac = pos_rank / max(n - 1, 1)
        # 目標位置との差 → 速度目標（後ろすぎたら押し上げる）
        tgt = _target_position(style, phase, cfg)
        push = 1.0 + cfg.pos_gain * (pos_frac - tgt)     # >1: もっと前へ
        v_target = A * np.clip(push, 0.3, 1.8) * np.clip(s, cfg.stamina_floor, None)

        # 2次元干渉: 前方同一レーンに詰まりがあるか（ペアワイズ）
        dx = x[:, None, :] - x[:, :, None]               # dx[s,i,j] = x_j - x_i
        dy = np.abs(y[:, None, :] - y[:, :, None])
        block = (dx > 0) & (dx < cfg.interf_dist) & (dy < cfg.lane_width)
        np.einsum("sii->si", block)[...] = False         # 自分は除外
        blocked = block.any(axis=2)                      # (n_sim,n)
        # 詰まり: 外に出す（ブロッカーの平均yと反対へ）。出せた分は減速ペナルティ、出せない時は減速。
        yj = np.broadcast_to(y[:, None, :], (n_sim, n, n))
        cnt = block.sum(axis=2)                          # 各馬の前方ブロッカー数
        ysum = np.where(block, yj, 0.0).sum(axis=2)
        blocker_y = np.where(cnt > 0, ysum / np.maximum(cnt, 1), np.nan)
        swing_dir = np.sign(np.where(np.isnan(blocker_y), y + 1e-9, y - blocker_y))
        swing_dir = np.where(swing_dir == 0, 1.0, swing_dir)
        # 詰まった馬は外へ持ち出し(y↑)、空いていれば内ラチ(y→0)へ戻る。外を回るコストは
        # 速度ペナルティでなく下段の実走距離ロス(1−turn_k·y)で払う＝物理的に正しい距離増。
        # swing_step は横移動「速度」（レーン/単位時間）として ×dt＝dt 不変（dt=1.0 で従来と同値）。
        y = np.where(blocked, np.clip(y + swing_dir * cfg.swing_step * cfg.dt, 0.0, 1.0),
                     np.clip(y - cfg.lane_return * cfg.dt, 0.0, 1.0))
        # まだ真後ろが塞がったまま(端で外へ出られない)なら減速
        boxed = blocked & ((y <= 0.001) | (y >= 0.999))
        v_target = np.where(boxed, v_target * cfg.interf_mult, v_target)
        # 砂被り（ダート・視界不良）: 前に馬がいる後方馬(cnt>0)は追走が鈍る。重い馬場で増幅・芝は無効。
        if cfg.kickback_k and field.is_dirt:
            kb = np.clip(cfg.kickback_k * np.minimum(cnt, 3) / 3.0 * (1.0 + float(field.going)), 0.0, 0.9)
            v_target = v_target * (1.0 - kb)

        # dt 不変な時間積分: 決定項は ×dt、ノイズは Wiener 過程で ×√dt（総時間 T·dt 固定で dt を細かく
        # しても分散が保存）。dt=1.0 では √dt=dt=1 で従来と数値一致・RNG 消費も1ステップ1ドローで不変。
        dv_det = (v_target - v) * cfg.accel_k * cfg.dt
        dv_noise = cfg.noise_mult * noise * np.sqrt(cfg.dt) * rng.normal(0, 1, (n_sim, n))
        v = np.clip(v + dv_det + dv_noise, 0.0, None)
        # 実走距離ロス: 外レーン(y大)ほど縦進行が減る＝同じ速度でも距離を余計に走る（可変距離）。
        x = x + v * cfg.dt * (1.0 - cfg.turn_k * y)
        s = np.clip(s - stamina_cost_eff * v * v * cfg.dt, 0.0, None)
        # 落馬・巻き込まれ（離散イベント）: 発火馬は以降 v=0・進行停止で DNF。
        if _events_on:
            base = cfg.fall_base_jump if field.is_jump else cfg.fall_base_flat
            p_fall = (base + cfg.fall_congestion_k * (cnt > 0)) * cfg.dt
            new_fall = (rng.random((n_sim, n)) < p_fall) & ~fallen
            if cfg.brought_down_p > 0.0:
                near_fallen = _crowd_within(x, fallen, cfg.interf_dist) > 0
                new_fall |= near_fallen & ~fallen & (rng.random((n_sim, n)) < cfg.brought_down_p * cfg.dt)
            fallen |= new_fall
            v = np.where(fallen, 0.0, v)

        if track_dynamics:
            if e0 <= t < e1:
                early_v += v.mean(axis=1)
            if l0 <= t < l1:
                late_v += v.mean(axis=1)
            if t == e1 - 1:
                early_pos_rank = (-x).argsort(axis=1).argsort(axis=1).astype(float)

    if _events_on and fallen.any():
        x = np.where(fallen, -np.inf, x)
    finish_rank = (-x).argsort(axis=1).argsort(axis=1)
    win = (finish_rank == 0).mean(axis=0)
    place = (finish_rank < place_k).mean(axis=0)
    mean_rank = finish_rank.mean(axis=0) + 1.0
    finish_counts = np.zeros((n, n), dtype=int)
    for r in range(n):
        finish_counts[:, r] = (finish_rank == r).sum(axis=0)
    out = {"win": win, "place": place, "mean_rank": mean_rank, "finish_counts": finish_counts}
    if track_dynamics:
        out["early_pos_rank"] = early_pos_rank.mean(axis=0)
        out["early_speed"] = float(early_v.mean() / max(e1 - e0, 1))
        out["late_speed"] = float(late_v.mean() / max(l1 - l0, 1))
    if track_exotics:
        # 各試行の着順（(-x) 昇順＝x 降順＝1着が先頭）。上位3頭の馬 index を返す。
        # 落馬馬は x=-inf で自動的に末尾＝top3 に入らない。
        order = (-x).argsort(axis=1)
        out["top3"] = order[:, :min(3, n)].astype(int)
    return out
