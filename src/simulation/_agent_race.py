"""エージェントベース競馬シミュレーション＋モンテカルロ（純 numpy・既存モデル非依存）。

各馬をエージェント化し（位置x・速度v・スタミナs・脚質style・能力μ・不確実性σ）、時間ステップで
状態更新（脚質ベースの目標速度→干渉→スタミナ制約→加速→ノイズ）。ペースは外生でなく先頭馬の
挙動から**内生**する（先行勢が多いと速くなりスタミナ切れ→差し台頭）。モンテカルロで多数回走らせ
勝率・複勝率・着順分布を得る。

【重要な位置づけ】これは確率**生成**モデルであり、情報を増やさない。出力の質は入力パラメータ
（μ,σ,style,stamina＝featuredから推定）の質で決まる。賭けのエッジになるかは「市場implied確率
より当たるか」でのみ決まり、それは Phase3（`_edge_diagnostic` で前進検証）で判定する。本モジュールは
Phase1＝エンジンのみ（純粋・決定論的にseed固定でテスト可能）。

性能: 状態を (n_sim, n_horses) の配列で持ち、全 sim×全馬を1ステップでベクトル更新する。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

# 脚質コード
STYLE_FRONT = 0    # 逃げ・先行
STYLE_STALKER = 1  # 差し（中団）
STYLE_CLOSER = 2   # 追込
STYLE_NAMES = {"front": STYLE_FRONT, "stalker": STYLE_STALKER, "closer": STYLE_CLOSER}


@dataclass
class RaceField:
    """1レースの出走馬パラメータ（各 (n_horses,) 配列）。"""
    ability: np.ndarray    # μ: 基礎能力（目標速度のスケール）
    style: np.ndarray      # 脚質コード（int）
    stamina: np.ndarray    # 初期スタミナ（>0、消費で減る）
    noise: np.ndarray      # σ: 加速度ノイズ（出遅れ・不利・詰まりの確率的表現）

    def __post_init__(self):
        self.ability = np.asarray(self.ability, dtype=float)
        self.style = np.asarray(self.style, dtype=int)
        self.stamina = np.asarray(self.stamina, dtype=float)
        self.noise = np.asarray(self.noise, dtype=float)

    @property
    def n(self) -> int:
        return len(self.ability)


@dataclass
class SimConfig:
    """シミュレーションのハイパーパラメータ（物理系）。"""
    T: int = 100                 # 時間ステップ数
    dt: float = 1.0
    accel_k: float = 0.5         # 比例制御ゲイン（目標速度への追従）
    stamina_cost: float = 0.01   # スタミナ消費係数（cost = α·v²）
    stamina_floor: float = 0.3   # スタミナ枯渇時の最低出力率
    interf_dist: float = 2.0     # この距離未満で前方干渉（詰まり）
    interf_mult: float = 0.7     # 干渉時の目標速度倍率
    # 脚質別 目標速度プロファイル（能力に対する倍率、レース進行率 phase で切替）
    front_mult: float = 1.0
    stalker_early: float = 0.9
    stalker_late: float = 1.1
    stalker_switch: float = 0.7
    closer_early: float = 0.7
    closer_late: float = 1.3
    closer_switch: float = 0.8


def _target_speed(style: np.ndarray, ability: np.ndarray, phase: float,
                  cfg: SimConfig) -> np.ndarray:
    """脚質×レース進行率 phase∈[0,1] に応じた目標速度（(n_sim,n_horses)）。"""
    vt = np.empty_like(ability)
    is_f = style == STYLE_FRONT
    is_s = style == STYLE_STALKER
    is_c = style == STYLE_CLOSER
    vt[is_f] = cfg.front_mult * ability[is_f]
    s_mult = cfg.stalker_early if phase < cfg.stalker_switch else cfg.stalker_late
    vt[is_s] = s_mult * ability[is_s]
    c_mult = cfg.closer_early if phase < cfg.closer_switch else cfg.closer_late
    vt[is_c] = c_mult * ability[is_c]
    return vt


def _front_distance(x: np.ndarray) -> np.ndarray:
    """各馬の「直前を走る馬との距離」（(n_sim,n_horses)）。先頭は大きな値。"""
    order = np.argsort(-x, axis=1)                    # 位置降順（先頭が先）
    x_sorted = np.take_along_axis(x, order, axis=1)
    gap_sorted = np.full_like(x_sorted, 1e9)
    gap_sorted[:, 1:] = x_sorted[:, :-1] - x_sorted[:, 1:]
    front = np.empty_like(x)
    np.put_along_axis(front, order, gap_sorted, axis=1)
    return front


def monte_carlo(field: RaceField, n_sim: int = 2000, cfg: SimConfig | None = None,
                seed: int = 0, place_k: int = 3, ability_sigma: float = 0.15,
                track_dynamics: bool = False) -> dict:
    """field を n_sim 回走らせ、勝率・複勝率(上位place_k)・平均着順・着順分布を返す。

    ability_sigma>0 のとき、各シミュレーションで能力を μ_i ± σ_i から引き直す
    （A_sim = μ + ability_sigma·σ_i·N(0,1)）。これが「その馬がどれだけブレるか」を表し、
    着順分布に本当の不確実性を与える＝**較正された勝率**になる（σ を入れないと勝者が
    ほぼ決定論的になり p_sim が [≒1,0,…] に潰れて log-loss が爆発する）。

    track_dynamics=True で創発ダイナミクスの要約も返す（忠実度検証用）:
      early_pos_rank : 各馬の「序盤(1/3地点)の平均位置順位」(0=先頭)。実測の第1コーナー通過順と対応。
      early_speed/late_speed : 序盤/終盤の全馬平均速度（全体のペース形。early>late＝前傾）。

    Returns
    -------
    {win, place, mean_rank, finish_counts[, early_pos_rank, early_speed, late_speed]}
    """
    cfg = cfg or SimConfig()
    n = field.n
    rng = np.random.default_rng(seed)

    A = np.tile(field.ability, (n_sim, 1))
    if ability_sigma > 0:
        # 各 sim・各馬で能力を μ±(ability_sigma·σ_i) から引き直す（σ_i=field.noise を相対的な
        # 能力不確実性として使い、ブレやすい馬ほど広く引く）。着順分布に本当の不確実性を与える。
        rel = field.noise / max(float(field.noise.mean()), 1e-6)   # 平均1に正規化した相対σ
        sig = np.tile(rel, (n_sim, 1))
        A = np.clip(A + ability_sigma * sig * rng.normal(0.0, 1.0, size=(n_sim, n)), 0.1, None)
    style = np.tile(field.style, (n_sim, 1))
    noise = np.tile(field.noise, (n_sim, 1))
    x = np.zeros((n_sim, n))
    v = np.zeros((n_sim, n))
    s = np.tile(field.stamina, (n_sim, 1)).astype(float)

    third = max(1, cfg.T // 3)
    early_pos_rank = np.zeros((n_sim, n))
    early_v = np.zeros(n_sim)
    late_v = np.zeros(n_sim)
    for t in range(cfg.T):
        phase = t / cfg.T
        vt = _target_speed(style, A, phase, cfg)
        front = _front_distance(x)
        vt = np.where(front < cfg.interf_dist, vt * cfg.interf_mult, vt)
        vt = vt * np.clip(s, cfg.stamina_floor, None)    # スタミナ制約
        a = (vt - v) * cfg.accel_k + rng.normal(0.0, 1.0, size=(n_sim, n)) * noise
        v = np.clip(v + a * cfg.dt, 0.0, None)
        x = x + v * cfg.dt
        s = np.clip(s - cfg.stamina_cost * v * v * cfg.dt, 0.0, None)
        if track_dynamics:
            if t < third:
                early_v += v.mean(axis=1)
            if t >= cfg.T - third:
                late_v += v.mean(axis=1)
            if t == third:
                early_pos_rank = (-x).argsort(axis=1).argsort(axis=1)

    # 各 sim の着順（0=1着）。位置降順の順位。
    finish_rank = (-x).argsort(axis=1).argsort(axis=1)
    win = (finish_rank == 0).mean(axis=0)
    place = (finish_rank < place_k).mean(axis=0)
    mean_rank = finish_rank.mean(axis=0) + 1.0
    finish_counts = np.zeros((n, n), dtype=int)
    for r in range(n):
        finish_counts[:, r] = (finish_rank == r).sum(axis=0)
    out = {"win": win, "place": place, "mean_rank": mean_rank,
           "finish_counts": finish_counts}
    if track_dynamics:
        out["early_pos_rank"] = early_pos_rank.mean(axis=0)     # (n,) 序盤位置順位の平均
        out["early_speed"] = float(early_v.mean() / third)
        out["late_speed"] = float(late_v.mean() / third)
    return out


def field_from_arrays(ability, style_names, stamina=None, noise=None) -> RaceField:
    """便利コンストラクタ: 脚質を文字列('front'等)で受け、既定スタミナ/ノイズを補う。"""
    style = np.array([STYLE_NAMES.get(str(s), STYLE_STALKER) for s in style_names], dtype=int)
    n = len(ability)
    stamina = np.ones(n) if stamina is None else np.asarray(stamina, dtype=float)
    noise = np.full(n, 0.05) if noise is None else np.asarray(noise, dtype=float)
    return RaceField(ability=np.asarray(ability, dtype=float), style=style,
                     stamina=stamina, noise=noise)
