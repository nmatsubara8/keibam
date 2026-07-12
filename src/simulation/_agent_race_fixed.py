"""固定距離エージェントシミュレーション（time-to-D）＋騎手戦略の確率モデル（モンテカルロ）。

時間箱(_agent_race)は「固定時間で距離が多い順に着順」で、速く走る＝有利になり前傾→差し有利の
展開×脚質を符号ごと再現できなかった。本エンジンは**実レース同様に固定距離 D を最短時間で
走る競争**にする（x>=D 到達時刻で着順）。これで飛ばした逃げは終盤失速して総時間が延び、
脚を溜めた差しに差される、という物理が自然に出る。

核心の3要素:
- 距離連動スタミナ: 予算 B_i = reserve · v0 · ability_i · D（等ペースで D を走り切ると使い切る量）。
  距離とスタミナを連動させることで knife-edge でなく自然な領域で挙動が決まる。
- drafting/位置取り: 前走馬の直後(スリップ)にいるとスタミナ消費が draft_save 分軽くなる。
  後方待機が「脚を溜められる」物理的理由。前で風を受ける逃げは満額消費。
- 騎手戦略の確率分布: 各シミュレーションで各馬の「狙う位置(前/後)」と「序盤の積極性」を
  分布から引く。これでペースが**創発的かつ確率的**になり（駆け引き）、モンテカルロが
  複数分布の結合を積分する。pace_intensity で場全体の積極性平均をずらし、学習ペースを注入できる。

出力は _agent_race.monte_carlo と同形（win/place/mean_rank/finish_counts[, dynamics]）で、
sim_fidelity / sim_pace_inject から --engine fixed で差し替えられる。
"""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from src.simulation._agent_race import (
    STYLE_CLOSER,
    STYLE_FRONT,
    STYLE_STALKER,
    RaceField,
)

# 脚質→狙う位置(0=先頭…1=最後方)の平均。騎手戦略はこの周りに分散して引かれる。
_STYLE_POS = {STYLE_FRONT: 0.20, STYLE_STALKER: 0.50, STYLE_CLOSER: 0.80}


@dataclass
class SimConfigFixed:
    """固定距離エンジンのハイパーパラメータ（物理系）。"""
    v0: float = 16.0             # 基準速度(単位任意、比のみ意味を持つ)
    dt: float = 1.0
    max_steps: int = 400
    accel_k: float = 0.5         # 目標速度への追従ゲイン
    reserve: float = 1.15        # スタミナ予算 = reserve·(基準距離 D_ref を等ペースで走り切る消費)
    stamina_ref_D: float = 1600.0  # スタミナ容量の基準距離[m]。予算は実距離でなくこれ×馬固有の
                                 # stamina 係数に固定＝馬ごとに異なる容量。短距離は使い切らず崩れない/
                                 # 長距離は使い果たし崩れる、かつ距離適性の個体差で効果が広い距離に及ぶ。
    cost_pow: float = 2.0        # 消費 = cost_coef·v^cost_pow（速いほど超過消費）
    exhaust_floor: float = 0.45  # スタミナ枯渇時の出力率（脚が上がる＝非線形の壁）
    early_frac: float = 0.6      # レース前半(位置取り)と後半(仕掛け)の境
    forward_gain: float = 0.15   # 位置取りの速度調整（小さく＝馬群は密集、速度差は僅か）
    late_base: float = 0.55      # 後半の基礎率（スタミナ枯渇馬はここまで失速＝脚が上がる）
    kick_gain: float = 0.60      # 残スタミナに応じた終盤の伸び（脚を溜めた馬が伸びる）
    draft_window: float = 8.0    # この距離以内で前走馬の直後＝ドラフト
    draft_save: float = 0.30     # ドラフト時の消費軽減率
    strat_pos_sigma: float = 0.14  # 騎手が狙う位置の分布幅（駆け引きの揺らぎ）
    aggr_sigma: float = 0.18     # 序盤の積極性の分布幅
    pressure_gain: float = 0.60  # 逃げ志向の馬が多いほど序盤ペースが上がる（競り合い＝前傾の創発）
    exec_noise: float = 0.35     # 実行ノイズ（出遅れ・不利の確率表現）


def _pos_frac(x: np.ndarray) -> np.ndarray:
    """各馬の『自分より前にいる馬の割合』∈[0,1]（0=先頭）。(n_sim,n)。"""
    n = x.shape[1]
    # 自分より x が大きい(前)馬の数 / (n-1)
    order = np.argsort(-x, axis=1)
    rank = np.empty_like(order)
    ar = np.arange(n)
    np.put_along_axis(rank, order, np.broadcast_to(ar, x.shape), axis=1)
    return rank / max(n - 1, 1)


def _front_gap(x: np.ndarray) -> np.ndarray:
    """直前を走る馬との距離（先頭は大きな値）。(n_sim,n)。"""
    order = np.argsort(-x, axis=1)
    x_sorted = np.take_along_axis(x, order, axis=1)
    gap_sorted = np.full_like(x_sorted, 1e9)
    gap_sorted[:, 1:] = x_sorted[:, :-1] - x_sorted[:, 1:]
    gap = np.empty_like(x)
    np.put_along_axis(gap, order, gap_sorted, axis=1)
    return gap


def monte_carlo_fixed(field: RaceField, D: float = 1600.0, n_sim: int = 400,
                      cfg: SimConfigFixed | None = None, seed: int = 0,
                      place_k: int = 3, ability_sigma: float = 0.30,
                      pace_intensity: float = 1.0,
                      track_dynamics: bool = False) -> dict:
    """固定距離 D を n_sim 回走らせ、勝率・複勝率・平均着順・着順分布を返す。

    pace_intensity>1 で場全体の積極性平均を上げる（＝前傾になりやすく、飛ばした先行が
    スタミナを削って終盤失速→差し台頭、という展開が確率的に増える）。学習ペースの注入口。
    """
    cfg = cfg or SimConfigFixed()
    n = field.n
    rng = np.random.default_rng(seed)
    v0 = cfg.v0

    # 能力（sim ごとに μ±σ で引き、着順分布に不確実性を与える）
    A = np.tile(field.ability, (n_sim, 1)).astype(float)
    if ability_sigma > 0:
        rel = field.noise / max(float(field.noise.mean()), 1e-6)
        A = np.clip(A + ability_sigma * np.tile(rel, (n_sim, 1)) * rng.normal(0, 1, (n_sim, n)),
                    0.1, None)

    # 騎手戦略を分布から引く: 狙う位置 pos_target と 序盤の積極性 aggr。
    pos_mean = np.array([_STYLE_POS.get(int(s), 0.5) for s in field.style])
    pos_target = np.clip(np.tile(pos_mean, (n_sim, 1))
                         + cfg.strat_pos_sigma * rng.normal(0, 1, (n_sim, n)), 0.0, 1.0)
    # pace_intensity>1 → 積極性平均↑（前を取りに行く馬が増える＝ペースが上がりやすい）
    aggr = np.clip(pace_intensity + cfg.aggr_sigma * rng.normal(0, 1, (n_sim, n)), 0.3, None)

    # スタミナ容量は馬固有(≒一定)で、基準距離 D_ref × 馬ごとの stamina 係数を等ペースで走り切る量
    # × reserve。実距離 D に比例させない → 消費/容量 = D/(reserve·D_ref·stam) が距離で変わり、短距離は
    # 使い切らず崩れない・長距離は超過して崩れる。stam の個体差(距離適性)で効果が広い距離帯に及ぶ。
    stam = np.tile(field.stamina, (n_sim, 1)).astype(float)
    stam = stam / max(float(field.stamina.mean()), 1e-6)          # 平均1に正規化した容量係数
    B = cfg.reserve * v0 * A * cfg.stamina_ref_D * stam
    s = B.copy()

    # 逃げの競り合い(創発ペース): 前を取りたい馬(pos_target 小)が多い sim ほど序盤ペースが上がる。
    # これで「実ペースが出走構成と相関する」＝前傾/後傾が composition から創発する。
    front_frac = (pos_target < 0.35).mean(axis=1, keepdims=True)     # (n_sim,1)
    field_pressure = 1.0 + cfg.pressure_gain * (front_frac - 0.35)

    x = np.zeros((n_sim, n))
    v = np.zeros((n_sim, n))
    finish_t = np.full((n_sim, n), np.inf)

    third = None
    if track_dynamics:
        early_v = np.zeros(n_sim); late_v = np.zeros(n_sim)
        early_pos = np.zeros((n_sim, n)); early_marked = False

    for t in range(cfg.max_steps):
        prog = float(np.clip(x.mean() / D, 0.0, 1.0))       # 全体進行率(位相)
        if prog < cfg.early_frac:
            # 前半: 場のペース(aggr=pace_intensity中心)で密集して走り、位置取りは小さな速度調整のみ。
            # 差がつくのは速度でなく drafting によるスタミナ消費（下段）——先頭は風を受け満額消費、
            # 後方は前走馬の直後で消費軽減＝脚を溜められる、という位置取りの本質。
            pf = _pos_frac(x)
            seek = np.clip(pf - pos_target, -0.4, 0.4)            # 目標より後ろ(>0)なら前へ加速
            vt = v0 * A * (aggr * field_pressure + cfg.forward_gain * seek)
        else:
            # 後半: 残スタミナが速度を分ける。枯れた逃げは late_base まで失速し、脚を溜めた
            # 差し(高srat)が伸びる＝『前傾で飛ばした先行が差される』機構がここで出る。
            srat = np.clip(s / np.maximum(B, 1e-9), 0.0, 1.5)
            vt = v0 * A * (cfg.late_base + cfg.kick_gain * srat)

        # スタミナ枯渇: 予算切れで出力が exhaust_floor 倍に落ちる（脚が上がる）
        depleted = s <= 0
        vt = np.where(depleted, vt * cfg.exhaust_floor, vt)

        # 加速（目標へ追従＋実行ノイズ）
        a = (vt - v) * cfg.accel_k + rng.normal(0, 1, (n_sim, n)) * cfg.exec_noise
        v = np.clip(v + a * cfg.dt, 0.0, None)
        x_new = x + v * cfg.dt

        # drafting: 直前に近接(0<gap<window)する馬は消費軽減
        gap = _front_gap(x)
        drafting = (gap > 0.3) & (gap < cfg.draft_window)
        cost_mult = np.where(drafting, 1.0 - cfg.draft_save, 1.0)

        # 到達時刻（線形補間）
        crossed = (finish_t == np.inf) & (x_new >= D)
        frac = np.where(v > 1e-9, (D - x) / np.maximum(v, 1e-9), 1.0)
        finish_t = np.where(crossed, t + np.clip(frac, 0.0, 1.0), finish_t)

        x = x_new
        s = np.clip(s - cost_mult * (v ** cfg.cost_pow) * cfg.dt, 0.0, None)

        if track_dynamics:
            if prog < 0.34:
                early_v += v.mean(axis=1)
                if not early_marked and prog >= 0.28:
                    early_pos = _pos_frac(x); early_marked = True
            if prog >= 0.66:
                late_v += v.mean(axis=1)
        if np.isfinite(finish_t).all():
            break

    # 未完走は残距離が大きいほど遅い扱いで順位付け
    unfinished = ~np.isfinite(finish_t)
    if unfinished.any():
        finish_t = np.where(unfinished, cfg.max_steps + (D - x) / max(v0, 1e-9), finish_t)

    finish_rank = finish_t.argsort(axis=1).argsort(axis=1)    # 早い到達=0(1着)
    win = (finish_rank == 0).mean(axis=0)
    place = (finish_rank < place_k).mean(axis=0)
    mean_rank = finish_rank.mean(axis=0) + 1.0
    finish_counts = np.zeros((n, n), dtype=int)
    for r in range(n):
        finish_counts[:, r] = (finish_rank == r).sum(axis=0)
    out = {"win": win, "place": place, "mean_rank": mean_rank, "finish_counts": finish_counts}
    if track_dynamics:
        steps = max(t + 1, 1)
        out["early_pos_rank"] = early_pos.mean(axis=0) * max(n - 1, 1)   # 0=先頭..(n-1)
        out["early_speed"] = float(early_v.sum() / steps)
        out["late_speed"] = float(late_v.sum() / steps)
    return out
