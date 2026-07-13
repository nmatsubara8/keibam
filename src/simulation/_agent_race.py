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
    # ゲート（枠順）を [0,1] に正規化（0=最内..1=最外）。序盤の位置取り優位に使う
    # （内枠ほど序盤に前を取りやすい）。None は中立(0.5)＝効果なし＝後方互換。
    gate: np.ndarray | None = None
    # 回り(右/左)適性（レース内 z 正規化・+ ＝この回りが得意/− ＝不得意）。能力を微修正する。
    # None は中立(0)＝効果なし＝後方互換。
    turn_apt: np.ndarray | None = None
    # コース状態（馬場）: レース単位のスカラー 0=良..1=不良（稍重≈0.33/重≈0.67）。重いほど全馬が
    # 遅く・スタミナ消費が増える。既定 0（良）＝効果なし＝後方互換。
    going: float = 0.0
    # 馬場適性（レース内 z・+ ＝道悪得意/− ＝不得意）。馬場が重い(going>0)ときだけ能力を修正する。
    going_apt: np.ndarray | None = None

    def __post_init__(self):
        self.ability = np.asarray(self.ability, dtype=float)
        self.style = np.asarray(self.style, dtype=int)
        self.stamina = np.asarray(self.stamina, dtype=float)
        self.noise = np.asarray(self.noise, dtype=float)
        if self.gate is None:
            self.gate = np.full(len(self.ability), 0.5)   # 中立（枠効果なし）
        else:
            self.gate = np.asarray(self.gate, dtype=float)
        if self.turn_apt is None:
            self.turn_apt = np.zeros(len(self.ability))   # 中立（回り効果なし）
        else:
            self.turn_apt = np.asarray(self.turn_apt, dtype=float)
        if self.going_apt is None:
            self.going_apt = np.zeros(len(self.ability))  # 中立（馬場適性効果なし）
        else:
            self.going_apt = np.asarray(self.going_apt, dtype=float)

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
    interf_dist: float = 2.0     # 前方バンドの深さ（この距離内の前走馬を「詰まり」と数える）
    interf_mult: float = 0.7     # 前列が定員一杯のときの目標速度倍率（最大減速）
    # コース幅・馬体幅による横の定員（位置取りの物理）。lane_capacity = course_width / body_width
    # ＝前方バンドに横並びできる頭数。前方バンドの頭数がこれを超えると前列が埋まり上がれない
    # （混雑度 crowd = min(1, n_ahead/定員) に比例して減速）。全先行馬が前に殺到できず前列争いが
    # 起き、脚質だけで位置が決まりすぎる（sim 0.61 vs 実 0.37）を実測方向へ散らす。
    # course_width は競走が密集する（内めの）有効レース幅の既定値。将来コース別データで置換可。
    body_width: float = 1.1      # m（馬体幅＋レース間隔の実効値）
    course_width: float = 7.7    # m（実効レース幅）→ 定員 ≈ 7 頭
    # ゲート（枠順）効果: 序盤フェーズだけ内枠に目標速度優位を与える（位置取りの物理）。
    # gate_early = 最内(gate=0)と最外(gate=1)の序盤目標速度差の振幅（±gate_early/2）。
    # gate_fade = 効果が 0 に減衰する進行率（phase）。この因子は脚質と独立に序盤位置を散らし、
    # 「脚質だけで位置が決まりすぎる（sim 0.61 vs 実 0.37）」を実測方向へ緩める。
    gate_early: float = 0.12
    gate_fade: float = 0.4
    # 回り(右/左)適性の効き: 実効能力を (1 + turn_gain·turn_apt) で微修正。turn_apt は
    # レース内 z（+得意/−不得意）。不得意な回りの馬をわずかに遅くする（位置取り＋能力の物理）。
    turn_gain: float = 0.04
    # コース状態（馬場）の効き（すべて going∈[0,1] に比例。良=0 で無効果）:
    going_speed_k: float = 0.06   # 重い馬場ほど全馬の実効能力(目標速度)を下げる
    going_stamina_k: float = 0.5  # 重い馬場ほどスタミナ消費を増やす（消費 ×(1+これ·going)）
    going_apt_gain: float = 0.05  # 馬場適性で能力を修正（道悪巧者は重馬場で相対的に有利）
    # ペース強度（外生注入用）: 先行馬の序盤ペースを乗算スケール。1.0=既定（内生のみ）。
    # >1 で先行が速く飛ばす→前傾（v²でスタミナ消費増→終盤失速→差し台頭）、<1 でスロー→先行残り。
    # 素朴仮定「先行多数→速い」を捨て、データ学習したペース予測をここに入れて符号ごと修正する。
    pace_intensity: float = 1.0
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
    # 先行馬がレースのペースを作る。pace_intensity で序盤の飛ばし方を外生スケール
    # （高いほど速い逃げ＝前傾。v²コストでスタミナを削り終盤失速→差し有利になる）。
    vt[is_f] = cfg.front_mult * cfg.pace_intensity * ability[is_f]
    s_mult = cfg.stalker_early if phase < cfg.stalker_switch else cfg.stalker_late
    vt[is_s] = s_mult * ability[is_s]
    c_mult = cfg.closer_early if phase < cfg.closer_switch else cfg.closer_late
    vt[is_c] = c_mult * ability[is_c]
    return vt


def _crowd_ahead(x: np.ndarray, dist: float) -> np.ndarray:
    """各馬の「前方バンド(0<Δ≤dist)に居る他馬の頭数」(n_sim,n)。詰まり定員判定に使う。

    コース幅÷馬体幅＝横に並べる定員(lane_capacity)に対し、前方バンドの頭数が定員を
    超えると『前列が埋まっていて上がれない』＝位置取りの物理制約になる。単一の直前馬
    だけを見る _front_distance と違い、前が何頭で詰まっているか（混雑度）を数える。
    """
    diff = x[:, None, :] - x[:, :, None]       # (n_sim, n_i, n_j): x_j - x_i
    ahead = (diff > 0.0) & (diff <= dist)
    return ahead.sum(axis=2).astype(float)     # (n_sim, n_i) 前方バンドの他馬数


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
    # 回り(右/左)適性で実効能力を微修正（不得意な回りの馬をわずかに遅く）。中立 turn_apt=0 は無効果。
    if cfg.turn_gain:
        A = np.clip(A * (1.0 + cfg.turn_gain * np.tile(field.turn_apt, (n_sim, 1))), 0.1, None)
    # コース状態（馬場）: 重い馬場ほど全馬を遅く（going_speed_k）＋道悪適性で相対修正（going_apt_gain）。
    # どちらも going に比例＝良(going=0)なら無効果。スタミナ消費増は下のループで going を反映する。
    if field.going > 0.0:
        g = float(field.going)
        A = A * (1.0 - cfg.going_speed_k * g)
        if cfg.going_apt_gain:
            A = A * (1.0 + cfg.going_apt_gain * np.tile(field.going_apt, (n_sim, 1)) * g)
        A = np.clip(A, 0.1, None)
    stamina_cost_eff = cfg.stamina_cost * (1.0 + cfg.going_stamina_k * float(field.going))
    style = np.tile(field.style, (n_sim, 1))
    noise = np.tile(field.noise, (n_sim, 1))
    gate = np.tile(field.gate, (n_sim, 1))       # (n_sim,n) 0=内..1=外
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
        # ゲート（枠順）優位: 序盤だけ内枠(gate<0.5)に目標速度を上乗せし、gate_fade で 0 へ減衰。
        # 脚質と独立に序盤位置を散らす（内枠は前を取りやすい）。中立 gate=0.5 は無効果。
        if cfg.gate_early and phase < cfg.gate_fade:
            vt = vt * (1.0 + cfg.gate_early * (0.5 - gate) * (1.0 - phase / cfg.gate_fade))
        # 直前馬への詰まり（縦の単一干渉。隊列を縦に伸ばす既存機構は保持）。
        front = _front_distance(x)
        vt = np.where(front < cfg.interf_dist, vt * cfg.interf_mult, vt)
        # コース幅÷馬体幅の定員による横の詰まり（追加）: 前方バンドが定員超過なら前列が埋まり
        # 上がれない。全先行馬が前に殺到できず前列争い→脚質だけで位置が決まりすぎるのを散らす。
        lane_cap = max(1.0, cfg.course_width / cfg.body_width)
        crowd = np.clip(_crowd_ahead(x, cfg.interf_dist) / lane_cap, 0.0, 1.0)
        vt = vt * (1.0 - (1.0 - cfg.interf_mult) * crowd)
        vt = vt * np.clip(s, cfg.stamina_floor, None)    # スタミナ制約
        # dt 不変な時間積分: 決定論項は dt に比例、加速度ノイズは √dt に比例（Wiener 過程）。
        # これで dt を細かく（T を増やして）しても速度ブレの分散が保存され、答えが dt に収束する。
        # dt=1.0 では従来式 v += ((vt-v)·accel_k + noise·N) と厳密に一致（後方互換）。
        dv_det = (vt - v) * cfg.accel_k * cfg.dt
        dv_noise = noise * np.sqrt(cfg.dt) * rng.normal(0.0, 1.0, size=(n_sim, n))
        v = np.clip(v + dv_det + dv_noise, 0.0, None)
        x = x + v * cfg.dt
        s = np.clip(s - stamina_cost_eff * v * v * cfg.dt, 0.0, None)
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
