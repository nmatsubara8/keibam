"""状態空間の潜在能力推定（Step4）— AbilityFilter 抽象と Elo/Kalman 実装。

強度分解の時系列項を推定する:
    s_i(t) = log q_i + μ_i(t) + c_i(t) + r_θ(x_i) + β(style_i, z)
      μ_i(t)  長期能力: ランダムウォーク  μ_t = μ_{t-1} + ε,  ε〜N(0, q_μ·Δ日)
      c_i(t)  短期調子: AR(1)            c_t = ρ^Δ日·c_{t-1} + η（定常分散 s_c²）
観測は性能指標 y_t = μ_t + c_t + v,  v〜N(0, R)（着順由来のレース内相対スコア等）。

設計原則（差し替え可能な状態推定器）:
- **Elo は独立の別物ではなく「更新ゲインを一定に固定した Kalman の特殊形（定常ゲイン近似）」**
  として実装する。抽象 API（initial/predict/update/strength/variance/predictive）だけを上位
  （Mixture-PL・市場アンカー残差）が使い、実装を Elo→Kalman→(EKF/UKF/粒子) と差し替えても
  上位レイヤには一切手を入れない。
- **分散を必ず持つ**: 欲しいのは点推定でなく a_i〜N(μ, σ²)。休み明け=平均高くても分散大/
  使い詰め=分散小、を predict の時間更新が自然に表現する（q_μ·Δ日 で分散が膨らむ）。
  この分散は後段 Kelly のシュリンケージ（不確実な馬ほど賭け率を下げる）に直接効く。
- 調子 c は能力 μ と分けて持つ: 休み明け→一叩き→疲労 の短期循環は AR(1) が担い、
  長期能力はランダムウォークが担う（役割分担）。

較正検査は `_model_compare.interval_calibration`（予測区間の被覆率＝PIT較正）で行う。
strength() の単位は log-odds 換算前の生スケール。s_i への合流係数（スケール）は学習レイヤで
較正する（本モジュールは推定器のみ）。純粋関数・stdlib のみ（scipy/pandas 非依存）。
"""
from __future__ import annotations

import dataclasses
import math
from abc import ABC
from abc import abstractmethod
from typing import Mapping


@dataclasses.dataclass(frozen=True)
class AbilityState:
    """1頭の潜在状態 N([μ, c], P)。P は 2×2 共分散（mu_var/c_var/cov）。

    観測 y=μ+c+v が和しか見ないため、更新後は μ と c に負の相関が生じる（cov<0）。
    これを落とすと分散を過大評価するので cov も必ず持ち回る。
    """

    mu: float = 0.0
    mu_var: float = 1.0
    c: float = 0.0
    c_var: float = 0.0
    cov: float = 0.0
    n_obs: int = 0


class AbilityFilter(ABC):
    """状態推定器の抽象。上位レイヤはこの API のみに依存する（実装差し替え自由）。"""

    @abstractmethod
    def initial(self) -> AbilityState:
        """デビュー馬の事前分布。"""

    @abstractmethod
    def predict(self, st: AbilityState, days: float) -> AbilityState:
        """時間更新（レース間隔 days 日）。休み明けほど分散が膨らみ、調子は減衰する。"""

    @abstractmethod
    def update(self, st: AbilityState, y: float) -> AbilityState:
        """観測更新（性能指標 y）。predict 済みの状態に適用する。"""

    def strength(self, st: AbilityState) -> float:
        """s_i への寄与 a_i = μ + c（点推定）。"""
        return st.mu + st.c

    def variance(self, st: AbilityState) -> float:
        """a_i = μ+c の分散 Var[μ+c] = mu_var + c_var + 2cov（Kelly シュリンケージ用）。"""
        return max(0.0, st.mu_var + st.c_var + 2.0 * st.cov)

    def predictive(self, st: AbilityState) -> tuple[float, float]:
        """次の観測 y の予測分布 N(μ+c, Var[μ+c]+R)。区間較正（PIT）に使う。"""
        return self.strength(st), self.variance(st) + self.obs_var

    # 観測ノイズ分散 R（predictive 用）。実装がプロパティで与える。
    obs_var: float = 1.0


class FixedGainFilter(AbilityFilter):
    """Elo 相当＝**更新ゲインを一定に固定した Kalman（定常ゲイン近似）**。

    update: μ ← μ + K·(y − μ)。動的項なし（predict は恒等・調子 c 不使用）。
    分散は定常値 steady_var に固定（ゲイン一定 ⇔ 事後分散一定、が Kalman との対応）。
    既存のペアワイズ Elo（_ratings.py）のレース単位版に相当する最も単純な基準実装。
    """

    def __init__(self, *, gain: float = 0.15, prior_mu: float = 0.0,
                 steady_var: float = 1.0, obs_var: float = 1.0) -> None:
        if not 0.0 < gain < 1.0:
            raise ValueError(f"gain は (0,1)（指定: {gain}）")
        self._gain = gain
        self._prior_mu = prior_mu
        self._steady_var = steady_var
        self.obs_var = obs_var

    def initial(self) -> AbilityState:
        return AbilityState(mu=self._prior_mu, mu_var=self._steady_var)

    def predict(self, st: AbilityState, days: float) -> AbilityState:
        return st  # 動的モデルなし（固定ゲイン近似）

    def update(self, st: AbilityState, y: float) -> AbilityState:
        return dataclasses.replace(
            st, mu=st.mu + self._gain * (float(y) - st.mu), n_obs=st.n_obs + 1
        )


class KalmanAbilityFilter(AbilityFilter):
    """線形ガウス厳密 Kalman（2状態: 能力ランダムウォーク＋調子 AR(1)）。

    predict（Δ=days, φ=ρ^Δ）:
        μ_var += q_mu·Δ            （能力 RW: 休み明けほど不確実に）
        c ← φ·c,  c_var ← φ²·c_var + s_c²·(1−φ²),  cov ← φ·cov
    update（H=[1,1], 観測分散 R）:
        S = Var[μ+c] + R,  K = P·Hᵀ/S,  x += K·(y−(μ+c)),  P ← (I−K·H)·P
    観測モデルが線形ガウスなのでこれが厳密解（EKF/UKF は非線形観測を入れる時に差し替え）。

    静的極限（q_mu=0, s_c=0）は正規-正規ベイズ更新の閉形式に厳密一致
    （例: 事前 N(85,4)・R=4・観測 y=89 → 事後 N(87,2)）。機構検査で保証。
    """

    def __init__(self, *, prior_mu: float = 0.0, prior_var: float = 1.0,
                 q_mu_per_day: float = 1e-3, rho_per_day: float = 0.97,
                 stationary_c_var: float = 0.25, obs_var: float = 1.0) -> None:
        if not 0.0 < rho_per_day <= 1.0:
            raise ValueError(f"rho_per_day は (0,1]（指定: {rho_per_day}）")
        self._prior_mu = prior_mu
        self._prior_var = prior_var
        self._q_mu = q_mu_per_day
        self._rho = rho_per_day
        self._s_c2 = stationary_c_var
        self.obs_var = obs_var

    def initial(self) -> AbilityState:
        # デビューの調子は定常分布 N(0, s_c²) から（情報なし）
        return AbilityState(mu=self._prior_mu, mu_var=self._prior_var,
                            c=0.0, c_var=self._s_c2, cov=0.0)

    def predict(self, st: AbilityState, days: float) -> AbilityState:
        d = max(0.0, float(days))
        phi = self._rho ** d
        return dataclasses.replace(
            st,
            mu_var=st.mu_var + self._q_mu * d,
            c=phi * st.c,
            c_var=phi * phi * st.c_var + self._s_c2 * (1.0 - phi * phi),
            cov=phi * st.cov,
        )

    def update(self, st: AbilityState, y: float) -> AbilityState:
        s_total = st.mu_var + st.c_var + 2.0 * st.cov + self.obs_var
        if s_total <= 0:
            return st
        k_mu = (st.mu_var + st.cov) / s_total
        k_c = (st.c_var + st.cov) / s_total
        e = float(y) - (st.mu + st.c)
        # (I−KH)P: HP = [mu_var+cov, cov+c_var]
        hp_mu = st.mu_var + st.cov
        hp_c = st.cov + st.c_var
        return AbilityState(
            mu=st.mu + k_mu * e,
            c=st.c + k_c * e,
            mu_var=st.mu_var - k_mu * hp_mu,
            c_var=st.c_var - k_c * hp_c,
            cov=st.cov - k_mu * hp_c,
            n_obs=st.n_obs + 1,
        )


def performance_from_rank(rank: int, n_horses: int) -> float:
    """着順→レース内相対性能スコア（+1=1着 … −1=最下位・線形）。観測 y の既定実装。

    y = (n+1−2·rank)/(n−1)。頭数差を吸収するレース内標準化。着差/タイム由来の
    より情報量の多い y に差し替えてよい（フィルタは y の作り方に依存しない）。
    """
    if n_horses < 2:
        return 0.0
    return (n_horses + 1 - 2 * float(rank)) / (n_horses - 1)


def strength_offsets(
    states: Mapping[int, AbilityState], flt: AbilityFilter, scale: float = 1.0
) -> dict[int, float]:
    """出走馬の状態 → s_i へ足す能力オフセット {馬番: scale·(μ+c)}。

    Mixture-PL/市場アンカー残差へは residual と同じ加算口で合流する（上位レイヤ不変更）:
        mixture_win_probs(odds, {h: r[h] + offsets[h]}, styles, beta, pz)
    scale は生スケール→log-odds の合流係数（学習レイヤで較正）。全状態が initial（情報なし）
    かつ prior_mu=0 なら offsets≡0 で帰無（Step1/3）に退化する。
    """
    return {h: scale * flt.strength(st) for h, st in states.items()}


def variance_offsets(
    states: Mapping[int, AbilityState], flt: AbilityFilter
) -> dict[int, float]:
    """出走馬の能力分散 {馬番: Var[μ+c]}（Step5: Kelly シュリンケージの入力）。"""
    return {h: flt.variance(st) for h, st in states.items()}
