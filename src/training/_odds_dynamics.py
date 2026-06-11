"""オッズ力学モデル — 投票シェアベクトルの確率過程として締切シェアを予測する。

単勝市場はゼロサム（控除率付き）なので、馬ごとの独立回帰ではなく
レース全体のシェアベクトル p（Σp=1）の時間発展をモデル化する:

1. DirichletShareModel — 最終シェア y ~ Dirichlet(c·μ)、μ_i ∝ s_i^β·exp(γ_b)
2. KalmanShareModel   — 状態 =「真の人気」の CLR 座標、座標独立のスカラー Kalman
3. ParticleShareModel — 同状態空間のブートストラップフィルタ（Student-t 遷移ノイズ）
4. EnsembleShareModel — 上記の重み付き平均（重み = 検証 KL の逆数比 = 総合判断）

全モデルが「市場の重力」（人気順バケット別 drift/vol、_odds_gravity.GravityStats）を
事前分布として共有する。観測フェーズの欠損・8〜18 頭・取消馬を許容し、
データゼロでは恒等予測（最新観測シェアをそのまま返す）へ退化する。
"""

from __future__ import annotations

import logging
from abc import ABC
from abc import abstractmethod

import numpy as np
import pandas as pd

from src.constants._odds_dynamics import DEFAULT_OBS_NOISE
from src.constants._odds_phases import OddsPhase
from src.constants._odds_phases import PHASE_TIMELINE
from src.training._odds_gravity import GravityStats
from src.training._odds_gravity import adjacent_phase_pairs
from src.training._simplex import clr
from src.training._simplex import clr_inv
from src.training._simplex import popularity_ranks

logger = logging.getLogger(__name__)

# 予測 horizon の指定子
HORIZON_NEXT = "next"  # 次のチェックポイント
HORIZON_FINAL = "final"  # 発走時（= t0）


def _ordered_obs(obs: dict) -> list[tuple[str, pd.Series]]:
    """観測 dict を時系列順 (phase, shares) のリストに整える（共通馬で再正規化済み）。"""
    phases = [p for p in PHASE_TIMELINE if p in obs and obs[p] is not None and len(obs[p]) >= 2]
    if not phases:
        return []
    # 全フェーズに共通する馬集合へ揃える（途中取消馬は除外して再正規化）
    common = obs[phases[0]].index
    for p in phases[1:]:
        common = common.intersection(obs[p].index)
    if len(common) < 2:
        # 共通馬が少なすぎる場合は最新フェーズのみ使う
        latest = phases[-1]
        s = obs[latest].astype(float)
        return [(latest, s / s.sum())]
    out = []
    for p in phases:
        s = obs[p].loc[common].astype(float)
        out.append((p, s / s.sum()))
    return out


def _remaining_steps(phase: str) -> list[tuple[str, str]]:
    """phase から t0 までの残り遷移列を返す。"""
    timeline = list(PHASE_TIMELINE)
    if phase not in timeline:
        return []
    idx = timeline.index(phase)
    return list(zip(timeline[idx:-1], timeline[idx + 1 :], strict=True))


class AbstractShareDynamicsModel(ABC):
    """レース単位のシェア力学モデルの契約。

    fit はレース別シェア系列（race_share_sequences の出力）と重力統計を受け取る。
    predict_shares は 1 レース分の観測 {phase: シェア Series} を受け取り、
    horizon（"next"=次チェックポイント / "final"=発走時）のシェア Series（Σ=1）を返す。
    """

    name: str = "abstract"

    @abstractmethod
    def fit(self, sequences: dict, gravity: GravityStats) -> "AbstractShareDynamicsModel":
        raise NotImplementedError

    @abstractmethod
    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        raise NotImplementedError


class IdentityShareModel(AbstractShareDynamicsModel):
    """最新観測シェアをそのまま返すベースライン（効率的市場仮説の素朴版）。"""

    name = "identity"

    def fit(self, sequences: dict, gravity: GravityStats) -> "IdentityShareModel":
        return self

    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        ordered = _ordered_obs(obs)
        if not ordered:
            return pd.Series(dtype=float)
        return ordered[-1][1]


class DirichletShareModel(AbstractShareDynamicsModel):
    """Dirichlet 回帰: 最終シェア y ~ Dirichlet(c·μ)、μ_i ∝ s_i^β · exp(γ_bucket(rank_i))。

    - β: 現在シェアの持続性（β<1 なら平均回帰 = 人気の重力）
    - γ_b: 人気順バケット別の系統バイアス（log 領域の加法補正）
    - c: 精度（大きいほど最終シェアが μ の近くに集中）

    学習は (β, γ, log c) を Dirichlet 対数尤度の同時 MLE（低次元なので Nelder-Mead、
    精度初期値はモーメント法）。学習レースが少ない場合は β=1, γ=0 に固定し
    モーメント法で c のみ推定。0 件なら恒等予測へフォールバック。
    """

    name = "dirichlet"

    def __init__(self, min_races_full_fit: int = 50) -> None:
        self._min_races = min_races_full_fit
        self.beta_: float = 1.0
        self.gamma_: np.ndarray | None = None  # バケット別バイアス
        self.precision_: float = 100.0
        self.n_fit_races_: int = 0

    # -- μ の構築 ----------------------------------------------------------
    def _mu(self, shares: np.ndarray) -> np.ndarray:
        from src.constants._odds_dynamics import RANK_BUCKETS
        from src.constants._odds_dynamics import bucket_for_rank

        gamma = self.gamma_ if self.gamma_ is not None else np.zeros(len(RANK_BUCKETS))
        ranks = popularity_ranks(shares)
        log_mu = self.beta_ * np.log(np.clip(shares, 1e-12, None))
        log_mu = log_mu + np.array([gamma[bucket_for_rank(int(r))] for r in ranks])
        return clr_inv(log_mu)

    # -- 学習 ---------------------------------------------------------------
    def fit(self, sequences: dict, gravity: GravityStats) -> "DirichletShareModel":
        from src.constants._odds_dynamics import RANK_BUCKETS

        pairs = []  # (現在シェア, 最終シェア) — 最新の非 t0 フェーズ → t0
        for per_phase in sequences.values():
            if OddsPhase.T0 not in per_phase:
                continue
            inputs = [p for p in PHASE_TIMELINE if p in per_phase and p != OddsPhase.T0]
            if not inputs:
                continue
            s_cur = per_phase[inputs[-1]]
            s_fin = per_phase[OddsPhase.T0]
            common = s_cur.index.intersection(s_fin.index)
            if len(common) < 2:
                continue
            c = s_cur.loc[common].to_numpy()
            f = s_fin.loc[common].to_numpy()
            pairs.append((c / c.sum(), f / f.sum()))

        self.n_fit_races_ = len(pairs)
        n_buckets = len(RANK_BUCKETS)
        if not pairs:
            self.beta_, self.gamma_ = 1.0, np.zeros(n_buckets)
            return self

        if len(pairs) < self._min_races:
            # 少データ: β=1, γ=0 固定、モーメント法で精度のみ推定
            self.beta_, self.gamma_ = 1.0, np.zeros(n_buckets)
            self.precision_ = self._moment_precision(pairs)
            return self

        from scipy.optimize import minimize  # noqa: PLC0415

        # (β, γ, log c) を Dirichlet 対数尤度で同時 MLE する。
        # 次元が低い（1 + バケット数 + 1）ため Nelder-Mead で十分。
        # 精度 c の初期値はモーメント法（Var(y)=μ(1−μ)/(c+1)）から取る。
        self.beta_, self.gamma_ = 1.0, np.zeros(n_buckets)
        init_c = self._moment_precision(pairs)

        def neg_loglik(theta: np.ndarray) -> float:
            self.beta_ = float(theta[0])
            self.gamma_ = theta[1:-1]
            c = float(np.exp(np.clip(theta[-1], 0.0, 14.0)))
            ll = 0.0
            for cur, fin in pairs:
                mu = self._mu(cur)
                alpha = np.clip(c * mu, 1e-6, None)
                ll += _dirichlet_loglik(fin, alpha)
            return -ll

        theta0 = np.concatenate([[1.0], np.zeros(n_buckets), [np.log(init_c)]])
        res = minimize(
            neg_loglik, theta0, method="Nelder-Mead",
            options={"maxiter": 2000, "xatol": 1e-4, "fatol": 1e-4},
        )
        self.beta_ = float(res.x[0])
        self.gamma_ = res.x[1:-1]
        self.precision_ = float(np.exp(np.clip(res.x[-1], 0.0, 14.0)))
        return self

    def _moment_precision(self, pairs: list) -> float:
        """モーメント法: Var(y_i) = μ_i(1-μ_i)/(c+1) から c を推定する。"""
        ratios = []
        for cur, fin in pairs:
            mu = self._mu(cur)
            var = (fin - mu) ** 2
            expected = mu * (1 - mu)
            mask = expected > 1e-9
            if mask.any():
                ratios.append(float(np.mean(expected[mask] / np.clip(var[mask], 1e-12, None))) - 1)
        if not ratios:
            return 100.0
        return float(np.clip(np.median(ratios), 5.0, 1e5))

    # -- 予測 ---------------------------------------------------------------
    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        ordered = _ordered_obs(obs)
        if not ordered:
            return pd.Series(dtype=float)
        latest_phase, latest = ordered[-1]
        if self.n_fit_races_ == 0:
            return latest  # 恒等フォールバック
        mu = self._mu(latest.to_numpy())
        return pd.Series(mu, index=latest.index)


def _dirichlet_loglik(y: np.ndarray, alpha: np.ndarray) -> float:
    from scipy.special import gammaln  # noqa: PLC0415

    y = np.clip(y, 1e-12, None)
    return float(
        gammaln(alpha.sum()) - gammaln(alpha).sum() + np.sum((alpha - 1) * np.log(y))
    )


class KalmanShareModel(AbstractShareDynamicsModel):
    """状態空間モデル + Kalman Filter。

    状態 x =「真の人気」の CLR 座標（観測 = 真の市場評価 + ノイズ、というユーザー仮説）。
    遷移（座標独立、b = 人気順バケット）:
        x_{k+1,i} = x_{k,i} + drift_b + w,  w ~ N(0, vol_b²)
    観測:
        z_k = x_k + v,  v ~ N(0, R_phase²)
    CLR の sum-zero 自由度は softmax 逆変換が吸収するため、対角（スカラー×n 頭）の
    Kalman フィルタで十分。欠損フェーズは predict のみ進める。
    """

    name = "kalman"

    def __init__(self, obs_noise: dict | None = None) -> None:
        self._obs_noise = obs_noise or dict(DEFAULT_OBS_NOISE)
        self._gravity = GravityStats()

    def fit(self, sequences: dict, gravity: GravityStats) -> "KalmanShareModel":
        self._gravity = gravity
        return self

    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        ordered = _ordered_obs(obs)
        if not ordered:
            return pd.Series(dtype=float)
        index = ordered[0][1].index
        n = len(index)

        # 初期化: 最初の観測をそのまま状態に
        first_phase, first_shares = ordered[0]
        x = clr(first_shares.to_numpy())
        var = np.full(n, self._obs_noise.get(first_phase, 0.1) ** 2)
        ranks = popularity_ranks(first_shares.to_numpy())

        # 観測区間をフィルタリング（観測間の欠損フェーズは predict のみ）
        observed = {p: s for p, s in ordered}
        phases_present = [p for p, _ in ordered]
        last_obs_phase = phases_present[-1]
        steps = adjacent_phase_pairs(list(PHASE_TIMELINE))
        started = False
        for phase_from, phase_to in steps:
            if phase_from == first_phase:
                started = True
            if not started:
                continue
            if phase_from != first_phase and not started:
                continue
            # predict
            drift = np.empty(n)
            vol = np.empty(n)
            for i in range(n):
                drift[i], vol[i] = self._gravity.lookup(phase_from, phase_to, int(ranks[i]))
            x = x + drift
            var = var + vol**2
            # update（phase_to に観測があれば）
            if phase_to in observed and phase_to != first_phase:
                z = clr(observed[phase_to].to_numpy())
                r2 = self._obs_noise.get(phase_to, 0.1) ** 2
                k_gain = var / (var + r2)
                x = x + k_gain * (z - x)
                var = (1 - k_gain) * var
                ranks = popularity_ranks(clr_inv(x))
            if horizon == HORIZON_NEXT and self._is_next_checkpoint(phase_to, last_obs_phase):
                break
            if phase_to == OddsPhase.T0:
                break

        return pd.Series(clr_inv(x), index=index)

    @staticmethod
    def _is_next_checkpoint(phase: str, last_obs_phase: str) -> bool:
        """phase が last_obs_phase の直後のチェックポイントかどうか。"""
        timeline = list(PHASE_TIMELINE)
        try:
            return timeline.index(phase) == timeline.index(last_obs_phase) + 1
        except ValueError:
            return False


class ParticleShareModel(AbstractShareDynamicsModel):
    """ブートストラップ・パーティクルフィルタ（非 Gaussian 版の状態空間モデル）。

    遷移ノイズに Student-t（自由度 ν≈4）を使い、Gaussian では捉えにくい
    急激な「突っ込み」（直前の大口投票）の重い裾を表現する。
    ESS（有効サンプルサイズ）が N/2 を下回ったら系統リサンプリング。
    """

    name = "particle"

    def __init__(self, n_particles: int = 500, nu: float = 4.0, seed: int = 7) -> None:
        self._n = n_particles
        self._nu = nu
        self._rng = np.random.default_rng(seed)
        self._obs_noise = dict(DEFAULT_OBS_NOISE)
        self._gravity = GravityStats()

    def fit(self, sequences: dict, gravity: GravityStats) -> "ParticleShareModel":
        self._gravity = gravity
        return self

    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        ordered = _ordered_obs(obs)
        if not ordered:
            return pd.Series(dtype=float)
        index = ordered[0][1].index
        n = len(index)

        first_phase, first_shares = ordered[0]
        x0 = clr(first_shares.to_numpy())
        r0 = self._obs_noise.get(first_phase, 0.1)
        particles = x0 + self._rng.normal(0.0, r0, size=(self._n, n))
        weights = np.full(self._n, 1.0 / self._n)
        ranks = popularity_ranks(first_shares.to_numpy())

        observed = {p: s for p, s in ordered}
        last_obs_phase = ordered[-1][0]
        started = False
        for phase_from, phase_to in adjacent_phase_pairs(list(PHASE_TIMELINE)):
            if phase_from == first_phase:
                started = True
            if not started:
                continue
            # 遷移（Student-t ノイズ: 標準正規 / sqrt(χ²/ν)）
            drift = np.empty(n)
            vol = np.empty(n)
            for i in range(n):
                drift[i], vol[i] = self._gravity.lookup(phase_from, phase_to, int(ranks[i]))
            t_noise = self._rng.standard_t(self._nu, size=(self._n, n))
            # t 分布の分散は ν/(ν−2) なので vol に合わせてスケール
            scale = vol / np.sqrt(self._nu / (self._nu - 2.0))
            particles = particles + drift + t_noise * scale

            # 観測更新
            if phase_to in observed and phase_to != first_phase:
                z = clr(observed[phase_to].to_numpy())
                r2 = self._obs_noise.get(phase_to, 0.1) ** 2
                log_w = -0.5 * np.sum((particles - z) ** 2, axis=1) / r2
                log_w -= log_w.max()
                weights = weights * np.exp(log_w)
                total = weights.sum()
                if total <= 0 or not np.isfinite(total):
                    weights = np.full(self._n, 1.0 / self._n)
                else:
                    weights = weights / total
                # ESS < N/2 で系統リサンプリング
                ess = 1.0 / np.sum(weights**2)
                if ess < self._n / 2:
                    particles = particles[self._systematic_resample(weights)]
                    weights = np.full(self._n, 1.0 / self._n)
                mean_share = clr_inv(np.average(particles, axis=0, weights=weights))
                ranks = popularity_ranks(mean_share)

            if horizon == HORIZON_NEXT and KalmanShareModel._is_next_checkpoint(phase_to, last_obs_phase):
                break
            if phase_to == OddsPhase.T0:
                break

        # 予測 = パーティクルをシェア化してから重み付き平均（シンプレックス上の平均）
        shares = np.stack([clr_inv(p) for p in particles])
        mean = np.average(shares, axis=0, weights=weights)
        return pd.Series(mean / mean.sum(), index=index)

    def _systematic_resample(self, weights: np.ndarray) -> np.ndarray:
        positions = (self._rng.random() + np.arange(self._n)) / self._n
        return np.searchsorted(np.cumsum(weights), positions)


class EnsembleShareModel(AbstractShareDynamicsModel):
    """メンバー予測の重み付き平均（再正規化）。「総合的に判断するモデル」。

    重みは評価ハーネスが検証 KL の逆数比で算出したものを注入する。
    未指定なら等重み。
    """

    name = "ensemble"

    def __init__(self, members: dict[str, AbstractShareDynamicsModel], weights: dict[str, float] | None = None) -> None:
        self._members = members
        self.weights = weights or {name: 1.0 / len(members) for name in members}

    def fit(self, sequences: dict, gravity: GravityStats) -> "EnsembleShareModel":
        for m in self._members.values():
            m.fit(sequences, gravity)
        return self

    def predict_shares(self, obs: dict, horizon: str = HORIZON_FINAL) -> pd.Series:
        acc: pd.Series | None = None
        total_w = 0.0
        for name, model in self._members.items():
            pred = model.predict_shares(obs, horizon)
            if pred.empty:
                continue
            w = self.weights.get(name, 0.0)
            acc = pred * w if acc is None else acc.add(pred * w, fill_value=0.0)
            total_w += w
        if acc is None or total_w <= 0:
            return pd.Series(dtype=float)
        out = acc / total_w
        return out / out.sum()


def default_models() -> dict[str, AbstractShareDynamicsModel]:
    """標準のモデルセット（評価ハーネス・ウォッチャーが使う）。"""
    return {
        "identity": IdentityShareModel(),
        "dirichlet": DirichletShareModel(),
        "kalman": KalmanShareModel(),
        "particle": ParticleShareModel(),
    }
