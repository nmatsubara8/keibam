"""効率的パリミュチュエル市場における RL / マルコフ(HMM)手法の無効性の実証（純 numpy）。

## 主張（no-free-information 定理）

強化学習(RL)も隠れマルコフ(HMM)によるレジーム検出も、**状態に存在しない情報を作り出す
ことはできない**。パリミュチュエル(pari-mutuel)ではオッズ=市場の総意であり、市場が効率的なら
「まだ価格に織り込まれていない構造」は残っていない。したがって:

    効率的市場では、任意のベットの期待純損益 = −(控除率) < 0 = 「賭けない」の報酬。
    ⇒ RL が学習しうる最適方策は **abstain（賭けない）** に収束する。

レジームが存在しても、市場がそのレジームを価格に織り込む（=効率的オッズ）限り、
レジームを正しく当てても払戻に反映済みなので依然として控除率ぶん負ける。マルコフ/HMM の
時系列フィルタが価値を持つのは、市場がレジームを**まだ織り込んでいない**（=非効率/stale な
オッズ）ときだけ。これが Benter が香港の低効率プールで、Ranogajec がリベートで edge を得た
構図であり、公開 JRA 単勝データ（echo≈0.989, ΔR²≈0 の効率的市場）では成立しない。

## 本モジュールが提供する実証装置

1. `simulate_market(mode=...)` — 潜在レジーム z（2状態マルコフ連鎖）を持つ市場を生成。
   - `MODE_EFFICIENT`: オッズが現在のレジームを織り込む（p_market = p_true）。
   - `MODE_STALE`   : オッズがレジームを無視（p_market はレジーム前の素の強さ）。=非効率の対照群。
2. `market_baseline` — 本命を毎レース買う素朴戦略。効率市場での平均純損益は厳密に −控除率。
3. `ContextualBandit` — 表形式 Q 学習の文脈付きバンディット。行動={賭けない, 本命, レジーム本命}。
   効率市場では abstain に収束（=「最適な打ち手は打たないこと」）。偽の文脈(noise)を与えると
   **in-sample では見かけの利益、OOS では控除率ぶんの損失**（=動画の300%と同じ後付け過学習）。
4. `regime_strategy` — HMM 前向きフィルタでレジームを推定し、レジーム時のみ賭ける。
   効率オッズ下では負け（レジームは価格済み）。stale オッズ下では勝つ（=対照群、非効率が edge の源泉）。

すべて seed 制御で再現可能。外部依存は numpy のみ（hmmlearn 不使用）。

レイヤ: simulation。I/O・グローバル状態なし。
"""

from __future__ import annotations

import dataclasses

import numpy as np

MODE_EFFICIENT = "efficient"
MODE_STALE = "stale"

# 行動の識別子（Q 学習の列インデックス）。
ACT_ABSTAIN = 0  # 賭けない（報酬は常に 0 = リスクフリー基準）
ACT_FAVORITE = 1  # 本命（最小オッズ）を買う
ACT_REGIME = 2  # レジーム本命（優遇集合内で市場評価最大の馬）を買う
N_ACTIONS = 3


# ---------------------------------------------------------------------------
# 市場の設定と生成
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class MarketConfig:
    """効率的市場シミュレータの設定。

    Attributes
    ----------
    n_horses : 1 レースの頭数。
    takeout : 控除率（払戻率 = 1 − takeout）。既定 0.20（JRA 単勝）。
    favored_fraction : レジーム時にボーナスを受ける「優遇集合」の割合（先頭側の馬）。
    regime_bonus : レジーム=1 のとき優遇集合の強さに加える量。大きいほど stale 市場での歪みが大。
    p_stay : レジームの持続確率（2状態マルコフ連鎖の対角）。時系列相関の強さ。
    signal_noise : レジーム信号（HMM の観測）の放射標準偏差。大きいほど検出が難しい。
    strength_sd : 各馬の素の強さ（レース固有の実力）の標準偏差。
    """

    n_horses: int = 8
    takeout: float = 0.20
    favored_fraction: float = 0.5
    regime_bonus: float = 1.6
    p_stay: float = 0.9
    signal_noise: float = 0.7
    strength_sd: float = 1.0


@dataclasses.dataclass(frozen=True)
class Races:
    """生成されたレース群（発走前確定情報 + 実現結果）。すべて長さ T の並び。"""

    regime: np.ndarray  # (T,) 潜在レジーム 0/1
    p_true: np.ndarray  # (T, N) 真の勝率
    p_market: np.ndarray  # (T, N) 市場含意勝率（効率なら p_true, stale ならレジーム無視）
    odds: np.ndarray  # (T, N) 単勝オッズ = (1-takeout)/p_market
    signal: np.ndarray  # (T,) レジームの雑音観測（HMM の入力）
    winner: np.ndarray  # (T,) 勝ち馬 index（p_true から抽選）
    favorite_idx: np.ndarray  # (T,) 本命 = argmax p_market
    regime_pick_idx: np.ndarray  # (T,) レジーム本命 = 優遇集合内で p_market 最大
    noise: np.ndarray  # (T,) 予測に無関係な偽の文脈（過学習の実演用）
    takeout: float
    mode: str

    def __len__(self) -> int:
        return int(self.regime.shape[0])


def _softmax(x: np.ndarray, axis: int = -1) -> np.ndarray:
    z = x - x.max(axis=axis, keepdims=True)
    e = np.exp(z)
    return e / e.sum(axis=axis, keepdims=True)


def transition_matrix(p_stay: float) -> np.ndarray:
    """2 状態レジームの遷移行列 [[stay,move],[move,stay]]。"""
    q = 1.0 - p_stay
    return np.array([[p_stay, q], [q, p_stay]], dtype=float)


def simulate_market(
    n_races: int,
    config: MarketConfig,
    mode: str,
    rng: np.random.Generator,
) -> Races:
    """潜在レジームを持つパリミュチュエル市場を生成する。

    mode=MODE_EFFICIENT: オッズが現在レジームを完全に織り込む（p_market=p_true）。
        → いかなるベットも E[純損益]=−控除率。RL/HMM は原理的に勝てない。
    mode=MODE_STALE: オッズがレジームのボーナスを無視（レジーム前の素の強さで価格）。
        → レジーム=1 のとき優遇集合が過小評価され、そこに賭けると正の期待値（=非効率＝対照群）。
    """
    if mode not in (MODE_EFFICIENT, MODE_STALE):
        raise ValueError(f"unknown mode: {mode}")
    N = config.n_horses
    n_fav = max(1, int(round(N * config.favored_fraction)))
    trans = transition_matrix(config.p_stay)

    # レジームの 2 状態マルコフ連鎖（時系列相関の源泉）。
    regimes = np.empty(n_races, dtype=int)
    z = int(rng.random() < 0.5)
    for t in range(n_races):
        if t > 0:
            z = int(rng.random() < trans[z, 1])
        regimes[t] = z

    # 各馬の素の強さ（レース固有）。優遇集合は先頭 n_fav 頭。
    s = rng.normal(0.0, config.strength_sd, size=(n_races, N))
    bonus = np.zeros((n_races, N))
    bonus[:, :n_fav] = config.regime_bonus * regimes[:, None]
    p_true = _softmax(s + bonus, axis=1)

    if mode == MODE_EFFICIENT:
        p_market = p_true
    else:  # MODE_STALE: レジームを織り込まない（素の強さのみ）
        p_market = _softmax(s, axis=1)

    odds = (1.0 - config.takeout) / p_market

    # 勝ち馬を p_true から抽選（逆関数法・ベクトル化）。
    cum = np.cumsum(p_true, axis=1)
    u = rng.random(n_races)[:, None]
    winner = (u < cum).argmax(axis=1)

    signal = regimes + rng.normal(0.0, config.signal_noise, size=n_races)
    favorite_idx = np.argmax(p_market, axis=1)
    # レジーム本命: 優遇集合（先頭 n_fav）内で市場評価が最大の馬（発走前に観測可能）。
    regime_pick_idx = np.argmax(p_market[:, :n_fav], axis=1)
    noise = rng.normal(0.0, 1.0, size=n_races)

    return Races(
        regime=regimes,
        p_true=p_true,
        p_market=p_market,
        odds=odds,
        signal=signal,
        winner=winner,
        favorite_idx=favorite_idx,
        regime_pick_idx=regime_pick_idx,
        noise=noise,
        takeout=config.takeout,
        mode=mode,
    )


# ---------------------------------------------------------------------------
# ベットの純損益（1 点 100% 元本、単位 stake=1）
# ---------------------------------------------------------------------------


def bet_net(races: Races, t: int, horse: int) -> float:
    """レース t で horse を単勝 1 点買ったときの純損益（払戻−元本）。"""
    ret = races.odds[t, horse] if races.winner[t] == horse else 0.0
    return float(ret) - 1.0


def _action_horse(races: Races, t: int, action: int) -> int:
    """行動 → 買う馬 index（ACT_ABSTAIN では呼ばない）。"""
    if action == ACT_FAVORITE:
        return int(races.favorite_idx[t])
    if action == ACT_REGIME:
        return int(races.regime_pick_idx[t])
    raise ValueError(f"action {action} is not a bet")


def action_net(races: Races, t: int, action: int) -> float:
    """行動 t の実現純損益（賭けない=0, それ以外は当該馬の純損益）。"""
    if action == ACT_ABSTAIN:
        return 0.0
    return bet_net(races, t, _action_horse(races, t, action))


# ---------------------------------------------------------------------------
# 市場ベースライン（本命を毎レース買う素朴戦略）
# ---------------------------------------------------------------------------


def market_baseline(races: Races) -> dict:
    """本命（最小オッズ）を毎レース買った場合の成績。

    効率市場では平均純損益率は厳密に −控除率に収束する（E[p_true·odds]=1−t）。
    """
    idx = np.arange(len(races))
    fav = races.favorite_idx
    hit = races.winner == fav
    ret = np.where(hit, races.odds[idx, fav], 0.0)
    net = ret - 1.0
    return {
        "mean_net": float(net.mean()),
        "hit_rate": float(hit.mean()),
        "recovery_rate": float(ret.mean()),  # 回収率（1.0 が損益分岐）
        "n_bets": int(len(races)),
    }


# ---------------------------------------------------------------------------
# HMM 前向きフィルタ（レジーム事後確率）
# ---------------------------------------------------------------------------


def regime_posterior(signals: np.ndarray, config: MarketConfig) -> np.ndarray:
    """信号列から P(z_t=1 | 観測 ≤ t) を前向きフィルタで推定（2 状態ガウス HMM）。

    放射は平均 {0,1}・標準偏差 signal_noise の正規分布。遷移は transition_matrix(p_stay)。
    正規化するため放射尤度の定数は無視してよい。hmmlearn 非依存の最小実装。
    """
    trans = transition_matrix(config.p_stay)
    sd = config.signal_noise
    means = np.array([0.0, 1.0])

    def emit(x: float) -> np.ndarray:
        return np.exp(-0.5 * ((x - means) / sd) ** 2)

    T = len(signals)
    post = np.empty(T)
    a = np.array([0.5, 0.5]) * emit(signals[0])
    a /= a.sum()
    post[0] = a[1]
    for t in range(1, T):
        pred = a @ trans
        a = pred * emit(signals[t])
        a /= a.sum()
        post[t] = a[1]
    return post


# ---------------------------------------------------------------------------
# マルコフ/HMM レジーム戦略（レジーム時のみ賭ける）
# ---------------------------------------------------------------------------


def regime_strategy(races: Races, config: MarketConfig, *, threshold: float = 0.6) -> dict:
    """HMM 事後 P(z=1) が閾値を超えたレースだけレジーム本命を買う戦略。

    効率オッズ下: レジームを正しく当てても払戻に反映済み → 賭けたレースの平均純損益 ≈ −控除率。
    stale オッズ下: レジーム=1 で優遇集合が過小評価 → 賭けたレースの平均純損益 > 0（対照群）。

    「同じアルゴリズムが、市場が非効率になった瞬間に勝ち始める」ことで、無効性の原因が
    アルゴリズムではなく**市場効率**であることを示す。
    """
    post = regime_posterior(races.signal, config)
    bet_mask = post > threshold
    nets = []
    for t in np.nonzero(bet_mask)[0]:
        nets.append(bet_net(races, int(t), int(races.regime_pick_idx[t])))
    nets = np.array(nets) if nets else np.array([])
    n_bets = int(nets.size)
    return {
        "mean_net_per_bet": float(nets.mean()) if n_bets else 0.0,
        "recovery_rate": float((nets + 1.0).mean()) if n_bets else 1.0,
        "n_bets": n_bets,
        "coverage": float(bet_mask.mean()),
        "mean_net_overall": float(nets.sum() / len(races)) if len(races) else 0.0,
    }


# ---------------------------------------------------------------------------
# 表形式バンディット（文脈付き・完全フィードバック）
# ---------------------------------------------------------------------------


def _bin_edges(n_bins: int, lo: float, hi: float) -> np.ndarray:
    """[lo, hi] を n_bins 等分する内部境界（digitize 用, 長さ n_bins-1）。"""
    return np.linspace(lo, hi, n_bins + 1)[1:-1]


@dataclasses.dataclass
class ContextualBandit:
    """文脈付きバンディット（完全フィードバックのサンプル平均 Q, γ=0）。

    競馬では発走後に**全馬の着順**が判明するので、各行動の反実仮想の払戻も計算できる
    （＝完全フィードバック）。よって Q は各 (状態, 行動) の純損益のサンプル平均で推定する。
    行動 = {賭けない(=常に報酬0), 本命, レジーム本命}。貪欲方策は Q が最大の行動を選ぶので、
    どの賭けの Q も 0 未満なら **abstain（賭けない）** を選ぶ。

    状態 = (レジーム事後の bin, noise の bin)。noise は予測に無関係な**偽の文脈**。

    - `n_noise_bins` を粗く取ると: 効率市場では全ての賭けの Q → −控除率 < 0 なので
      **全レース abstain**。「効率市場での RL の最適方策は打たないこと」。
    - `n_noise_bins` を細かく取ると: 各セルの標本が疎になり、たまたま in-sample の平均が
      正のセルが現れる（多重比較）。貪欲方策はそのセルだけ賭ける ⇒ **in-sample は利益、
      OOS では控除率ぶんの損失**。動画の「回収率300%」= test 上で買い目閾値を後付け最適化
      した過学習と同じ機序を、文脈の細かさ（モデル複雑度）の関数として再現する。
    """

    n_post_bins: int = 3
    n_noise_bins: int = 5
    q: np.ndarray = dataclasses.field(default=None, repr=False)
    counts: np.ndarray = dataclasses.field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.q = np.zeros((self.n_post_bins, self.n_noise_bins, N_ACTIONS))
        self.counts = np.zeros((self.n_post_bins, self.n_noise_bins, N_ACTIONS))
        self._noise_edges = _bin_edges(self.n_noise_bins, -2.5, 2.5)
        self._post_edges = _bin_edges(self.n_post_bins, 0.0, 1.0)

    def _state(self, post_t: float, noise_t: float) -> tuple[int, int]:
        pb = int(np.digitize(post_t, self._post_edges))
        nb = int(np.digitize(noise_t, self._noise_edges))
        return pb, nb

    def _greedy(self, pb: int, nb: int) -> int:
        """Q 最大の行動（賭けない=0 が全ての賭けを上回れば abstain）。"""
        return int(np.argmax(self.q[pb, nb]))

    def train(self, races: Races) -> "ContextualBandit":
        """各 (状態, 賭け行動) の純損益サンプル平均を Q に積む（abstain の Q は 0 固定）。"""
        post = regime_posterior(races.signal, _filter_config(races))
        sums = np.zeros_like(self.q)
        for t in range(len(races)):
            pb, nb = self._state(post[t], races.noise[t])
            for a in (ACT_FAVORITE, ACT_REGIME):
                sums[pb, nb, a] += action_net(races, t, a)
                self.counts[pb, nb, a] += 1.0
        with np.errstate(invalid="ignore", divide="ignore"):
            self.q = np.where(self.counts > 0, sums / np.maximum(self.counts, 1.0), 0.0)
        self.q[:, :, ACT_ABSTAIN] = 0.0  # 賭けない報酬は常に 0
        return self

    def evaluate(self, races: Races) -> dict:
        """貪欲方策で成績を測る（in-sample にも OOS にも使える）。"""
        post = regime_posterior(races.signal, _filter_config(races))
        nets = np.empty(len(races))
        n_abstain = n_bet = n_bet_hit = 0
        for t in range(len(races)):
            pb, nb = self._state(post[t], races.noise[t])
            a = self._greedy(pb, nb)
            nets[t] = action_net(races, t, a)
            if a == ACT_ABSTAIN:
                n_abstain += 1
            else:
                n_bet += 1
                if nets[t] > 0:
                    n_bet_hit += 1
        return {
            "mean_net": float(nets.mean()),  # abstain=0 を含む全レース平均
            "mean_net_per_bet": float(np.sum(nets) / n_bet) if n_bet else 0.0,
            "abstain_fraction": float(n_abstain / len(races)),
            "n_bets": int(n_bet),
            "bet_hit_rate": float(n_bet_hit / n_bet) if n_bet else 0.0,
        }


def _filter_config(races: Races) -> MarketConfig:
    """HMM フィルタが使う設定（頭数と控除率だけ races に合わせ、他は既定）。"""
    return MarketConfig(n_horses=races.p_true.shape[1], takeout=races.takeout)


def train_bandit(
    races_train: Races,
    *,
    n_post_bins: int = 3,
    n_noise_bins: int = 5,
) -> ContextualBandit:
    """訓練集合でバンディットを学習して返す（便宜関数）。"""
    return ContextualBandit(n_post_bins=n_post_bins, n_noise_bins=n_noise_bins).train(
        races_train
    )


# ---------------------------------------------------------------------------
# 実験オーケストレーション
# ---------------------------------------------------------------------------

_DEFAULT_CONFIG = MarketConfig()


def run_futility_experiment(
    *,
    n_train: int = 1500,
    n_test: int = 1500,
    config: MarketConfig = _DEFAULT_CONFIG,
    seed: int = 0,
    regime_threshold: float = 0.6,
    overfit_noise_bins: int = 40,
) -> dict:
    """効率市場と stale 市場（対照群）で全戦略を走らせた結果をまとめて返す。

    返り値の要点:
      efficient.baseline.mean_net ≈ −控除率              （素朴ベット=控除率ぶん負け）
      efficient.bandit.oos.abstain_fraction ≈ 1.0        （粗い RL は「賭けない」に収束＝勝てない）
      efficient.overfit_bandit.in_sample.mean_net_per_bet > 0 > oos.mean_net_per_bet
                                                          （文脈を細かく切ると偽の in-sample 利益→
                                                           OOS で控除率ぶんの損失。動画300%の機序）
      efficient.regime.mean_net_per_bet ≈ −控除率         （レジーム検出は効率市場で無価値）
      stale.regime.mean_net_per_bet > 0                  （対照群: 非効率なら同じ手法が勝つ）
      stale.bandit.oos.mean_net > 0                      （RL も非効率市場なら真の edge を掴む）
    """
    rng = np.random.default_rng(seed)
    out: dict = {"config": config, "takeout": config.takeout}

    for mode in (MODE_EFFICIENT, MODE_STALE):
        train = simulate_market(n_train, config, mode, rng)
        test = simulate_market(n_test, config, mode, rng)

        # (a) 粗い文脈のバンディット: 効率市場では abstain に収束（打たないのが最適）。
        bandit = train_bandit(train)
        # (b) 細かい偽文脈のバンディット: 過学習で in-sample だけ利益（＝backtest の嘘）。
        overfit = train_bandit(train, n_noise_bins=overfit_noise_bins)

        out[mode] = {
            "baseline": market_baseline(test),
            "bandit": {
                "in_sample": bandit.evaluate(train),
                "oos": bandit.evaluate(test),
            },
            "overfit_bandit": {
                "in_sample": overfit.evaluate(train),
                "oos": overfit.evaluate(test),
            },
            "regime": regime_strategy(test, config, threshold=regime_threshold),
        }
    return out
