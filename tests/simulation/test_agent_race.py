"""エージェントベース競馬シミュレーション＋モンテカルロのコアテスト。

決定論性（seed固定）・確率の整合（勝率和=1・複勝≥勝率）・単調性（能力↑→勝率↑）・
機構の効き（スタミナ枯渇でハイペース逃げが差しに沈む）を固定する。
"""
from __future__ import annotations

import numpy as np

from src.simulation._agent_race import (
    SimConfig,
    field_from_arrays,
    monte_carlo,
)


def test_probabilities_are_consistent():
    field = field_from_arrays([1.0, 1.0, 1.0, 1.0], ["front", "stalker", "closer", "stalker"])
    r = monte_carlo(field, n_sim=500, seed=1)
    assert np.isclose(r["win"].sum(), 1.0)             # 勝率は1レースで1頭 → 和=1
    assert (r["place"] >= r["win"] - 1e-9).all()       # 複勝率 ≥ 勝率
    assert r["finish_counts"].sum() == 500 * 4          # 全 sim×全馬の着順が記録される


def test_deterministic_with_seed():
    field = field_from_arrays([1.0, 0.9, 1.1], ["front", "stalker", "closer"])
    a = monte_carlo(field, n_sim=300, seed=7)
    b = monte_carlo(field, n_sim=300, seed=7)
    assert np.array_equal(a["finish_counts"], b["finish_counts"])
    # 異なる seed では（ほぼ確実に）異なる
    c = monte_carlo(field, n_sim=300, seed=8)
    assert not np.array_equal(a["finish_counts"], c["finish_counts"])


def test_higher_ability_wins_more():
    # 能力チャンネルを分離するためスタミナは潤沢（枯渇が支配しない）にする。
    # （実運用では ability と stamina は特徴量から相関して推定される＝強い馬は持続もする）
    field = field_from_arrays([1.3, 1.0, 0.7], ["stalker", "stalker", "stalker"],
                              stamina=[5.0, 5.0, 5.0], noise=[0.02, 0.02, 0.02])
    r = monte_carlo(field, n_sim=1500, seed=3)
    # 平均着順が能力順に単調（勝率は上位馬に集中しやすく 0 同士の比較になり得るので mean_rank を使う）
    assert r["mean_rank"][0] < r["mean_rank"][1] < r["mean_rank"][2]
    assert r["win"][0] >= r["win"][1] >= r["win"][2] and r["win"][0] > 0.5


def test_stamina_lets_closer_beat_burning_frontrunner():
    # ハイペースを作るため逃げ2頭 + 追込1頭。逃げはスタミナ低め・追込は高め。
    # 機構が効けば「前半飛ばした逃げがバテ、同能力の追込が差す」＝追込の勝率が逃げ1頭を上回る。
    field = field_from_arrays(
        ability=[1.0, 1.0, 1.0],
        style_names=["front", "front", "closer"],
        stamina=[0.6, 0.6, 1.4],       # 逃げは早枯れ、追込は余力
        noise=[0.03, 0.03, 0.03],
    )
    cfg = SimConfig(stamina_cost=0.03)  # 消耗を効かせる
    r = monte_carlo(field, n_sim=2000, seed=5, cfg=cfg)
    # 追込(idx2)の勝率が、各逃げ(idx0,1)の勝率より高い＝展開（スタミナ切れ）の再現
    assert r["win"][2] > r["win"][0]
    assert r["win"][2] > r["win"][1]


def test_field_from_arrays_defaults():
    f = field_from_arrays([1.0, 1.0], ["front", "closer"])
    assert f.n == 2
    assert list(f.style) == [0, 2]
    assert (f.stamina == 1.0).all()


def test_pace_intensity_raises_early_pace_shape():
    # pace_intensity が robust に効かせるのは「場のペース形(序盤−終盤速度)」。高いほど前傾。
    # 注: これで『前傾→差し有利』の帰結まで robust に反転はしない。誰が得するかはスタミナ較正に
    #   カオス的に敏感で、薄い物理では再現性が出ない——という負の結果を sim_pace_inject.py の
    #   忠実度(2)で経験的に確認する。ここでは注入レバーが確実に効かせるペース"水準"の向きを固定。
    field = field_from_arrays(
        ability=[1.0, 1.0, 1.0, 1.0],
        style_names=["front", "front", "closer", "closer"],
        stamina=[1.0, 1.0, 1.0, 1.0], noise=[0.02, 0.02, 0.02, 0.02],
    )

    def _shape(it):
        r = monte_carlo(field, n_sim=1500, seed=11,
                        cfg=SimConfig(stamina_cost=0.03, pace_intensity=it),
                        track_dynamics=True)
        e, l = r["early_speed"], r["late_speed"]
        return (e - l) / (e + l + 1e-9)

    # 高 intensity ほど序盤が速い＝前傾度が大きい（注入レバーの符号が正しい）
    assert _shape(1.30) > _shape(0.85)


def test_pace_predictor_intensity_sign_and_default():
    import numpy as np

    from src.simulation._pace_model import PacePredictor
    # 特徴0がペースを正に効かせる合成データ。学習後、高特徴→intensity>1、低→<1。
    rng = np.random.default_rng(0)
    X = rng.normal(size=(400, 3))
    y = 2.0 * X[:, 0] + 0.1 * rng.normal(size=400)     # pace_diff は特徴0に比例
    p = PacePredictor(gain=0.25).fit(X, y)
    hi = p.predict_intensity([2.0, 0.0, 0.0])
    lo = p.predict_intensity([-2.0, 0.0, 0.0])
    assert hi > 1.0 > lo                                # 前傾予測→強め、後傾予測→弱め
    assert 1.0 - 0.25 - 1e-6 <= lo and hi <= 1.0 + 0.25 + 1e-6   # gain 帯に収まる
    # 未学習器は中立(1.0)
    assert PacePredictor().predict_intensity([1.0, 2.0, 3.0]) == 1.0


def test_gate_inside_secures_forward_early_position():
    """同能力・同脚質でゲート(枠順)だけ違えば、内枠(gate=0)が序盤で前を取る。

    ゲート効果を単離するため全馬 stalker・能力一定。gate は内→外に単調。
    序盤位置順位(early_pos_rank: 0=先頭)が gate と正相関＝内ほど前、を確認する。
    """
    from src.simulation._agent_race import RaceField, STYLE_STALKER

    n = 8
    field = RaceField(
        ability=np.ones(n),
        style=np.full(n, STYLE_STALKER),
        stamina=np.ones(n),
        noise=np.full(n, 0.02),
        gate=np.linspace(0.0, 1.0, n),      # 0=最内 .. 1=最外
    )
    r = monte_carlo(field, n_sim=800, seed=3, ability_sigma=0.0, track_dynamics=True)
    epr = r["early_pos_rank"]               # 0=先頭
    # 内(gate小)ほど早い位置(epr小) → gate と epr は正相関
    corr = np.corrcoef(np.linspace(0, 1, n), epr)[0, 1]
    assert corr > 0.5, f"gate→序盤位置の相関が弱い: {corr:.3f}"


def test_gate_neutral_default_is_backward_compatible():
    """gate 未指定(None→中立0.5)なら枠効果ゼロ＝従来と同一挙動（決定論再現）。"""
    f_none = field_from_arrays([1.0, 1.2, 0.9, 1.1], ["front", "stalker", "closer", "stalker"])
    a = monte_carlo(f_none, n_sim=300, seed=7)
    b = monte_carlo(f_none, n_sim=300, seed=7)
    assert np.allclose(a["win"], b["win"])           # 決定論
    # 中立 gate は全馬同値 → ゲート項 (0.5-gate)=0 で vt 不変
    assert np.allclose(f_none.gate, 0.5)


def test_narrow_course_congests_front_slows_early_speed():
    """コース幅が狭い(定員小)ほど、多頭の先行が前列で詰まり序盤速度が落ちる。

    全馬 front・同能力で前に殺到させ、狭コース(course_width小→定員小)と
    広コース(定員大)の序盤平均速度を比較。狭い方が混雑で遅い＝体幅/コース幅の定員が効く。
    """
    from src.simulation._agent_race import RaceField, STYLE_FRONT

    n = 12
    base = dict(ability=np.ones(n), style=np.full(n, STYLE_FRONT),
                stamina=np.full(n, 5.0), noise=np.full(n, 0.01))
    narrow = monte_carlo(RaceField(**base), n_sim=400, seed=2, ability_sigma=0.0,
                         cfg=SimConfig(course_width=2.2), track_dynamics=True)   # 定員≈2
    wide = monte_carlo(RaceField(**base), n_sim=400, seed=2, ability_sigma=0.0,
                       cfg=SimConfig(course_width=100.0), track_dynamics=True)   # 定員≈91
    assert narrow["early_speed"] < wide["early_speed"], (
        f"狭コースで序盤が遅くならない: narrow={narrow['early_speed']:.4f} wide={wide['early_speed']:.4f}")


def test_turn_apt_orders_advantaged_over_disadvantaged():
    """回り適性は 得意(turn_apt>0) < 中立 < 不得意(turn_apt<0) の順で着順に効く（符号確認）。

    既定 turn_gain は小さく、ほぼ同能力だと混雑ダイナミクスに埋もれるため、機序（符号・順序）が
    見える大きめの turn_gain で検証する。
    """
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    n = 6
    apt = np.zeros(n); apt[0] = -1.5; apt[1] = +1.5     # 0=不得意, 1=得意, 2..5=中立
    field = RaceField(ability=np.ones(n), style=np.full(n, STYLE_STALKER),
                      stamina=np.full(n, 3.0), noise=np.full(n, 0.02), turn_apt=apt)
    r = monte_carlo(field, n_sim=2000, seed=4, ability_sigma=0.0,
                    cfg=SimConfig(turn_gain=0.3))
    neutral = float(np.mean([r["mean_rank"][i] for i in range(2, n)]))
    # 得意馬 < 中立 < 不得意馬
    assert r["mean_rank"][1] < neutral < r["mean_rank"][0]


def test_turn_apt_neutral_default_backward_compatible():
    """turn_apt 未指定(None→0)なら回り効果ゼロ＝従来と同一。"""
    f = field_from_arrays([1.0, 1.1, 0.9], ["front", "stalker", "closer"])
    assert np.allclose(f.turn_apt, 0.0)
    a = monte_carlo(f, n_sim=300, seed=8)
    b = monte_carlo(f, n_sim=300, seed=8)
    assert np.allclose(a["win"], b["win"])


def test_dt_invariance_finer_mesh_converges():
    """時間刻み dt を細かくしても（T·dt を一定に保てば）着順分布が保存される（dt 不変な積分）。

    加速度ノイズを √dt スケールにしたので、dt=1.0(T=100) と dt=0.25(T=400) で勝率がほぼ一致する。
    """
    f = field_from_arrays([1.25, 1.0, 0.85, 1.05, 0.9],
                          ["front", "stalker", "closer", "stalker", "closer"],
                          stamina=[1.2, 1.0, 1.1, 1.0, 0.9], noise=[0.05] * 5)
    coarse = monte_carlo(f, n_sim=4000, seed=11, ability_sigma=0.1,
                         cfg=SimConfig(T=100, dt=1.0))
    fine = monte_carlo(f, n_sim=4000, seed=11, ability_sigma=0.1,
                       cfg=SimConfig(T=400, dt=0.25))     # 同じ総時間 T·dt=100、4倍細かい
    # 勝率ベクトルが近い（細分化しても答えが変わらない＝dt に収束）
    assert np.max(np.abs(coarse["win"] - fine["win"])) < 0.05


def test_heavy_going_slows_field():
    """重い馬場(going>0)は全馬の序盤速度を下げる（going_speed_k>0）。"""
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    n = 8
    base = dict(ability=np.ones(n), style=np.full(n, STYLE_STALKER),
                stamina=np.full(n, 1.0), noise=np.full(n, 0.02))
    good = monte_carlo(RaceField(going=0.0, **base), n_sim=300, seed=2,
                       ability_sigma=0.0, track_dynamics=True)
    heavy = monte_carlo(RaceField(going=1.0, **base), n_sim=300, seed=2,
                        ability_sigma=0.0, track_dynamics=True)
    assert heavy["early_speed"] < good["early_speed"]


def test_going_apt_helps_on_heavy_not_on_good():
    """道悪巧者(going_apt>0)は重馬場で相対的に有利、良馬場では効かない（going 比例）。"""
    from src.simulation._agent_race import RaceField, STYLE_STALKER
    n = 6
    apt = np.zeros(n); apt[0] = +1.5; apt[1] = -1.5     # 0=道悪得意, 1=道悪不得意
    base = dict(ability=np.ones(n), style=np.full(n, STYLE_STALKER),
                stamina=np.full(n, 1.5), noise=np.full(n, 0.02), going_apt=apt)
    heavy = monte_carlo(RaceField(going=1.0, **base), n_sim=2000, seed=4,
                        ability_sigma=0.0, cfg=SimConfig(going_apt_gain=0.3))
    good = monte_carlo(RaceField(going=0.0, **base), n_sim=2000, seed=4,
                       ability_sigma=0.0, cfg=SimConfig(going_apt_gain=0.3))
    # 重馬場: 道悪得意(0) が 不得意(1) より上位
    assert heavy["mean_rank"][0] < heavy["mean_rank"][1]
    # 良馬場: going=0 なので馬場適性は無効果 → 0 と 1 はほぼ互角（差が重馬場より小さい）
    assert abs(good["mean_rank"][0] - good["mean_rank"][1]) < abs(heavy["mean_rank"][0] - heavy["mean_rank"][1])
