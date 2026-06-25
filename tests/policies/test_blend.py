"""ベンター2段目（対数線形プール合成・ΔR²）src.policies._blend のテスト。"""

import pytest

from src.policies import _blend as B
from src.policies._blend import BlendWeights


def _weighted_races(p_fund, p_public, true_dist, scale=3000):
    """true_dist の頻度で勝ち馬を割り当てた決定論的レース群（母集団 MLE 用）。"""
    races = []
    for h, p in true_dist.items():
        races.extend([(p_fund, p_public, h)] * round(p * scale))
    return races


class TestCombineLogpool:
    def test_alpha_only_is_fund(self):
        pf = {1: 0.6, 2: 0.3, 3: 0.1}
        pp = {1: 0.2, 2: 0.3, 3: 0.5}
        out = B.combine_logpool(pf, pp, 1.0, 0.0)
        assert out == pytest.approx(pf)  # 既に Σ=1

    def test_beta_only_is_public(self):
        pf = {1: 0.6, 2: 0.3, 3: 0.1}
        pp = {1: 0.2, 2: 0.3, 3: 0.5}
        out = B.combine_logpool(pf, pp, 0.0, 1.0)
        assert out == pytest.approx(pp)

    def test_normalized(self):
        out = B.combine_logpool({1: 0.5, 2: 0.5}, {1: 0.8, 2: 0.2}, 0.7, 0.5)
        assert sum(out.values()) == pytest.approx(1.0)

    def test_geometric_mean_direction(self):
        # 両者が同じ馬を支持 → 合成はさらに尖る（product of experts）
        pf = {1: 0.6, 2: 0.4}
        pp = {1: 0.6, 2: 0.4}
        out = B.combine_logpool(pf, pp, 1.0, 1.0)
        assert out[1] > pf[1]  # 0.6^2/(0.6^2+0.4^2) = 0.69 > 0.6


class TestFitBlend:
    def test_recovers_weights_favoring_fund(self):
        # 真の勝ち馬分布 = p_fund（ファンダが当たる）→ α が β より大きく出る
        pf = {1: 0.2, 2: 0.3, 3: 0.5}
        pp = {1: 0.5, 2: 0.3, 3: 0.2}
        races = _weighted_races(pf, pp, true_dist=pf)
        w = B.fit_blend(races)
        assert w.alpha > w.beta  # ファンダに重み

    def test_empty_returns_init(self):
        assert B.fit_blend([], init=(0.5, 0.5)) == BlendWeights(0.5, 0.5)


class TestPseudoR2:
    def test_perfect_model_r2_one(self):
        winners = [1, 2]
        probs = [{1: 1.0, 2: 0.0}, {1: 0.0, 2: 1.0}]
        ll = B.total_loglik(probs, winners)
        uni = B.uniform_loglik([2, 2])
        assert B.pseudo_r2(ll, uni) == pytest.approx(1.0, abs=1e-9)

    def test_uniform_model_r2_zero(self):
        winners = [1, 2]
        probs = [{1: 0.5, 2: 0.5}, {1: 0.5, 2: 0.5}]
        ll = B.total_loglik(probs, winners)
        uni = B.uniform_loglik([2, 2])
        assert B.pseudo_r2(ll, uni) == pytest.approx(0.0)


class TestDeltaR2:
    def test_informative_fund_positive_delta(self):
        # 真の勝ち馬分布 = p_fund（公衆とは別） → 合成は公衆を上回る ΔR²>0
        pf = {1: 0.2, 2: 0.3, 3: 0.5}
        pp = {1: 0.5, 2: 0.3, 3: 0.2}
        races = _weighted_races(pf, pp, true_dist=pf)
        w = B.fit_blend(races)
        diag = B.blend_diagnostic(races, w)
        assert diag["delta_r2"] > 0.02
        assert diag["r2_combined"] >= diag["r2_public"]

    def test_echo_fund_near_zero_delta(self):
        # ファンダ=公衆の写し、真の分布=公衆 → 合成は公衆を上回れない ΔR²≈0
        pp = {1: 0.5, 2: 0.3, 3: 0.2}
        pf = dict(pp)  # 完全な写し
        races = _weighted_races(pf, pp, true_dist=pp)
        w = B.fit_blend(races)
        diag = B.blend_diagnostic(races, w)
        assert abs(diag["delta_r2"]) < 0.02

    def test_empty_diagnostic(self):
        d = B.blend_diagnostic([], BlendWeights())
        assert d["n"] == 0 and d["delta_r2"] == 0.0


class TestPersist:
    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "blend.json")
        w = BlendWeights(0.6, 0.8)
        B.save_blend_weights(w, path)
        assert B.load_blend_weights(path) == w

    def test_missing_none(self, tmp_path):
        assert B.load_blend_weights(str(tmp_path / "x.json")) is None
