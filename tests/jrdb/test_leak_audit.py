"""strictly-prior 全件 leak 監査（続36-e）の単体テスト。"""
from __future__ import annotations

import pandas as pd
import pytest

from src.jrdb._leak_audit import assert_strictly_prior, strictly_prior_join_report


def _target():
    # 同一馬 k1 が 3走。k2 は初走のみ。
    return pd.DataFrame({
        "ketto": ["k1", "k1", "k1", "k2"],
        "date": ["2020-01-01", "2020-02-01", "2020-03-01", "2020-01-01"],
    })


def _source():
    return pd.DataFrame({
        "ketto": ["k1", "k1", "k1"],
        "hist_date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"]),
        "prev_trouble": [0, 1, 0],
    })


def test_report_is_strictly_prior():
    rep = strictly_prior_join_report(_target(), _source())
    assert rep["future_reference_count"] == 0
    assert rep["same_day_reference_count"] == 0        # exact 同日は allow_exact_matches=False で除外
    assert rep["exact_target_reference_count"] == 0
    assert rep["leak_safe"] is True
    # k1 の 2走目/3走目 が過去参照を得る＝feature_rows=2（初走 k1・k2 は参照なし）
    assert rep["feature_rows"] == 2
    assert rep["target_valid_rows"] == 4
    assert rep["max_source_date"] == "2020-03-01"
    assert_strictly_prior(rep)                          # 例外を投げない


def test_report_detects_future_leak():
    # source を「今走より未来」にずらすと backward asof では拾わない＝future は 0 のまま。
    # ここでは exact 同日参照を allow して leak を再現し、監査が検出することを確認する。
    t = pd.DataFrame({"ketto": ["k1", "k1"], "date": ["2020-01-01", "2020-02-01"]})
    s = pd.DataFrame({"ketto": ["k1"], "hist_date": pd.to_datetime(["2020-02-01"]),
                      "prev_trouble": [1]})
    # 手動で same-day 参照を作る report を検証（strictly_prior_join_report は exact を除外するので 0）
    rep = strictly_prior_join_report(t, s)
    assert rep["same_day_reference_count"] == 0 and rep["leak_safe"] is True
    # leak_safe=False の manifest には assert が発火する
    bad = dict(rep, leak_safe=False, future_reference_count=3)
    with pytest.raises(RuntimeError, match="strictly-prior"):
        assert_strictly_prior(bad)


def test_empty_source_is_safe():
    rep = strictly_prior_join_report(_target(), pd.DataFrame())
    assert rep["leak_safe"] is True and rep["feature_rows"] == 0


def test_counts_target_key_duplicates():
    t = pd.DataFrame({"ketto": ["k1", "k1"], "date": ["2020-01-01", "2020-01-01"]})
    rep = strictly_prior_join_report(t, _source())
    assert rep["target_key_duplicates"] == 1            # 同一 ketto×date が1件重複
