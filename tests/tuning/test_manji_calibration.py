"""Model 2 / Layer A（回収率較正＋普遍性フィルタ）の単体テスト。

合成データに「本物の回収率シグナル」と「一時期だけ効くノイズ」を仕込み、
- 本物は正の点数として採用され、
- 収縮で小標本が 0 に寄り、
- 普遍性フィルタで非定常ノイズが排除される
ことを決定的に検証する。

注: 1レース4頭(馬番1..4)、奇数(1,3)/偶数(2,4)。バケット回収率は「そのバケットの全馬を
フラット買い」なので、奇数が rate で勝つ→奇数の1頭だけ的中→奇数行の的中率=rate/2。
recovery = (rate/2)×odds。odds=4.0, rate=0.7 なら 0.35×4=1.4>1 のオーバーレイを仕込める。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from src.tuning._manji_calibration import bucket_recovery, calibrate_points


def _period(prefix, n_races, day0, *, odd_win_rate, odds, seed):
    """奇数馬番が odd_win_rate で勝つレース群。勝ち負けに関わらず単勝=odds。"""
    rng = np.random.default_rng(seed)
    rows, idx = [], []
    for i in range(n_races):
        rid = f"{prefix}_{i:04d}"
        odd_wins = rng.random() < odd_win_rate
        winner = int(rng.choice([1, 3])) if odd_wins else int(rng.choice([2, 4]))
        day = day0 + pd.Timedelta(days=i)
        for u in (1, 2, 3, 4):
            rows.append({"馬番": u, "着順": 1 if u == winner else u + 1,
                         "単勝": odds, "date": day})
            idx.append(rid)
    return pd.DataFrame(rows, index=idx)


def test_bucket_recovery_detects_signal():
    df = _period("A", 200, pd.Timestamp("2020-01-01"), odd_win_rate=0.7, odds=4.0, seed=1)
    rec = bucket_recovery(df, "umaban_parity")
    assert rec.loc["odd", "recovery"] > 1.0    # ≈ 0.35×4 = 1.4
    assert rec.loc["even", "recovery"] < 1.0   # ≈ 0.15×4 = 0.6
    assert rec.loc["odd", "n"] == 400          # 200レース×2頭(奇数)


def test_calibrate_assigns_positive_point_to_profitable_bucket():
    df = _period("A", 200, pd.Timestamp("2020-01-01"), odd_win_rate=0.7, odds=4.0, seed=2)
    pts = calibrate_points(df, ["umaban_parity"], min_n=30, universality_slices=1)
    assert "umaban_parity" in pts
    assert pts["umaban_parity"]["odd"] > 0
    assert pts["umaban_parity"].get("even", 0.0) < 0


def test_small_sample_bucket_is_dropped_by_min_n():
    df = _period("A", 5, pd.Timestamp("2020-01-01"), odd_win_rate=0.7, odds=4.0, seed=3)
    pts = calibrate_points(df, ["umaban_parity"], min_n=100, universality_slices=1)
    assert pts.get("umaban_parity", {}) == {}   # n<100 → 全バケット不採用


def test_universality_filter_drops_nonstationary_signal():
    # 3期間: 奇数が [0.8, 0.8, 0.2] で勝つ。前2期は odd 回収率>1、最終期は<1 で符号反転。
    # 全期間平均では odd 回収率>1（raw は odd を採用）だが、符号一致は 2/3=0.667<0.7 → フィルタで排除。
    p1 = _period("t0", 100, pd.Timestamp("2020-01-01"), odd_win_rate=0.8, odds=4.0, seed=4)
    p2 = _period("t1", 100, pd.Timestamp("2020-06-01"), odd_win_rate=0.8, odds=4.0, seed=5)
    p3 = _period("t2", 100, pd.Timestamp("2020-11-01"), odd_win_rate=0.2, odds=4.0, seed=6)
    df = pd.concat([p1, p2, p3])

    pts_raw = calibrate_points(df, ["umaban_parity"], min_n=30, universality_slices=1)
    pts_filtered = calibrate_points(df, ["umaban_parity"], min_n=30,
                                    universality_slices=3, min_agree=0.7)

    # raw は全期間平均で odd を profitable と判定（採用）
    assert pts_raw["umaban_parity"]["odd"] > 0
    # 普遍性フィルタは符号反転する odd を排除する
    assert "odd" not in pts_filtered.get("umaban_parity", {})


def test_calibrate_factor_weights_center_and_range():
    from src.tuning._manji_calibration import calibrate_factor_weights
    # 単一因子: z=0 → 1.0 中心（解像度を潰さない）
    p = _period("A", 400, pd.Timestamp("2020-01-01"), odd_win_rate=0.7, odds=4.0, seed=11)
    w = calibrate_factor_weights(p, ["umaban_parity"], min_n=30,
                                 universality_slices=1, min_side=50)
    assert w["umaban_parity"] == pytest.approx(1.0)   # 単因子は中立1.0


def test_calibrate_factor_weights_strong_outweighs_noise():
    import numpy as np
    from src.tuning._manji_calibration import calibrate_factor_weights
    # 奇数馬番=強シグナル、性別=ノイズ。強因子の重み > ノイズ因子の重み、範囲[0,2]。
    rng = np.random.default_rng(3)
    rows, idx = [], []
    for i in range(600):
        rid = f"R{i:04d}"
        day = pd.Timestamp("2020-01-01") + pd.Timedelta(days=i)
        odd_wins = rng.random() < 0.7
        winner = int(rng.choice([1, 3])) if odd_wins else int(rng.choice([2, 4]))
        for u in (1, 2, 3, 4):
            rows.append({"馬番": u, "着順": 1 if u == winner else u + 1, "単勝": 4.0,
                         "性別": rng.choice(["牡", "牝"]), "date": day})
            idx.append(rid)
    df = pd.DataFrame(rows, index=idx)
    w = calibrate_factor_weights(df, ["umaban_parity", "sex"], min_n=30,
                                 universality_slices=1, min_side=50)
    assert w["umaban_parity"] > w["sex"]              # 強因子が重い
    for v in w.values():
        assert 0.0 <= v <= 2.0                        # w_min..w_max
