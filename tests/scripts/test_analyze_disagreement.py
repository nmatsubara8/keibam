"""analyze_disagreement.py の純関数（二頭差分結合・列選択・Wilson CI）の単体テスト。

featured 実データ(gitignore・巨大)なしで、合成 featured フレームに対し
  - `_two_horse_diff` が (race_id, 馬番) 二系統で本命を引き当て lgbm−market 差分を作る
  - レース内一定の列は文脈(実値)、馬ごとに違う列は差分 d_* に振り分ける
  - 疎な one-hot ダミー(race_class__*)を除外し ordinal race_class_level を残す
  - `_wilson` が二項割合の 95%CI を正しく返す
を検証する。
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pandas as pd

from src.constants._results_cols import ResultsCols

_MOD_PATH = Path(__file__).resolve().parents[2] / "scripts" / "analyze_disagreement.py"
_spec = importlib.util.spec_from_file_location("analyze_disagreement", _MOD_PATH)
ad = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ad)

UMA = ResultsCols.UMABAN  # '馬番'


def _featured():
    """合成 featured: 2レース。馬固有(jockey_win)・レース一定(dist/race_class_level)・疎ダミー・除外列。"""
    rows = [
        # R1: 馬番1(騎手0.30) 2(0.20) 3(0.10)  dist=1600 level=5
        ("R1", 1, 0.30, 1600, 5, 0, "x"),
        ("R1", 2, 0.20, 1600, 5, 0, "x"),
        ("R1", 3, 0.10, 1600, 5, 0, "x"),
        # R2: 馬番1(0.20) 2(0.05)  dist=2000 level=7  G1ダミー=1
        ("R2", 1, 0.20, 2000, 7, 1, "y"),
        ("R2", 2, 0.05, 2000, 7, 1, "y"),
    ]
    df = pd.DataFrame(
        rows, columns=[".rid", UMA, "jockey_win", "dist", "race_class_level",
                       "race_class__G1", "date"],
    ).set_index(".rid")
    df.index.name = "race_id"
    return df


def _disagreement():
    """不一致 CSV 相当: R1 は LGBM=1/市場=3、R2 は LGBM=2/市場=1。"""
    return pd.DataFrame({
        "race_id": ["R1", "R2"],
        "lgbm_top": [1, 2],
        "market_fav": [3, 1],
        "year": ["2025", "2026"],
        "lgbm_hit": [1, 0],
        "market_hit": [0, 1],
    })


def test_horse_feature_cols_excludes_sparse_onehot_keeps_ordinal():
    cols = ad._horse_feature_cols(_featured(), UMA)
    assert "jockey_win" in cols
    assert "dist" in cols
    assert "race_class_level" in cols          # ordinal は残す
    assert "race_class__G1" not in cols        # 疎な one-hot は除外
    assert UMA not in cols and "date" not in cols


def test_two_horse_diff_builds_lgbm_minus_market():
    df, race_cols, diff_cols = ad._two_horse_diff(_disagreement(), _featured(), UMA)
    # 馬固有 jockey_win は差分列に、レース一定 dist / race_class_level は文脈列に
    assert "d_jockey_win" in diff_cols
    assert "dist" in race_cols and "race_class_level" in race_cols
    assert "d_dist" not in df.columns          # 差分0のレース一定列は差分化しない
    # R1: 0.30(LGBM=1) − 0.10(市場=3) = +0.20 ; R2: 0.05(LGBM=2) − 0.20(市場=1) = −0.15
    d = df.set_index("race_id")["d_jockey_win"]
    assert d["R1"] == pytest_approx(0.20)
    assert d["R2"] == pytest_approx(-0.15)
    # 文脈列はどちらの本命でも同じ実値（レース内一定）
    assert df.set_index("race_id")["dist"].to_dict() == {"R1": 1600.0, "R2": 2000.0}


def test_two_horse_diff_missing_umaban_column_returns_empty():
    feat = _featured().drop(columns=[UMA])
    df, race_cols, diff_cols = ad._two_horse_diff(_disagreement(), feat, UMA)
    assert race_cols == [] and diff_cols == []


def test_wilson_ci_basic():
    p, lo, hi = ad._wilson(214, 388)
    assert p == pytest_approx(214 / 388)
    assert 0.50 < lo < p < hi < 0.61           # ユーザ手計算 ~[50.2%,60.0%] と整合
    # 全勝/空はクラッシュしない
    assert ad._wilson(0, 0) == (0.0, 0.0, 0.0)
    p1, lo1, hi1 = ad._wilson(10, 10)
    assert p1 == 1.0 and hi1 <= 1.0 + 1e-9


# pytest.approx の薄いラッパ（import 名の衝突回避のため関数化）
def pytest_approx(x, tol=1e-9):
    import pytest
    return pytest.approx(x, abs=tol)


# ---- shap_disagreement.summarize_delta_shap（純関数）のテスト ----
_SHAP_PATH = Path(__file__).resolve().parents[2] / "scripts" / "shap_disagreement.py"
_sspec = importlib.util.spec_from_file_location("shap_disagreement", _SHAP_PATH)
sd = importlib.util.module_from_spec(_sspec)
_sspec.loader.exec_module(sd)


def test_summarize_delta_shap_ranks_and_group_diff():
    import numpy as np
    # 3特徴 f0/f1/f2, 4レース。f0 が最も大きく振れる＝平均|Δ|最大。
    delta = np.array([
        [+2.0, +0.1, -0.2],   # 2025 LGBM勝ち
        [-2.0, -0.1, +0.2],   # 2025 市場勝ち
        [+3.0, +0.0, +0.1],   # 2026 LGBM勝ち
        [-3.0, +0.0, -0.1],   # 2026 市場勝ち
    ])
    feat = ["f0", "f1", "f2"]
    years = ["2025", "2025", "2026", "2026"]
    lgbm_won = [1, 0, 1, 0]
    s = sd.summarize_delta_shap(delta, feat, years, lgbm_won, topn=3, per_race_top=2)
    assert s["n_race"] == 4
    assert s["overall"][0][0] == "f0"                       # 最大平均|Δ|
    assert s["overall"][0][1] == pytest_approx((2 + 2 + 3 + 3) / 4)
    # ③ 群差: f0 は LGBM勝ちで +、市場勝ちで − → 群差が最大かつ正
    gd = {r[0]: r[1] for r in s["group_diff"]}
    assert gd["f0"] > 0 and abs(gd["f0"]) >= abs(gd["f1"]) and abs(gd["f0"]) >= abs(gd["f2"])
    # ④ 各レース上位2理由: レース0は f0 が最大
    assert s["per_race"][0][0][0] == "f0"
    assert len(s["per_race"][0]) == 2


def test_summarize_delta_shap_consistent_sign_across_years():
    import numpy as np
    # f0 は両年とも平均Δが負（符号一致）、f1 は年で符号反転。
    delta = np.array([
        [-1.0, +1.0, 0.0],   # 2025
        [-1.0, +1.0, 0.0],   # 2025
        [-2.0, -1.0, 0.0],   # 2026
        [-2.0, -1.0, 0.0],   # 2026
    ])
    s = sd.summarize_delta_shap(delta, ["f0", "f1", "f2"], ["2025", "2025", "2026", "2026"],
                                [1, 0, 1, 0], topn=3)
    cons = {r[0] for r in s["consistent"]}
    assert "f0" in cons          # 両年とも負＝符号一致
    assert "f1" not in cons      # 符号反転
    assert "f2" not in cons      # ゼロ（符号なし）
