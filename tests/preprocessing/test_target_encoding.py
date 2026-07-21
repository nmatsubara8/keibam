"""_target_encoding のリーク耐性・スムージングの単体テスト（PyCon A1/A2 中核）。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.preprocessing._target_encoding import (
    build_person_form_features,
    expanding_target_encode,
)


def test_strictly_past_and_same_date_block_excluded():
    """各行の encoding は「厳密に過去（date<自分）」のみ。同一日の他行も含めない（リーク遮断）。"""
    df = pd.DataFrame({
        "jockey_id": ["J", "J", "J", "J", "J", "K"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-03", "2024-01-04", "2024-01-01"],
        "_win": [1, 0, 1, 1, 0, 0],
    })
    te = expanding_target_encode(df, keys=["jockey_id"], target="_win", alpha=0.0)
    got = te.tolist()
    # J: d1=履歴なし(NaN), d2=過去{1}=1.0, d3×2=過去{1,0}=0.5(同日互いを含めない), d4=過去{1,0,1,1}=0.75
    expected = [np.nan, 1.0, 0.5, 0.5, 0.75, np.nan]
    for e, g in zip(expected, got):
        assert (np.isnan(e) and np.isnan(g)) or abs(e - g) < 1e-9


def test_no_leak_of_current_row():
    """自分自身の結果は encoding に絶対入らない（最終行を勝ちに変えても過去平均は不変）。"""
    base = pd.DataFrame({
        "e": ["A", "A", "A"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "_win": [1, 0, 0],
    })
    te0 = expanding_target_encode(base, ["e"], "_win", alpha=0.0).iloc[-1]
    flipped = base.copy()
    flipped.loc[2, "_win"] = 1  # 最終行の結果だけ変える
    te1 = expanding_target_encode(flipped, ["e"], "_win", alpha=0.0).iloc[-1]
    assert abs(te0 - te1) < 1e-12  # 過去平均は自分の結果に依存しない


def test_smoothing_pulls_sparse_categories_harder():
    """高い α は少数カテゴリ（n小）を全体平均へより強く縮める。"""
    rng = np.random.RandomState(0)
    d0 = pd.Timestamp("2024-01-01")
    rows = [(f"bg{i % 50}", d0 + pd.Timedelta(days=i % 30), int(rng.rand() < 0.1)) for i in range(500)]
    rows += [("A", d0 + pd.Timedelta(days=i), 1) for i in range(20)]  # A: 20戦全勝
    rows.append(("B", d0, 1))                                          # B: 1戦1勝
    rows += [("A", pd.Timestamp("2024-06-01"), 0), ("B", pd.Timestamp("2024-06-01"), 0)]
    d = pd.DataFrame(rows, columns=["jk", "date", "_win"])

    te_hi = expanding_target_encode(d, ["jk"], "_win", alpha=50.0)
    a_hi, b_hi = te_hi.iloc[-2], te_hi.iloc[-1]
    # 全体平均 ~0.1。生はどちらも 1.0 だが、n=1 の B の方が強く 0.1 側へ縮む。
    assert b_hi < a_hi
    assert b_hi < 0.3 < a_hi  # B は大きく縮み、A はまだ高い


def test_no_history_rows_fall_back_to_global_prior():
    """履歴ゼロの行は全体 prior（＝過去全体平均）になる（NaN ではなく安全な既定）。"""
    d = pd.DataFrame({
        "e": ["X", "Y", "Y"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "_win": [1, 0, 1],
    })
    te = expanding_target_encode(d, ["e"], "_win", alpha=10.0)
    # 行1(Y d2): Y は履歴なし → smoothed = prior_glob（d2 より前の全体平均 = {行0=1} = 1.0）
    assert abs(te.iloc[1] - 1.0) < 1e-9


def test_context_keys_filter_history():
    """context を keys に足すと、同じ context の過去だけで集計される。"""
    d = pd.DataFrame({
        "jk": ["A", "A", "A"],
        "race_type": ["芝", "ダート", "芝"],
        "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "_win": [1, 0, 0],
    })
    te = expanding_target_encode(d, ["jk", "race_type"], "_win", alpha=0.0)
    # 行2(A,芝,d3): 同context(A,芝)の過去= 行0{1} のみ（ダートの行1は無視）→ 1.0
    assert abs(te.iloc[2] - 1.0) < 1e-9


def test_build_person_form_features_skips_missing_columns():
    """必要な entity/context 列が無い spec は自動スキップし、あるものだけ列を作る。"""
    results = pd.DataFrame({
        "jockey_id": ["J1", "J1", "J2"],
        "date": ["2024-01-01", "2024-01-08", "2024-01-01"],
        "着順": [1, 3, 5],
    })
    out = build_person_form_features(results, alpha=10.0)
    assert "jockey_win_te" in out.columns and "jockey_place_te" in out.columns
    # trainer_id / owner_id / race_type / 開催 が無いのでそれらの spec は作られない
    assert "trainer_win_te" not in out.columns
    assert "jockey_win_te_by_type" not in out.columns
    assert len(out) == len(results)


def test_context_specs_produce_place_and_class_te():
    """DEFAULT_CONTEXT_SPECS で 開催/race_class の複勝率・勝率 TE 列が生成される。"""
    import pandas as pd

    from src.preprocessing._target_encoding import (
        DEFAULT_CONTEXT_SPECS,
        build_person_form_features,
    )

    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2024-01-01", "2024-01-08", "2024-01-15", "2024-01-22", "2024-01-29"]
            ),
            "開催": ["05", "05", "05", "05", "09"],
            "race_class": ["G1", "G1", "G1", "未勝利", "未勝利"],
            "着順": [1, 3, 1, 5, 2],
        }
    )
    out = build_person_form_features(
        df, specs=DEFAULT_CONTEXT_SPECS, rank_col="着順", alpha=10.0
    )
    expected = {"place_place_te", "place_win_te", "race_class_place_te", "race_class_win_te"}
    assert expected.issubset(set(out.columns))
    assert out.notna().all().all()
    assert ((out >= 0.0) & (out <= 1.0)).all().all()


def test_context_te_is_leak_free_for_place():
    """開催 TE は「厳密に過去」のみ参照する（自分の結果を含めない）。"""
    import pandas as pd

    from src.preprocessing._target_encoding import expanding_target_encode

    # 同一開催 '05' の 3 レース。最終行の TE は過去 2 行（勝1/複2 のうち）だけで決まり、
    # 自分（着順1=勝ち）を含めない。alpha=0 で純粋な過去平均。
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2024-01-01", "2024-01-08", "2024-01-15"]),
            "開催": ["05", "05", "05"],
            "_win": [0.0, 0.0, 1.0],
        }
    )
    te = expanding_target_encode(df, keys=["開催"], target="_win", alpha=0.0)
    # 最終行: 過去2行の _win 平均 = 0.0（自分の 1.0 は含めない）
    assert te.iloc[-1] == 0.0
