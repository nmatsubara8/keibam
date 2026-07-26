"""①.5a ファクター事前計算層のテスト（前進安全・非ユニークindex・帯化）。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import NA, factor_series
from src.tuning._manji_factor_store import (
    MF_COLS,
    build_factor_table,
    build_recent_form_features,
)


def _featured():
    """race_id 非ユニーク（1レース複数馬）の最小 featured。

    馬A: r1(着1,単5.0) → r2(着3,単4.0) → r3(着2,単6.0)
    馬B: r1(着2,単3.0) → r3(着1,単2.0)
    """
    rows = [
        # race_id, horse_id, date,        馬番, 着順, 単勝
        ("r1", "A", "2024-01-06", 1, 1, 5.0),
        ("r1", "B", "2024-01-06", 2, 2, 3.0),
        ("r2", "A", "2024-01-13", 1, 3, 4.0),
        ("r3", "A", "2024-01-20", 1, 2, 6.0),
        ("r3", "B", "2024-01-20", 3, 1, 2.0),
    ]
    df = pd.DataFrame(rows, columns=["race_id", "horse_id", "date",
                                     ResultsCols.UMABAN, ResultsCols.RANK, ResultsCols.TANSHO_ODDS])
    return df.set_index("race_id")


def _row(mf, featured, horse, race_id):
    """(horse, race_id) の mf 行を1つ取り出す（位置対応の検証込み）。"""
    mask = (featured["horse_id"].to_numpy() == horse) & (featured.index.to_numpy() == race_id)
    idx = np.flatnonzero(mask)
    assert len(idx) == 1
    return mf.iloc[idx[0]]


def test_recent_form_is_forward_only_excludes_current_race():
    featured = _featured()
    mf = build_recent_form_features(featured)
    assert list(mf.columns) == list(MF_COLS)
    assert len(mf) == len(featured)

    # 馬Aのデビュー戦 r1: 過去走なし → 全 mf は NaN（当該走を含めない）
    a1 = _row(mf, featured, "A", "r1")
    assert np.isnan(a1["mf_recent3_avg_rank"])
    assert np.isnan(a1["mf_career_winrate"])

    # 馬Aの r2: 過去=[r1(着1,単5)] のみ
    a2 = _row(mf, featured, "A", "r2")
    assert a2["mf_recent3_avg_rank"] == 1.0          # mean(着1)
    assert a2["mf_recent3_recovery"] == 5.0          # mean(5.0*1)
    assert a2["mf_career_winrate"] == 1.0            # 1勝/1走
    assert a2["mf_career_n"] == 1

    # 馬Aの r3: 過去=[r1(着1,単5), r2(着3,単4)]（当該 r3 は除外）
    a3 = _row(mf, featured, "A", "r3")
    assert a3["mf_recent3_avg_rank"] == 2.0          # mean(1,3)
    assert a3["mf_recent3_recovery"] == 2.5          # mean(5, 0)
    assert a3["mf_career_winrate"] == 0.5            # 1勝/2走
    assert a3["mf_career_n"] == 2


def test_recent_form_handles_nonunique_race_id_index():
    """race_id が重複する index でも各馬の履歴が混ざらない（位置対応が正しい）。"""
    featured = _featured()
    mf = build_recent_form_features(featured)
    # 馬B の r3: 過去=[r1(着2,単3)] のみ。馬A の r1/r2 と混ざらないこと。
    b3 = _row(mf, featured, "B", "r3")
    assert b3["mf_recent3_avg_rank"] == 2.0          # 馬B の r1 着2 のみ
    assert b3["mf_recent3_recovery"] == 0.0          # 3.0*0（着2＝非当選）
    # 馬B の r1（デビュー）は NaN
    b1 = _row(mf, featured, "B", "r1")
    assert np.isnan(b1["mf_recent3_avg_rank"])


def test_build_factor_table_keys_and_history_factors():
    featured = _featured()
    table = build_factor_table(featured)
    # キー列と行数
    assert {"race_id", "馬番", "horse_id", "date"} <= set(table.columns)
    assert len(table) == len(featured)
    # 履歴依拠因子が帯化されて列に入る
    for f in ("recent3_form", "recent3_recovery", "career_form", "career_recovery"):
        assert f in table.columns

    # 馬A r2（recent3_avg_rank=1.0）→ form "good"
    m = (table["horse_id"] == "A") & (table["race_id"] == "r2")
    assert table.loc[m, "recent3_form"].iloc[0] == "good"
    # 馬A r1（デビュー・履歴なし）→ na
    m1 = (table["horse_id"] == "A") & (table["race_id"] == "r1")
    assert table.loc[m1, "recent3_form"].iloc[0] == NA


def test_history_factor_functions_na_without_columns():
    """mf_* 列が無い featured では履歴因子は na を返す（既存の列不在スキップ作法）。"""
    df = pd.DataFrame(
        {ResultsCols.UMABAN: [1, 2]},
        index=pd.Index(["r1", "r1"], name="race_id"),
    )
    for f in ("recent3_form", "career_recovery"):
        assert (factor_series(df, f) == NA).all()
