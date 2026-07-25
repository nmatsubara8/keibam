"""近走詳細ファクター（出遅れ/不利/着差/逆トラック・逆馬場）のテスト。"""

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._manji_factors import NA, factor_series
from src.tuning._manji_factor_store import (
    build_factor_table,
    build_recent_form_features,
    compute_head2head,
)


def _feat(rows):
    df = pd.DataFrame(rows)
    return df.set_index("race_id")


def test_recent_deokure_and_trouble_from_history():
    # 馬A: r1 出遅れ有・不利有 → r2 では「近走に出遅れ/不利有」= yes
    rows = [
        {"race_id": "r1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 5, ResultsCols.TANSHO_ODDS: 10.0, "出遅れ": 1, "不利": 1},
        {"race_id": "r2", "horse_id": "A", "date": "2024-02-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 3.0, "出遅れ": 0, "不利": 0},
    ]
    feat = _feat(rows)
    mf = build_recent_form_features(feat)
    for c in feat.columns:
        pass
    view = feat.copy()
    for c in mf.columns:
        view[c] = mf[c].to_numpy()
    deo = factor_series(view, "recent_deokure")
    tro = factor_series(view, "recent_trouble")
    # r1(デビュー相当・過去なし)=na、r2=yes（前走r1で出遅れ・不利）
    idxA_r2 = np.flatnonzero((view["horse_id"].to_numpy() == "A") & (view.index.to_numpy() == "r2"))[0]
    assert deo.iloc[idxA_r2] == "yes"
    assert tro.iloc[idxA_r2] == "yes"


def test_recent_close_margin_buckets():
    # 馬A: r1 で勝ち馬から0.1秒差 → r2 で within02
    rows = [
        {"race_id": "r1", "horse_id": "W", "date": "2024-01-01", ResultsCols.UMABAN: 2,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 2.0, "time_seconds": 60.0},
        {"race_id": "r1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 2, ResultsCols.TANSHO_ODDS: 5.0, "time_seconds": 60.1},
        {"race_id": "r2", "horse_id": "A", "date": "2024-02-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 3.0, "time_seconds": 58.0},
    ]
    feat = _feat(rows)
    mf = build_recent_form_features(feat)
    view = feat.copy()
    for c in mf.columns:
        view[c] = mf[c].to_numpy()
    cl = factor_series(view, "recent_close")
    idxA_r2 = np.flatnonzero((view["horse_id"].to_numpy() == "A") & (view.index.to_numpy() == "r2"))[0]
    assert cl.iloc[idxA_r2] == "within02"  # 0.1秒差


def test_offsurface_form_contrarian():
    # 馬A: 過去ダートで1着(好走) → 今走が芝なら offsurf_good（逆トラック好走=減点対象）
    rows = [
        {"race_id": "d1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 3.0, "race_type": "ダート"},
        {"race_id": "t2", "horse_id": "A", "date": "2024-02-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 3, ResultsCols.TANSHO_ODDS: 4.0, "race_type": "芝"},
    ]
    feat = _feat(rows)
    mf = build_recent_form_features(feat)
    view = feat.copy()
    for c in mf.columns:
        view[c] = mf[c].to_numpy()
    off = factor_series(view, "offsurface_form")
    idxA_t2 = np.flatnonzero((view["horse_id"].to_numpy() == "A") & (view.index.to_numpy() == "t2"))[0]
    assert off.iloc[idxA_t2] == "offsurf_good"  # 今走芝×近走ダで好走


def test_head2head_from_past_meeting():
    # 過去 p1 で A(1着) が B(2着) に勝利 → 今走 R で再戦: A=favorite(割引), B=underdog(妙味)
    rows = [
        {"race_id": "p1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 2.0},
        {"race_id": "p1", "horse_id": "B", "date": "2024-01-01", ResultsCols.UMABAN: 2,
         ResultsCols.RANK: 2, ResultsCols.TANSHO_ODDS: 3.0},
        {"race_id": "R", "horse_id": "A", "date": "2024-02-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 2, ResultsCols.TANSHO_ODDS: 2.0},
        {"race_id": "R", "horse_id": "B", "date": "2024-02-01", ResultsCols.UMABAN: 2,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 4.0},
    ]
    feat = _feat(rows)
    s = compute_head2head(feat)
    a_R = np.flatnonzero((feat["horse_id"].to_numpy() == "A") & (feat.index.to_numpy() == "R"))[0]
    b_R = np.flatnonzero((feat["horse_id"].to_numpy() == "B") & (feat.index.to_numpy() == "R"))[0]
    assert s.iloc[a_R] == -1  # A は過去に勝ち → net −1（favorite）
    assert s.iloc[b_R] == +1  # B は過去に負け → net +1（underdog）
    # 過去対戦の無い p1 側は NaN
    a_p1 = np.flatnonzero((feat["horse_id"].to_numpy() == "A") & (feat.index.to_numpy() == "p1"))[0]
    assert np.isnan(s.iloc[a_p1])

    # factor_table(with_h2h=True) 経由で head2head 帯化
    table = build_factor_table(feat, ["head2head"], with_h2h=True)
    m = (table["race_id"] == "R")
    labs = dict(zip(table.loc[m, "馬番"], table.loc[m, "head2head"]))
    assert labs[1] == "favorite" and labs[2] == "underdog"


def test_prev_finish_and_rotation_resurrected_from_history():
    """元列が無くても、履歴から算出した mf_prev_rank / mf_interval で prev_finish・rotation が発火。"""
    rows = [
        {"race_id": "r1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 6, ResultsCols.TANSHO_ODDS: 10.0, "course_len": 1600},
        {"race_id": "r2", "horse_id": "A", "date": "2024-01-15", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 3.0, "course_len": 1400},
    ]
    feat = _feat(rows)
    mf = build_recent_form_features(feat)
    assert "mf_prev_rank" in mf.columns and "mf_interval" in mf.columns
    assert "mf_dist_change" in mf.columns  # course_len があるので算出
    view = feat.copy()
    for c in mf.columns:
        view[c] = mf[c].to_numpy()
    a_r2 = np.flatnonzero((view["horse_id"].to_numpy() == "A") & (view.index.to_numpy() == "r2"))[0]
    # r2 の前走(r1)は6着 → prev_finish=p6、間隔14日 → naka1_3、距離1400-1600=-200 → short
    assert factor_series(view, "prev_finish").iloc[a_r2] == "p6"
    assert factor_series(view, "rotation").iloc[a_r2] == "naka1_3"
    assert factor_series(view, "dist_change").iloc[a_r2] == "short"


def test_prev_travel_resurrected_from_history():
    """前走場所を履歴(mf_prev_place)から算出し、遠征ファクターが発火する。"""
    rows = [
        # r1: 東京(場05) → r2: 大井(場44)。r2 の前走は東京＝関東遠征 yes
        {"race_id": "202405010101", "horse_id": "A", "date": "2024-05-01",
         ResultsCols.UMABAN: 1, ResultsCols.RANK: 3, ResultsCols.TANSHO_ODDS: 5.0},
        {"race_id": "202444060101", "horse_id": "A", "date": "2024-06-01",
         ResultsCols.UMABAN: 1, ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 3.0},
    ]
    feat = _feat(rows)
    mf = build_recent_form_features(feat)
    assert "mf_prev_place" in mf.columns
    view = feat.copy()
    for c in mf.columns:
        view[c] = mf[c].to_numpy()
    a_r2 = np.flatnonzero((view["horse_id"].to_numpy() == "A")
                          & (view.index.to_numpy() == "202444060101"))[0]
    assert factor_series(view, "prev_kanto").iloc[a_r2] == "yes"    # 前走=東京(関東)
    assert factor_series(view, "prev_kansai").iloc[a_r2] == "no"
    # r1(デビュー相当・前走なし)は na
    a_r1 = np.flatnonzero((view["horse_id"].to_numpy() == "A")
                          & (view.index.to_numpy() == "202405010101"))[0]
    assert factor_series(view, "prev_kanto").iloc[a_r1] == NA


def test_recent_detail_factors_na_without_source_columns():
    feat = _feat([
        {"race_id": "r1", "horse_id": "A", "date": "2024-01-01", ResultsCols.UMABAN: 1,
         ResultsCols.RANK: 1, ResultsCols.TANSHO_ODDS: 2.0},
    ])
    # 元列(出遅れ/不利/time_seconds/race_type/ground)が無い → 全て na
    for f in ("recent_deokure", "recent_trouble", "recent_close",
              "offsurface_form", "offground_form"):
        assert (factor_series(feat, f) == NA).all()
