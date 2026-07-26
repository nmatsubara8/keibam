"""results 自己結合による horse_results 再構成アダプタの単体テスト。"""

from __future__ import annotations

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols as HRCols
from src.preprocessing._horse_features import (
    add_pace_stats,
    add_recent_form_stats,
    add_speed_figure_stats,
    build_horse_results_from_results,
)


def _results_fixture():
    """H の3走（勝ち→負け→当該レース）を含む results（race_info マージ済み想定）。"""
    return pd.DataFrame(
        {
            "horse_id": ["H", "H", "H"],
            "date": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
            "着順": [1, 5, 3],
            "n_horses": [12, 10, 14],
            "course_len": [16.0, 16.0, 20.0],
            "race_type": ["芝", "芝", "ダート"],
            "ground_state1": ["良", "稍重", "良"],
            "ground_state2": ["良", "良", "重"],
            "開催": pd.array([5, 5, 6], dtype="Int64"),
            "斤量": [55.0, 55.0, 57.0],
            "jockey_id": ["j1", "j1", "j2"],
        },
        index=pd.Index(["202401010101", "202402010101", "202403010101"], name="race_id"),
    )


def test_reconstruct_columns_and_index():
    recon = build_horse_results_from_results(_results_fixture())
    assert recon.index.name == "horse_id"
    # 率・適性系に必要な列は揃う
    for c in [HRCols.RANK, HRCols.N_HORSES, "course_len", "race_type",
              HRCols.KINRYO, HRCols.PLACE, HRCols.JOCKEY, HRCols.GROUND_STATE, "date"]:
        assert c in recon.columns, c
    # ページ固有列（通過/タイム/スピード指数）は持たない → pace/speed は自動スキップされる
    assert "first_corner" not in recon.columns
    assert "speed_figure" not in recon.columns


def test_effective_ground_turf_vs_dirt():
    recon = build_horse_results_from_results(_results_fixture())
    # 芝走は ground_state1、ダート走は ground_state2 を採用
    g = recon[HRCols.GROUND_STATE].tolist()
    # 行順は results 順: 芝(良=gs1) / 芝(稍重=gs1) / ダート(重=gs2)
    assert g == ["良", "稍重", "重"]


def test_recent_form_from_reconstructed_past():
    """再構成フレームを date で過去に絞り、recent_form が正しい勝率を出す（リーク無し）。"""
    recon = build_horse_results_from_results(_results_fixture())
    # 当該レース 2024-03-01 より前だけ（＝勝ち+負けの2走）を過去フレームに
    past = recon[recon["date"] < pd.Timestamp("2024-03-01")]
    current = pd.DataFrame({"horse_id": ["H"], "course_len": [20.0]},
                           index=pd.Index(["202403010101"], name="race_id"))
    out = add_recent_form_stats(current, past)
    # 過去2走 {1着, 5着} → win_rate=0.5, place_rate(<=3)=0.5
    assert abs(out["win_rate_5R"].iloc[0] - 0.5) < 1e-9
    assert abs(out["place_rate_5R"].iloc[0] - 0.5) < 1e-9


def test_pace_and_speed_skip_without_page_columns():
    """通過/スピード指数の列が無いので pace/speed 特徴は付与されない（既存ガード）。"""
    recon = build_horse_results_from_results(_results_fixture())
    past = recon[recon["date"] < pd.Timestamp("2024-03-01")]
    current = pd.DataFrame({"horse_id": ["H"], "course_len": [20.0]},
                           index=pd.Index(["202403010101"], name="race_id"))
    out = add_pace_stats(current.copy(), past)
    assert "pace_median" not in out.columns  # first_corner 不在でスキップ
    out2 = add_speed_figure_stats(current.copy(), past)
    assert "speed_fig_best" not in out2.columns  # speed_figure 不在でスキップ


def test_empty_results_returns_empty():
    assert build_horse_results_from_results(pd.DataFrame()).empty
