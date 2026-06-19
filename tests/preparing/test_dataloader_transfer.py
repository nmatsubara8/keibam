"""DataLoader.transfer_temp_file のマージ/重複除去のリグレッションテスト。

kaisai_date_list（キー列 kaisai_data）が「単純追記」で重複蓄積していた問題の修正を検証する。
"""

import os

import pandas as pd
import pytest

from src.preparing.DataLoader import DataLoader


def _make_loader(tmp_path):
    """transfer_temp_file を動かす最小構成の DataLoader を返す。

    save_file_name は ./data/raw に存在しない名前にして、実 raw を読まないようにする。
    """
    temp_dir = tmp_path / "tmp"
    comp_dir = tmp_path / "comp"
    temp_dir.mkdir()
    comp_dir.mkdir()
    return DataLoader(
        alias="kaisai_date",  # csv_reader の else 分岐（素の read_csv）
        temp_save_file_name="temp_kaisai_date_table.csv",
        to_temp_location=str(temp_dir),
        to_location=str(comp_dir),
        save_file_name="test_kaisai_date_list_for_unittest.pkl",
    )


def test_transfer_dedups_kaisai_date(tmp_path):
    """既存 pkl が重複を含んでいても kaisai_data でユニーク化される。"""
    loader = _make_loader(tmp_path)

    # 既存（過去の単純追記で重複が蓄積した状態を模擬）
    existing = pd.DataFrame({"kaisai_data": [20080105, 20080105, 20080106, 20080112]})
    existing.to_pickle(loader.get_local_comp_file_path(loader.alias))

    # 新規スクレイプ分（既存と一部重複 + 新しい日付）
    new = pd.DataFrame({"kaisai_data": [20080112, 20080120]})
    new.to_csv(loader.get_local_temp_file_path(), index=False)

    loader.transfer_temp_file()

    out = pd.read_pickle(loader.get_local_comp_file_path(loader.alias))
    # 重複が消え、全日付がユニークに揃う
    assert out["kaisai_data"].duplicated().sum() == 0
    assert set(out["kaisai_data"]) == {20080105, 20080106, 20080112, 20080120}


def test_transfer_keeps_race_id_dedup(tmp_path):
    """race_id 系は従来どおりキーで重複除去される（回帰防止）。"""
    loader = _make_loader(tmp_path)
    loader.save_file_name = "test_race_id_list_for_unittest.pkl"

    # 実運用では既存 pkl・新規 CSV とも同じ csv 経路を通り型が一致するため int で統一する
    existing = pd.DataFrame({"race_id": [200801010101, 200801010102], "kaisai_date": [1, 1]})
    existing.to_pickle(loader.get_local_comp_file_path(loader.alias))

    new = pd.DataFrame({"race_id": [200801010102, 200801010103], "kaisai_date": [2, 2]})
    new.to_csv(loader.get_local_temp_file_path(), index=False)

    loader.transfer_temp_file()

    out = pd.read_pickle(loader.get_local_comp_file_path(loader.alias))
    assert out["race_id"].duplicated().sum() == 0
    assert set(out["race_id"]) == {200801010101, 200801010102, 200801010103}
    # 重複した race_id は新データ優先（keep="last"）
    row = out[out["race_id"] == 200801010102]
    assert row["kaisai_date"].iloc[0] == 2


def test_transfer_preserves_multirow_per_race(tmp_path):
    """results 系（1 レース複数馬）は race_id だけで潰さず全馬行を保持する（欠損バグ回帰）。"""
    loader = _make_loader(tmp_path)
    loader.save_file_name = "test_results_for_unittest.pkl"

    # 既存: レース 1 の 3 頭
    existing = pd.DataFrame(
        {"race_id": [1, 1, 1], "馬番": [1, 2, 3], "着順": [1, 2, 3]}
    )
    existing.to_pickle(loader.get_local_comp_file_path(loader.alias))

    # 新規: レース 2 の 2 頭
    new = pd.DataFrame({"race_id": [2, 2], "馬番": [1, 2], "着順": [1, 2]})
    new.to_csv(loader.get_local_temp_file_path(), index=False)

    loader.transfer_temp_file()

    out = pd.read_pickle(loader.get_local_comp_file_path(loader.alias))
    # レース1（3頭）+ レース2（2頭）= 5 行。race_id 単独 dedup なら 2 行に潰れていた。
    assert len(out) == 5
    assert (out["race_id"] == 1).sum() == 3
    assert (out["race_id"] == 2).sum() == 2


def test_transfer_rescrape_replaces_whole_race(tmp_path):
    """同一 race_id の再取得は、その race の既存行を丸ごと新データで置換する。"""
    loader = _make_loader(tmp_path)
    loader.save_file_name = "test_results_for_unittest.pkl"

    # 既存: レース 1 の 3 頭（古い）
    existing = pd.DataFrame(
        {"race_id": [1, 1, 1], "馬番": [1, 2, 3], "着順": [9, 9, 9]}
    )
    existing.to_pickle(loader.get_local_comp_file_path(loader.alias))

    # 再取得: レース 1 を 2 頭で（修正後）
    new = pd.DataFrame({"race_id": [1, 1], "馬番": [1, 2], "着順": [1, 2]})
    new.to_csv(loader.get_local_temp_file_path(), index=False)

    loader.transfer_temp_file()

    out = pd.read_pickle(loader.get_local_comp_file_path(loader.alias))
    # 古い 3 頭は消え、新しい 2 頭に置換（着順も新データ）
    assert len(out) == 2
    assert set(out["着順"]) == {1, 2}


@pytest.fixture(autouse=True)
def _cleanup_raw():
    """テスト用 save_file_name が万一 ./data/raw に作られても掃除する。"""
    yield
    for name in (
        "test_kaisai_date_list_for_unittest.pkl",
        "test_race_id_list_for_unittest.pkl",
        "test_results_for_unittest.pkl",
    ):
        p = os.path.join("./data/raw", name)
        if os.path.exists(p):
            os.remove(p)
