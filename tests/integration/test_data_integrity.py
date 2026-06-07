"""クロステーブルデータ整合性テスト（§10 計画）。

スクレイピングバグを早期検出するため、テーブル間の参照整合性を検証する。
ローカルデータが存在しない場合（CI 環境）は自動スキップする。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.constants._local_paths import LocalPaths

_PATHS = LocalPaths()

# CI ではデータファイルが存在しないためスキップ
pytestmark = pytest.mark.skipif(
    not Path(_PATHS.RAW_RESULTS_PATH).exists(),
    reason="local race data not available (CI)",
)


@pytest.fixture(scope="module")
def race_results() -> pd.DataFrame:
    return pd.read_pickle(_PATHS.RAW_RESULTS_PATH)


@pytest.fixture(scope="module")
def peds() -> pd.DataFrame:
    if not Path(_PATHS.RAW_PEDS_PATH).exists():
        pytest.skip("peds data not available")
    return pd.read_pickle(_PATHS.RAW_PEDS_PATH)


def test_race_results_horse_id_not_null(race_results):
    """race_results の horse_id に欠損がないことを確認する。"""
    assert race_results["horse_id"].notna().all(), "race_results に horse_id の欠損が存在する"


def test_no_duplicate_horse_in_race(race_results):
    """同一レース内で同一馬が 2 行以上存在しないことを確認する。"""
    dupes = race_results.duplicated(subset=["horse_id"]).sum()
    # index が race_id であることを前提として (race_id, horse_id) の重複チェック
    df = race_results.copy()
    df["race_id"] = df.index
    dupes = df.duplicated(subset=["race_id", "horse_id"]).sum()
    assert dupes == 0, f"(race_id, horse_id) の重複が {dupes} 件存在する"


def test_horse_id_in_peds(race_results, peds):
    """race_results に登場する全 horse_id が peds に存在することを確認する。"""
    result_ids = set(race_results["horse_id"].dropna().astype(str))
    # peds が horse_id 列を持つ場合はその列から、インデックスの場合はインデックスから取得する
    if "horse_id" in peds.columns:
        peds_ids = set(peds["horse_id"].dropna().astype(str))
    else:
        peds_ids = set(peds.index.astype(str))
    missing = result_ids - peds_ids
    assert not missing, f"peds に存在しない horse_id が {len(missing)} 件ある: {list(missing)[:5]}"


def test_peds_sire_id_not_null(peds):
    """peds の peds_0（父 ID）に欠損がないことを確認する。"""
    if "peds_0" not in peds.columns:
        pytest.skip("peds_0 列が存在しない")
    assert peds["peds_0"].notna().all(), "peds_0（父 ID）に欠損が存在する"


def test_return_tables_race_ids_subset_of_results(race_results):
    """return_tables の race_id が race_results の race_id に含まれることを確認する。"""
    if not Path(_PATHS.RAW_RETURN_TABLES_PATH).exists():
        pytest.skip("return_tables data not available")
    return_tables = pd.read_pickle(_PATHS.RAW_RETURN_TABLES_PATH)
    results_race_ids = set(race_results.index.astype(str))
    return_race_ids = set(return_tables.index.astype(str))
    orphan = return_race_ids - results_race_ids
    assert not orphan, f"race_results に対応する race_id がない払戻データが {len(orphan)} 件: {list(orphan)[:5]}"
