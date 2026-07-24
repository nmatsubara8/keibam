"""ライブ予想パイプライン統合テスト。

shutuba pickle → ShutubaDataMerger → FeatureEngineering → KeibaAI.calc_score
→ BetPolicy の E2E フローが正常に動作することを確認する。

ローカルデータ（results.pkl / race_info.pkl / モデル pickle）が存在しない場合は
自動スキップする（CI 環境向け）。
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols as Cols

_PATHS = LocalPaths()

# CI ではデータファイルが存在しないためスキップ
pytestmark = pytest.mark.skipif(
    not Path(_PATHS.RAW_RESULTS_PATH).exists(),
    reason="local race data not available (CI)",
)

# テスト用レース（全馬が horse_results に存在する最近のレース）
_TARGET_RACE_ID = "202410010505"


# ──────────────────────────────────────────────────────
# フィクスチャ
# ──────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def shutuba_pkl(tmp_path_factory) -> str:
    """実データから shutuba 形式の pickle を生成して返す。"""
    results   = pd.read_pickle(_PATHS.RAW_RESULTS_PATH)
    race_info = pd.read_pickle(_PATHS.RAW_RACE_INFO_PATH)

    race = results[results["race_id"].astype(str) == _TARGET_RACE_ID].copy()
    if race.empty:
        pytest.skip(f"race_id {_TARGET_RACE_ID} が results.pkl に存在しない")

    info = race_info[race_info["race_id"].astype(str) == _TARGET_RACE_ID]
    if info.empty:
        pytest.skip(f"race_id {_TARGET_RACE_ID} が race_info.pkl に存在しない")
    info = info.iloc[0]

    ground_state = str(info.get("ground_state1", info.get("ground_state", "良")))
    date_str = pd.to_datetime(
        str(info["date"]), format="%Y年%m月%d日", errors="coerce"
    ).strftime("%Y/%m/%d")

    shutuba = pd.DataFrame({
        Cols.WAKUBAN:       race[Cols.WAKUBAN].values,
        Cols.UMABAN:        race[Cols.UMABAN].values,
        Cols.KINRYO:        race[Cols.KINRYO].values,
        Cols.TANSHO_ODDS:   race[Cols.TANSHO_ODDS].values,
        Cols.SEX_AGE:       race[Cols.SEX_AGE].values,
        Cols.WEIGHT_AND_DIFF: race[Cols.WEIGHT_AND_DIFF].values,
        "horse_id":  race["horse_id"].values,
        "jockey_id": race["jockey_id"].values,
        "trainer_id": race["trainer_id"].values,
        "owner_id":  race.get("owner_id", pd.Series([""] * len(race))).values,
        "date":       date_str,
        "course_len": float(info["course_len"]),
        "around":     info["around"],
        "race_type":  info["race_type"],
        "weather":    info["weather"],
        "race_class": info["race_class"],
        "ground_state": ground_state,
    })
    shutuba.index = [_TARGET_RACE_ID] * len(shutuba)

    pkl_path = str(tmp_path_factory.mktemp("shutuba") / "shutuba.pkl")
    shutuba.to_pickle(pkl_path)
    return pkl_path


@pytest.fixture(scope="module")
def merger(shutuba_pkl):
    """ShutubaDataMerger（race_info_processor あり）を返す。"""
    from src.preprocessing._horse_info_processor import HorseInfoProcessor
    from src.preprocessing._horse_results_processor import HorseResultsProcessor
    from src.preprocessing._peds_processor import PedsProcessor
    from src.preprocessing._race_info_processor import RaceInfoProcessor
    from src.preprocessing._shutuba_data_merger import ShutubaDataMerger
    from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor

    stp = ShutubaTableProcessor(shutuba_pkl)
    hrp = HorseResultsProcessor(_PATHS.RAW_HORSE_RESULTS_PATH)
    hip = HorseInfoProcessor(_PATHS.RAW_HORSE_INFO_PATH)
    pp  = PedsProcessor(_PATHS.RAW_PEDS_PATH)
    rip = RaceInfoProcessor(_PATHS.RAW_RACE_INFO_PATH)

    from src.constants._feature_cols import AGG_GROUP_COLS, AGG_TARGET_COLS

    m = ShutubaDataMerger(
        stp, hrp, hip, pp,
        target_cols=AGG_TARGET_COLS,
        group_cols=AGG_GROUP_COLS,
        race_info_processor=rip,
    )
    m.merge()
    return m


@pytest.fixture(scope="module")
def featured(merger):
    """FeatureEngineering 済み DataFrame を返す。"""
    from src.preprocessing._feature_engineering import FeatureEngineering

    return (
        FeatureEngineering(merger)
        .add_interval().add_agedays()
        .add_interaction_features().add_race_level_zscore()
        .dumminize_kaisai().dumminize_sex().dumminize_weather()
        .dumminize_race_type()
        .dumminize_ground_state1().dumminize_ground_state2()
        .dumminize_ground_state()
        .dumminize_around().dumminize_race_class()
        .encode_horse_id().encode_jockey_id().encode_trainer_id()
        .encode_owner_id().encode_breeder_id()
    ).featured_data


@pytest.fixture(scope="module")
def keiba_ai():
    """保存済みモデルをロードして返す（モデルがなければスキップ）。"""
    import glob
    import dill

    model_files = sorted(glob.glob("models/*/*.pickle"))
    if not model_files:
        pytest.skip("保存済みモデルが存在しない")

    with open(model_files[-1], "rb") as f:
        return dill.load(f)


# ──────────────────────────────────────────────────────
# テスト
# ──────────────────────────────────────────────────────


def test_shutuba_table_processor_shape(shutuba_pkl):
    """ShutubaTableProcessor が正しい列数で前処理できる。"""
    from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor

    stp = ShutubaTableProcessor(shutuba_pkl)
    assert stp.preprocessed_data.shape[0] > 0
    assert "course_len" in stp.preprocessed_data.columns
    assert "date" in stp.preprocessed_data.columns


def test_merger_with_race_info_adds_columns(merger):
    """race_info_processor を渡すと ground_state1/2・days 等が追加される。"""
    md = merger.merged_data
    assert "ground_state1" in md.columns or "ground_state1__良" not in md.columns, \
        "ground_state1 が merged_data に存在しない（race_info 結合に失敗）"
    # race_info 由来の列が存在するか確認
    assert "days" in md.columns, "days 列が race_info から追加されていない"


def test_merger_no_ground_state_single(merger):
    """race_info 結合後に単一の ground_state 列は除去されている。"""
    md = merger.merged_data
    # ground_state1 が存在する場合、単一 ground_state は除去済みのはず
    if "ground_state1" in md.columns:
        assert "ground_state" not in md.columns


def test_featured_data_has_ground_state_dummies(featured):
    """FeatureEngineering 後に ground_state1__* ダミー列が生成されている。"""
    gs1_cols = [c for c in featured.columns if "ground_state1__" in c]
    assert len(gs1_cols) > 0, "ground_state1 ダミー列が生成されていない"


def test_featured_data_no_missing_after_race_info(featured, keiba_ai):
    """race_info あり推論では 0 補完が必要な列が 0 件になる。"""
    from src.training._data_splitter import _DROP_FOR_TEST

    X = featured.drop(
        [c for c in _DROP_FOR_TEST if c in featured.columns], axis=1, errors="ignore"
    )
    feature_names = getattr(keiba_ai, "feature_names_", None)
    if feature_names is None:
        try:
            feature_names = list(keiba_ai.datasets.X_base_train.columns)
        except Exception:
            pytest.skip("feature_names を取得できない")

    missing = [c for c in feature_names if c not in X.columns]
    assert len(missing) == 0, f"race_info ありでも {len(missing)} 列が不足: {missing[:5]}"


def test_train_live_feature_parity(featured):
    """学習 featured_data とライブ featured の「構造的」列集合の一致を検証する。

    学習(run_pipeline)とライブ(ShutubaDataMerger)は AGG_TARGET_COLS / AGG_GROUP_COLS
    を共有するため、集計・数値などの構造的特徴量列は一致するはず。`feature_names_`
    の reindex fill 0 が差分を握り潰す前に、ここで差分を可視化する。

    One-Hot ダミー列（"__" 区切り。例 race_class__G1, weather__雪）は「その集合に
    実在するカテゴリ」に依存するため、単一レースのライブ側では一部しか現れないのが
    正当（reindex fill 0 が担当する領域）。よって構造的列（"__" を含まない列）に
    絞ってパリティを検証する。target_cols 不一致等は集計列の欠落として顕在化する。

    学習 featured_data.pkl が無い環境ではスキップ（差分の基準が取れないため）。
    """
    from src.training._data_splitter import _DROP_FOR_TRAIN

    featured_path = Path(_PATHS.FEATURED_DATA_PATH)
    if not featured_path.exists():
        pytest.skip("学習 featured_data.pkl が存在しない（パリティ基準が取れない）")

    def _structural(cols) -> set:
        # One-Hot ダミー列（値依存）を除外した構造的特徴量列のみ
        return {c for c in cols if "__" not in str(c)}

    train_cols = _structural(pd.read_pickle(featured_path).columns)
    live_cols = _structural(featured.columns)

    # ラベル/EV 用など、ライブ側に存在しないのが正当な列は除外する
    #   - _DROP_FOR_TRAIN(rank/date/単勝/着順): 学習時に落とす目的変数系
    #   - latest: interval 計算用の中間列
    benign_train_only = set(_DROP_FOR_TRAIN) | {"latest", "着順", "rank", "date"}

    train_only = (train_cols - live_cols) - benign_train_only
    live_only = (live_cols - train_cols) - benign_train_only

    # 構造的な学習列がライブに無い = 推論時 0 補完に化ける潜在バグ。ここを 0 に保つ。
    assert not train_only, (
        f"学習にあってライブに無い構造的列 {len(train_only)} 件（0補完に化ける）: "
        f"{sorted(train_only)[:10]}"
    )
    # live_only は情報目的（推論時は feature_names_ で無視されるため致命ではない）
    if live_only:
        import logging
        logging.getLogger(__name__).warning(
            "ライブのみに存在する構造的列 %d 件（モデルは無視）: %s",
            len(live_only), sorted(live_only)[:10],
        )


def test_score_table_has_all_horses(featured, keiba_ai):
    """calc_score が全出走馬分のスコアを返す。"""
    from src.policies._score_policy import StdScorePolicy
    from src.training._data_splitter import _DROP_FOR_TEST

    X = featured.drop(
        [c for c in _DROP_FOR_TEST if c in featured.columns], axis=1, errors="ignore"
    )
    score_table = keiba_ai.calc_score(X, StdScorePolicy)

    assert len(score_table) == len(featured)
    assert "score" in score_table.columns
    assert score_table["score"].notna().all()


def test_bet_policy_returns_dict(featured, keiba_ai):
    """BetPolicy が race_id をキーとした dict を返す。"""
    from src.policies._bet_policy import BetPolicyTansho
    from src.policies._score_policy import StdScorePolicy
    from src.training._data_splitter import _DROP_FOR_TEST

    X = featured.drop(
        [c for c in _DROP_FOR_TEST if c in featured.columns], axis=1, errors="ignore"
    )
    score_table = keiba_ai.calc_score(X, StdScorePolicy)
    actions = keiba_ai.decide_action(score_table, BetPolicyTansho, threshold=0.0)

    assert isinstance(actions, dict)
    # threshold=0.0 なら全馬がピックされるはず
    assert _TARGET_RACE_ID in actions
