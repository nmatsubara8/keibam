"""カテゴリ分類ロジック（全国/地方 × 芝/ダート/障害）のテスト。"""

from src.constants._master import Master
from src.constants._model_category import ALL_CATEGORIES
from src.constants._model_category import CATEGORY_LABELS
from src.constants._model_category import COMBINED
from src.constants._model_category import ORG_CENTRAL
from src.constants._model_category import ORG_LOCAL
from src.constants._model_category import categorize
from src.constants._model_category import live_netkeiba_base
from src.constants._model_category import live_netkeiba_base_for_race_id
from src.constants._model_category import organizer_of_race_id
from src.constants._model_category import race_type_to_slug


def test_all_categories_has_six_entries():
    assert len(ALL_CATEGORIES) == 6
    assert len(set(ALL_CATEGORIES)) == 6


def test_live_netkeiba_base_by_organizer():
    assert live_netkeiba_base(ORG_CENTRAL) == "https://race.netkeiba.com"
    assert live_netkeiba_base(ORG_LOCAL) == "https://nar.netkeiba.com"
    # 未知値は中央にフォールバック（既存挙動を壊さない）
    assert live_netkeiba_base("bogus") == "https://race.netkeiba.com"


def test_live_netkeiba_base_for_race_id():
    # 中央（場コード 01〜10）は race.netkeiba.com、地方（30番台〜）は nar.netkeiba.com
    assert live_netkeiba_base_for_race_id("202405010101") == "https://race.netkeiba.com"
    assert live_netkeiba_base_for_race_id("202630072201") == "https://nar.netkeiba.com"  # 門別30
    assert live_netkeiba_base_for_race_id("202444010101") == "https://nar.netkeiba.com"  # 大井44


def test_labels_are_japanese():
    assert CATEGORY_LABELS["central_turf"] == "全国・芝"
    assert CATEGORY_LABELS["local_dirt"] == "地方・ダート"
    assert CATEGORY_LABELS[COMBINED] == "統合（全レース）"


def test_organizer_central_codes():
    # 中央（JRA）: 場コード 01〜10（東京=05, 中山=06 ...）
    assert organizer_of_race_id("202405010101") == ORG_CENTRAL
    assert organizer_of_race_id("202401010101") == ORG_CENTRAL
    assert organizer_of_race_id("202410010101") == ORG_CENTRAL


def test_organizer_local_codes():
    # 地方（NAR）: 大井=44, 川崎=45, 門別=30 ...
    assert organizer_of_race_id("202444010101") == ORG_LOCAL
    assert organizer_of_race_id("202430010101") == ORG_LOCAL
    assert organizer_of_race_id("202458010101") == ORG_LOCAL


def test_organizer_accepts_int_and_float_str():
    assert organizer_of_race_id(202405010101) == ORG_CENTRAL
    assert organizer_of_race_id("202405010101.0") == ORG_CENTRAL


def test_organizer_short_id_is_local():
    assert organizer_of_race_id("123") == ORG_LOCAL


def test_race_type_to_slug():
    assert race_type_to_slug(Master.RACE_TYPE_TURF) == "turf"
    assert race_type_to_slug(Master.RACE_TYPE_DIRT) == "dirt"
    assert race_type_to_slug(Master.RACE_TYPE_HURDLE) == "hurdle"


def test_race_type_to_slug_unknown_and_nan():
    assert race_type_to_slug(None) is None
    assert race_type_to_slug(float("nan")) is None
    assert race_type_to_slug("unknown") is None


def test_categorize_combines_axes():
    assert categorize("202405010101", Master.RACE_TYPE_TURF) == "central_turf"
    assert categorize("202444010101", Master.RACE_TYPE_DIRT) == "local_dirt"
    assert categorize("202406010101", Master.RACE_TYPE_HURDLE) == "central_hurdle"


def test_categorize_unknown_race_type_is_none():
    assert categorize("202405010101", None) is None
    assert categorize("202405010101", "芝ダ混合") is None


def test_every_categorize_result_is_in_all_categories():
    for org_id in ("202405010101", "202444010101"):
        for rt in (Master.RACE_TYPE_TURF, Master.RACE_TYPE_DIRT, Master.RACE_TYPE_HURDLE):
            assert categorize(org_id, rt) in ALL_CATEGORIES
