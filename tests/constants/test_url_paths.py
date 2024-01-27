import pytest

from src.constants import _url_paths


# テスト用インスタンスを作成
@pytest.fixture
def test_ins():
    return _url_paths.UrlPaths()


# デフォルト値を確認
def test_default_value(test_ins):
    assert test_ins.DB_DOMAIN is not None and isinstance(test_ins.DB_DOMAIN, tuple)
    assert test_ins.RACE_URL is not None and isinstance(test_ins.RACE_URL, tuple)
    assert test_ins.HORSE_URL is not None and isinstance(test_ins.HORSE_URL, tuple)
    assert test_ins.PED_URL is not None and isinstance(test_ins.PED_URL, tuple)
    assert test_ins.CALENDAR_URL is not None and isinstance(test_ins.CALENDAR_URL, tuple)
    assert test_ins.RACE_LIST_URL is not None and isinstance(test_ins.RACE_LIST_URL, tuple)
    assert test_ins.SHUTUBA_TABLE is not None and isinstance(test_ins.SHUTUBA_TABLE, tuple)


# 実際の設定値を確認
def test_actual_value(test_ins):
    print(test_ins.DB_DOMAIN)
    print(test_ins.RACE_URL)
    print(test_ins.HORSE_URL)
    print(test_ins.PED_URL)
    print(test_ins.CALENDAR_URL)
    print(test_ins.RACE_LIST_URL)
    print(test_ins.SHUTUBA_TABLE)
