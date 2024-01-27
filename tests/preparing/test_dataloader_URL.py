import pytest

from src.preparing import DataLoader_URL


# テスト用インスタンスを作成
@pytest.fixture
def test_ins():
    return DataLoader_URL.DataLoader_URL()


# デフォルト値を確認
def test_default_value(test_ins):
    assert test_ins.DB_DOMAIN is not None and isinstance(test_ins.alias, str)
    assert test_ins.from_location is not None and isinstance(test_ins.from_location, str)
    assert test_ins.save_location is not None and isinstance(test_ins.save_location, str)
    assert test_ins.save_file_name is not None and isinstance(test_ins.save_file_name, str)
    assert test_ins.rerun is not None and isinstance(test_ins.rerun, bool)


def test_func(test_ins):
    attr, value = test_ins.alias_existence_check()
    print(attr, value)
