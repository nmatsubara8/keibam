import pytest

from src.preparing import url_loader


# テスト用インスタンスを作成
@pytest.fixture
def test_ins():
    return url_loader.URL_Loader()


# デフォルト値を確認
def test_default_value(test_ins):
    assert test_ins.alias is not None and isinstance(test_ins.alias, str)
    assert test_ins.from_location is not None and isinstance(test_ins.from_location, str)
    assert test_ins.save_location is not None and isinstance(test_ins.save_location, str)
    assert test_ins.save_file_name is not None and isinstance(test_ins.save_file_name, str)
    assert test_ins.rerun is not None and isinstance(test_ins.rerun, bool)


@pytest.fixture(
    params=[
        ("kaisai_date_list"),
        ("race_list"),
        ("horse_list"),
        ("ped_list"),
        ("top_page"),
        ("horse_result_list"),
        ("race_result_list"),
        ("shutuba_table"),
    ]
)
def test_ins_get_data(request):
    alias = request.param
    return url_loader.URL_Loader(alias=alias)


def test_func(test_ins_get_data):
    test_ins_get_data.alias_existence_check()
