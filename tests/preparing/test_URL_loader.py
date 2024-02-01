import pytest

from src.preparing.url_loader import URL_Loader


# テスト用インスタンスを作成
@pytest.fixture
def test_ins():
    return URL_Loader()
