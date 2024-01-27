from src.preparing._scrape_kaisai_date import scrape_kaisai_date

# @pytest.fixture()
# def test_date():
#  return


def test_init():
    assert scrape_kaisai_date("2020-01-01", "2021-01-01") == 0


def arg_type_check():
    assert from_
