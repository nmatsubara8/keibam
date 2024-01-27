import os

import pytest

from src.preparing import DataLoader


@pytest.fixture
def sample_ins():
    return DataLoader.CustomDataLoader()


def test_default_value(sample_ins):
    assert isinstance(sample_ins.from_location, str)
    assert isinstance(sample_ins.load_file_name, str)
    assert isinstance(sample_ins.to_location, str)

    assert isinstance(sample_ins.load_file_name, str)
    assert isinstance(sample_ins.save_file_name, str)


def test_load_from_local(sample_ins):
    sample_ins.load_data_from_local()

    # assert os.path.exists(sample_ins.from_location)

    assert os.path.exists(sample_ins.save_file_path)
