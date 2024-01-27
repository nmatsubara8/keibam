import os

import pytest

from src.preparing import DataLoader


@pytest.fixture
def sample_ins():
    return DataLoader.CustomDataLoader()


def test_default_value(sample_ins):
    assert isinstance(sample_ins.data_location, str)
    assert isinstance(sample_ins.file_name, str)


def test_load_from_local_folder(sample_ins):
    sample_ins.load_data_from_local_folder()
    assert os.path.exists(sample_ins.folder_path)
    assert os.path.exists(sample_ins.file_path)
    assert os.path.exists(sample_ins.data_location)
