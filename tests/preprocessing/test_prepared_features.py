"""PreparedFeatures DTO のテスト。"""

import pandas as pd
import pytest

from src.preprocessing._prepared_features import PreparedFeatures


def _make_df(n=10):
    return pd.DataFrame({"a": range(n), "b": range(n, n * 2)})


class TestPreparedFeatures:
    def test_construction(self):
        gbdt = _make_df()
        nn = _make_df(5)
        pf = PreparedFeatures(gbdt=gbdt, nn=nn)
        assert len(pf.gbdt) == 10
        assert len(pf.nn) == 5

    def test_frozen_raises_on_reassign(self):
        pf = PreparedFeatures(gbdt=_make_df(), nn=_make_df())
        with pytest.raises((TypeError, AttributeError)):
            pf.gbdt = _make_df()  # type: ignore[misc]

    def test_fields_are_dataframes(self):
        pf = PreparedFeatures(gbdt=_make_df(), nn=_make_df())
        assert isinstance(pf.gbdt, pd.DataFrame)
        assert isinstance(pf.nn, pd.DataFrame)
