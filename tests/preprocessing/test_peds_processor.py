import pandas as pd
import pytest

from src.preprocessing._peds_processor import PedsProcessor


def _write_peds_pkl(path, horse_ids, peds_0, peds_1=None):
    data = {"horse_id": horse_ids, "peds_0": peds_0}
    if peds_1 is not None:
        data["peds_1"] = peds_1
    pd.DataFrame(data).to_pickle(str(path))


class TestPedsProcessorTrainingMode:
    def test_stores_encoders_for_each_column(self, tmp_path):
        _write_peds_pkl(tmp_path / "peds.pkl", [1001, 1002], ["Sire_A", "Sire_B"], ["Dam_X", "Dam_Y"])
        proc = PedsProcessor(str(tmp_path / "peds.pkl"))

        assert "peds_0" in proc.encoders_
        assert "peds_1" in proc.encoders_

    def test_unknown_sentinel_in_classes(self, tmp_path):
        _write_peds_pkl(tmp_path / "peds.pkl", [1001, 1002], ["Sire_A", "Sire_B"])
        proc = PedsProcessor(str(tmp_path / "peds.pkl"))

        assert "__unknown__" in proc.encoders_["peds_0"].classes_

    def test_output_is_integer_category(self, tmp_path):
        _write_peds_pkl(tmp_path / "peds.pkl", [1001, 1002], ["Sire_A", "Sire_B"])
        proc = PedsProcessor(str(tmp_path / "peds.pkl"))
        result = proc.preprocessed_data

        assert str(result["peds_0"].dtype) == "category"

    def test_horse_id_is_index(self, tmp_path):
        _write_peds_pkl(tmp_path / "peds.pkl", [1001, 1002], ["Sire_A", "Sire_B"])
        proc = PedsProcessor(str(tmp_path / "peds.pkl"))

        assert proc.preprocessed_data.index.name == "horse_id"
        assert 1001 in proc.preprocessed_data.index


class TestPedsProcessorInferenceMode:
    def test_same_value_same_encoding(self, tmp_path):
        """Known values must encode identically between training and inference."""
        _write_peds_pkl(tmp_path / "train.pkl", [1001, 1002, 1003], ["Sire_A", "Sire_B", "Sire_A"])
        train_proc = PedsProcessor(str(tmp_path / "train.pkl"))

        _write_peds_pkl(tmp_path / "infer.pkl", [1001, 1002], ["Sire_A", "Sire_B"])
        infer_proc = PedsProcessor(str(tmp_path / "infer.pkl"), encoders=train_proc.encoders_)

        train_out = train_proc.preprocessed_data
        infer_out = infer_proc.preprocessed_data

        assert int(train_out.loc[1001, "peds_0"]) == int(infer_out.loc[1001, "peds_0"])
        assert int(train_out.loc[1002, "peds_0"]) == int(infer_out.loc[1002, "peds_0"])

    def test_unknown_category_does_not_raise(self, tmp_path):
        """An unseen category at inference time must map to __unknown__ without raising."""
        _write_peds_pkl(tmp_path / "train.pkl", [1001, 1002], ["Sire_A", "Sire_B"])
        train_proc = PedsProcessor(str(tmp_path / "train.pkl"))

        _write_peds_pkl(tmp_path / "infer.pkl", [9999], ["Sire_UNSEEN"])
        infer_proc = PedsProcessor(str(tmp_path / "infer.pkl"), encoders=train_proc.encoders_)

        result = infer_proc.preprocessed_data
        assert not result["peds_0"].isna().any()

    def test_unknown_category_gets_unknown_encoding(self, tmp_path):
        """Unseen categories should receive the same integer as the __unknown__ sentinel."""
        _write_peds_pkl(tmp_path / "train.pkl", [1001], ["Sire_A"])
        train_proc = PedsProcessor(str(tmp_path / "train.pkl"))
        unknown_code = int(train_proc.encoders_["peds_0"].transform(["__unknown__"])[0])

        _write_peds_pkl(tmp_path / "infer.pkl", [9999], ["Sire_UNSEEN"])
        infer_proc = PedsProcessor(str(tmp_path / "infer.pkl"), encoders=train_proc.encoders_)

        assert int(infer_proc.preprocessed_data.loc[9999, "peds_0"]) == unknown_code

    def test_inference_mode_mirrors_encoders(self, tmp_path):
        """Inference-mode PedsProcessor.encoders_ should reference the provided encoders."""
        _write_peds_pkl(tmp_path / "train.pkl", [1001], ["Sire_A"])
        train_proc = PedsProcessor(str(tmp_path / "train.pkl"))

        _write_peds_pkl(tmp_path / "infer.pkl", [1002], ["Sire_A"])
        infer_proc = PedsProcessor(str(tmp_path / "infer.pkl"), encoders=train_proc.encoders_)

        assert infer_proc.encoders_["peds_0"] is train_proc.encoders_["peds_0"]
