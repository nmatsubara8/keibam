"""増分取込フィルタ（modules._filter_target_bins）のテスト。

ingest が新規 race_id / horse_id の bin だけを再パースする（全 HTML コーパスの
再パースを避ける）ことを担保する純粋関数の回帰テスト。
"""

from src.preparing._scrape_pages import _filter_target_bins


def test_none_returns_all():
    files = ["202406010101.bin", "202406010102.bin", "9999.bin"]
    assert _filter_target_bins(files, None) == files


def test_filters_to_given_ids():
    files = ["202406010101.bin", "202406010102.bin", "202406010103.bin"]
    out = _filter_target_bins(files, [202406010101, 202406010103])
    assert out == ["202406010101.bin", "202406010103.bin"]


def test_id_type_normalized_to_str():
    files = ["123.bin", "456.bin"]
    # int でも str でも一致する
    assert _filter_target_bins(files, [123]) == ["123.bin"]
    assert _filter_target_bins(files, ["456"]) == ["456.bin"]


def test_empty_ids_filters_everything():
    files = ["1.bin", "2.bin"]
    assert _filter_target_bins(files, []) == []


def test_handles_path_prefix():
    files = ["data/html/race/202406010101.bin", "data/html/race/202406010102.bin"]
    out = _filter_target_bins(files, [202406010102])
    assert out == ["data/html/race/202406010102.bin"]


def test_non_matching_ids_excluded():
    files = ["1.bin", "2.bin"]
    assert _filter_target_bins(files, [999]) == []
