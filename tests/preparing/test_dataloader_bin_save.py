"""DataLoader.save_temp_file の bin 保存が「上書き」であることのリグレッションテスト。

bin ファイル（馬/レース/血統ページ HTML）は processing_id ごとに独立した 1 ファイル
（HTML 1 ページ分）。保存先 to_temp_location には既存 bin が多数あるため、旧実装の
「to_temp_location が空でなければ追記(ab)」判定は常に追記を選び、既存馬の再スクレイプ時に
古い HTML の末尾へ新しい HTML を連結して 2 重 HTML の壊れた bin を作っていた。
save_temp_file は常に上書き(wb)で保存しなければならない。
"""

from src.preparing.DataLoader import DataLoader


def _make_bin_loader(tmp_path, processing_id):
    """bin を書き込む最小構成の DataLoader を返す。

    temp_save_file_name を *.bin にすることで get_filetype() が "bin" を返す。
    """
    return DataLoader(
        alias="horse_html",
        temp_save_file_name="horse_html.bin",  # get_filetype -> "bin"
        to_temp_location=str(tmp_path),
        processing_id=processing_id,
    )


def test_bin_save_overwrites_on_rescrape(tmp_path):
    """同一 horse_id を再スクレイプしても bin が連結されず最新内容で上書きされる。"""
    loader = _make_bin_loader(tmp_path, "2018110105")

    loader.target_data = b"<html>FIRST-PAGE</html>"
    loader.save_temp_file("horse_html")

    # 既存 bin が存在する状態での 2 回目の保存（再スクレイプ相当）
    loader.target_data = b"<html>SECOND-PAGE</html>"
    loader.save_temp_file("horse_html")

    saved = (tmp_path / "2018110105.bin").read_bytes()
    assert saved == b"<html>SECOND-PAGE</html>"  # 連結されていないこと


def test_bin_save_independent_files_not_appended(tmp_path):
    """別 horse_id は既存 bin の有無に関わらず単一ページのみを保持する。"""
    first = _make_bin_loader(tmp_path, "2018110105")
    first.target_data = b"<html>HORSE-A</html>"
    first.save_temp_file("horse_html")

    # ディレクトリに既存 bin がある状態で別 id を保存
    second = _make_bin_loader(tmp_path, "2019104? ".strip())
    second.processing_id = "2019104001"
    second.target_data = b"<html>HORSE-B</html>"
    second.save_temp_file("horse_html")

    assert (tmp_path / "2018110105.bin").read_bytes() == b"<html>HORSE-A</html>"
    assert (tmp_path / "2019104001.bin").read_bytes() == b"<html>HORSE-B</html>"
