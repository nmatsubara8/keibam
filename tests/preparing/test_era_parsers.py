"""年代別レースページ互換のリグレッションテスト（実 HTML 構造の最小再現）。

1986〜2023 の実ページ検証で見つかった差分:
- 旧年代ページは空テーブルを含み、全テーブル一括の read_html が IndexError で落ちる
  → summary 属性（全年代共通）で対象テーブルだけを解析する
- 払戻はテーブル位置 (dfs[1], dfs[2]) ではなく summary="払い戻し" で特定する
  （旧年代は馬券種が少なく表構成が異なる）
- 2019 年以前の条件クラス（500万下/1000万下/1600万下/900万下）は現行クラスへ正規化
"""

import os

import pandas as pd
import pytest

from src.constants._master import Master
from src.preparing._raw_parsers import create_raw_race_info
from src.preparing._raw_parsers import create_raw_race_results
from src.preparing._raw_parsers import create_raw_race_return

# 旧年代ページの最小再現: レース結果 + 払戻 2 表 + 空テーブル（馬場情報）
_OLD_RACE_HTML = """
<html><body>
<table summary="レース結果" class="race_table_01 nk_tb_common">
<tr><th>着 順</th><th>枠 番</th><th>馬 番</th><th>馬名</th><th>騎手</th><th>調教師</th><th>馬主</th></tr>
<tr><td>1</td><td>1</td><td>1</td><td><a href="/horse/1983100001/">ウマイチ</a></td>
<td><a href="/jockey/result/recent/00001/">騎手A</a></td>
<td><a href="/trainer/result/recent/00101/">調教師A</a></td>
<td><a href="/owner/result/recent/x00001/">馬主A</a></td></tr>
<tr><td>2</td><td>2</td><td>2</td><td><a href="/horse/1983100002/">ウマニ</a></td>
<td><a href="/jockey/result/recent/00002/">騎手B</a></td>
<td><a href="/trainer/result/recent/00102/">調教師B</a></td>
<td><a href="/owner/result/recent/x00002/">馬主B</a></td></tr>
</table>
<table class="pay_table_01" summary="払い戻し">
<tr><th>単勝</th><td>1</td><td>200</td><td>1</td></tr>
<tr><th>複勝</th><td>1<br />2</td><td>110<br />150</td><td>1<br />2</td></tr>
</table>
<table class="pay_table_01" summary="払い戻し">
<tr><th>枠連</th><td>1-2</td><td>500</td><td>2</td></tr>
</table>
<table class="result_table_02" summary="馬場情報"></table>
</body></html>
"""


@pytest.fixture
def old_race_bin(tmp_path):
    # パーサは bin パス中の最初の数値列を race_id とみなすため、
    # 数字を含む pytest の tmp_path をそのまま使えない。数字なしの中間
    # ディレクトリを挟み、相対化してパス先頭の数字を避ける。
    digitless = tmp_path / "era" / "bins"
    digitless.mkdir(parents=True)
    path = os.path.join(digitless, "198601010101.bin")
    with open(path, "wb") as f:
        f.write(_OLD_RACE_HTML.encode("utf-8"))
    return path


def test_create_raw_race_results_old_era(old_race_bin):
    """空テーブルがあっても summary 指定でレース結果を解析できる。"""
    df = create_raw_race_results(old_race_bin)
    assert len(df) == 2
    assert df.index.unique().tolist() == ["198601010101"]
    assert df["horse_id"].tolist() == ["1983100001", "1983100002"]
    assert df["jockey_id"].tolist() == ["00001", "00002"]


def test_create_raw_race_return_old_era(old_race_bin):
    """払戻はテーブル位置ではなく summary で特定する（旧年代の 3 券種構成）。"""
    df = create_raw_race_return(old_race_bin)
    assert df.index.unique().tolist() == ["198601010101"]
    # 単勝・複勝・枠連 の 3 行
    assert len(df) == 3
    assert set(df[0]) == {"単勝", "複勝", "枠連"}


def test_legacy_race_class_aliases_map_to_current_classes():
    """旧条件クラスのエイリアスはすべて現行 RACE_CLASS_LIST のクラスに正規化される。"""
    assert Master.RACE_CLASS_LEGACY_ALIASES["500万下"] == Master.RACE_CLASS_1SHO
    assert Master.RACE_CLASS_LEGACY_ALIASES["1000万下"] == Master.RACE_CLASS_2SHO
    assert Master.RACE_CLASS_LEGACY_ALIASES["1600万下"] == Master.RACE_CLASS_3SHO
    for modern in Master.RACE_CLASS_LEGACY_ALIASES.values():
        assert modern in Master.RACE_CLASS_LIST


# 1970 年代ページの最小再現: リンクの無い行（データ未整備）+ 英字混じり horse_id
_1975_RACE_HTML = """
<html><body>
<table summary="レース結果" class="race_table_01 nk_tb_common">
<tr><th>着 順</th><th>馬 番</th><th>馬名</th><th>騎手</th></tr>
<tr><td>1</td><td>3</td><td><a href="/horse/1972z00735/">エンザンオー</a></td>
<td><a href="/jockey/result/recent/00384/">町屋幸二</a></td></tr>
<tr><td>2</td><td>5</td><td>リンクナシ</td><td>同上</td></tr>
</table>
<table class="pay_table_01" summary="払い戻し">
<tr><th>単勝</th><td>3</td><td>340</td><td>1</td></tr>
</table>
</body></html>
"""


# netkeiba に実体のない空ページの再現（race_name 空・日付 1970-01-01・情報なし）。
# 例: 200808020204 / 200808020804 はこの形でクラッシュしていた。
_EMPTY_RACE_HTML = """
<html><body>
<div class="data_intro">
<p>\n\n\n\xa0/\xa0\n天候 : \xa0/\xa0\n\xa0\xa0/\xa0\n\n\n\n\n　特集\n\n</p>
<p>1970年01月01日  \xa0\xa0</p>
</div>
</body></html>
"""


def test_create_raw_race_info_empty_page_skipped(tmp_path):
    """空ページは UnboundLocalError ではなく ValueError でクリーンにスキップされる。"""
    path = os.path.join(tmp_path / "x", "200808020204.bin")
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as f:
        f.write(_EMPTY_RACE_HTML.encode("utf-8"))

    # 呼び出し側は ValueError を捕捉して「中止/欠番レース」としてスキップする。
    with pytest.raises(ValueError, match="empty race page"):
        create_raw_race_info(path)


def test_create_raw_race_results_1970s_era(tmp_path):
    """1970s: 英字混じり horse_id を保持し、リンクの無い行は None になる。"""
    path = os.path.join(tmp_path / "x", "197501010101.bin")
    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as f:
        f.write(_1975_RACE_HTML.encode("utf-8"))

    df = create_raw_race_results(path)
    assert len(df) == 2
    # 旧 ID 体系（英字混じり）が "1972" に壊れず保持される
    assert df["horse_id"].tolist()[0] == "1972z00735"
    # リンクの無い行は欠損（None は pandas により NaN へ変換される）
    assert pd.isna(df["horse_id"].tolist()[1])
    assert df["jockey_id"].tolist()[0] == "00384"
    assert pd.isna(df["jockey_id"].tolist()[1])
