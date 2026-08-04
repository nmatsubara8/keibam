"""JRDB キー→netkeiba キー 変換の単体テスト（実サンプル値で検証）。"""
from __future__ import annotations

from src.jrdb._keys import kaisai_key_to_kaisai_id, ketto_to_horse_id, race_key_to_race_id


def test_race_key_sample():
    # 実サンプル KYI: key=02152201 → 場02 年15(2015) 回2 日2 R01
    assert race_key_to_race_id("02152201") == "201502020201"
    # SED: key=01082101 → 場01 年08(2008) 回2 日1 R01
    assert race_key_to_race_id("01082101") == "200801020101"


def test_hex_day_conversion():
    # 日は16進1桁: 'a'=10日, 'f'=15日
    assert race_key_to_race_id("06151a05")[8:10] == "10"   # 日=10
    assert race_key_to_race_id("06151f05")[8:10] == "15"   # 日=15


def test_century_pivot():
    # yy>=86 → 1900+yy（1990年代）, <86 → 2000+yy
    assert race_key_to_race_id("06902101").startswith("1990")
    assert race_key_to_race_id("06052101").startswith("2005")


def test_ketto_to_horse_id_sample():
    # 実サンプル KYI: 血統登録=13103588（2013年生）→ horse_id=2013103588
    assert ketto_to_horse_id("13103588") == "2013103588"
    # 1990年代生
    assert ketto_to_horse_id("90500123") == "1990500123"


def test_kaisai_key_conversion():
    # 開催キー(6=場2+年2+回1+日1) → race_id の先頭10桁（R抜き）
    assert kaisai_key_to_kaisai_id("021522") == "2015020202"
    assert kaisai_key_to_kaisai_id("02152201") == "2015020202"  # R付きでも先頭6で判定
    assert kaisai_key_to_kaisai_id("06151a") == "2015060110"    # 場06 年15 回1 日a=10
    assert kaisai_key_to_kaisai_id("123") is None               # 桁不足
    # race_id[:10] と一致することを確認（同一開催のレース）
    assert race_key_to_race_id("02152201")[:10] == kaisai_key_to_kaisai_id("021522")


def test_invalid_inputs():
    assert race_key_to_race_id("") is None
    assert race_key_to_race_id(None) is None
    assert ketto_to_horse_id("123") is None       # 桁不足
    assert ketto_to_horse_id("abcd1234") is None   # 非数字
