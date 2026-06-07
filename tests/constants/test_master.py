"""Master 名前付き定数と既存の list/dict 値の整合性テスト。

`_master.py` の RACE_TYPE_DICT / GROUND_STATE_LIST / AROUND_LIST / RACE_CLASS_LIST
は順序が変わるとスクレイパ側（src/preparing/）の位置参照が壊れていたため、
新規追加した名前付き定数が list/dict と同じ値を返すことを固定し、リファクタ後の
回帰ガードとする。
"""

from __future__ import annotations

from src.constants._master import Master


class TestMasterNamedConstants:
    def test_race_type_dict_values_match_named(self):
        assert Master.RACE_TYPE_DICT["芝"] == Master.RACE_TYPE_TURF == "芝"
        assert Master.RACE_TYPE_DICT["ダ"] == Master.RACE_TYPE_DIRT == "ダート"
        assert Master.RACE_TYPE_DICT["障"] == Master.RACE_TYPE_HURDLE == "障害"

    def test_ground_state_list_positional_match_named(self):
        assert Master.GROUND_STATE_LIST[0] == Master.GROUND_STATE_GOOD == "良"
        assert Master.GROUND_STATE_LIST[1] == Master.GROUND_STATE_YAYA_OMO == "稍重"
        assert Master.GROUND_STATE_LIST[2] == Master.GROUND_STATE_OMO == "重"
        assert Master.GROUND_STATE_LIST[3] == Master.GROUND_STATE_BAD == "不良"

    def test_around_list_positional_match_named(self):
        assert Master.AROUND_LIST[0] == Master.AROUND_RIGHT == "右"
        assert Master.AROUND_LIST[1] == Master.AROUND_LEFT == "左"
        assert Master.AROUND_LIST[2] == Master.AROUND_STRAIGHT == "直線"
        # AROUND_LIST[3] は意図的に範囲外（既存スクレイパが参照しており、挙動保持の対象）
        assert len(Master.AROUND_LIST) == 3

    def test_race_class_list_positional_match_named(self):
        # 既存スクレイパ（table_creator / modules）が使う位置参照の値を固定する。
        # 一部の参照は意味的に怪しい（"オープン" テキスト→LISTED 等）が、本テストは
        # 値の連続性のみを保証する（fix は別 PR）。
        expected = [
            Master.RACE_CLASS_SHINBA,
            Master.RACE_CLASS_MISHORI,
            Master.RACE_CLASS_1SHO,
            Master.RACE_CLASS_2SHO,
            Master.RACE_CLASS_3SHO,
            Master.RACE_CLASS_LISTED,
            Master.RACE_CLASS_OPEN,
            Master.RACE_CLASS_OPEN_SPECIAL,
            Master.RACE_CLASS_G3,
            Master.RACE_CLASS_G2,
            Master.RACE_CLASS_G1,
        ]
        assert list(Master.RACE_CLASS_LIST) == expected

    def test_race_class_list_length_unchanged(self):
        assert len(Master.RACE_CLASS_LIST) == 11
