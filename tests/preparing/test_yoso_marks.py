"""予想印パーサ（_yoso_marks）のテスト。

実 API レスポンス（api_get_pro_yoso_list_v2）の構造を模した合成 JSON で、
無料予想家のみ抽出・印コード変換・(race_id,馬番) 集約を検証する。
"""

import json

import pandas as pd

from src.preparing._yoso_marks import (
    aggregate_consensus,
    parse_pro_yoso_json,
)

# 実レスポンス 202605030611 の構造を縮約（本紙=no1_free, 須田=no1_premium, CP予想=no1_free）
_SAMPLE = {
    "status": "OK",
    "reason": "",
    "data": {
        "ary_item": [
            {
                "goods_kbn": "no1_free",
                "yosoka_id": "266987",
                "yosoka_name": "本<br>紙<br>",  # 本紙
                "mark": {"13": "1", "11": "2", "9": "3", "2": "5", "12": "4"},
            },
            {
                "goods_kbn": "no1_premium",  # プレミアム指定 → 除外対象
                "yosoka_id": "266991",
                "yosoka_name": "須<br>田<br>",  # 須田
                "mark": {"11": "1", "13": "5", "2": "3"},
            },
            {
                "goods_kbn": "no1_free",
                "yosoka_id": "266992",
                "yosoka_name": "C<br>P<br>予<br>想<br>",  # CP予想
                "mark": {"9": "1", "11": "2", "4": "3", "1": "4"},
            },
            {
                "goods_kbn": "umai_sell",  # 有料・mark空 → 除外
                "yosoka_id": "999999",
                "yosoka_name": "有料家",
                "mark": {},
            },
        ]
    },
}


class TestParse:
    def test_default_includes_premium_excludes_empty_paid(self):
        # 既定(free_only=False)は無料＋プレミアム指定の両方を取得
        df = parse_pro_yoso_json(_SAMPLE, "202605030611")
        yids = set(df["predictor_yid"])
        assert yids == {"266987", "266991", "266992"}  # 本紙・須田(premium)・CP予想
        assert "999999" not in yids  # umai_sell は mark 空 → 自然に行なし

    def test_goods_kbn_preserved(self):
        df = parse_pro_yoso_json(_SAMPLE, "R")
        kbn = dict(zip(df["predictor_yid"], df["goods_kbn"], strict=False))
        assert kbn["266987"] == "no1_free"
        assert kbn["266991"] == "no1_premium"

    def test_free_only_excludes_premium(self):
        df = parse_pro_yoso_json(_SAMPLE, "R", free_only=True)
        yids = set(df["predictor_yid"])
        assert yids == {"266987", "266992"}  # プレミアム指定 266991 を除外

    def test_name_br_stripped(self):
        df = parse_pro_yoso_json(_SAMPLE, "R")
        names = dict(zip(df["predictor_yid"], df["predictor_name"], strict=True))
        assert names["266987"] == "本紙"
        assert names["266992"] == "CP予想"

    def test_mark_code_mapping(self):
        df = parse_pro_yoso_json(_SAMPLE, "R")
        honsi = df[df["predictor_yid"] == "266987"].set_index("馬番")
        assert honsi.loc[13, "mark"] == "◎"
        assert honsi.loc[13, "mark_score"] == 5
        assert honsi.loc[11, "mark"] == "○"
        assert honsi.loc[2, "mark"] == "☆"
        assert honsi.loc[12, "mark"] == "△"

    def test_umaban_is_int(self):
        df = parse_pro_yoso_json(_SAMPLE, "R")
        assert df["馬番"].map(type).eq(int).all()

    def test_accepts_json_string(self):
        df = parse_pro_yoso_json(json.dumps(_SAMPLE), "R")
        assert len(df) > 0

    def test_accepts_jsonp_string(self):
        wrapped = "cb(" + json.dumps(_SAMPLE) + ");"
        df = parse_pro_yoso_json(wrapped, "R")
        assert set(df["predictor_yid"]) == {"266987", "266991", "266992"}

    def test_status_ng_returns_empty(self):
        df = parse_pro_yoso_json({"status": "NG", "reason": "x"}, "R")
        assert df.empty
        assert list(df.columns) == [
            "race_id",
            "馬番",
            "predictor_yid",
            "predictor_name",
            "goods_kbn",
            "mark",
            "mark_score",
        ]

    def test_robust_to_malformed_payload(self):
        # 壊れた入力・欠損フィールドでも例外を投げず空/スキップで返す
        assert parse_pro_yoso_json("not json at all", "R").empty
        assert parse_pro_yoso_json({"data": None}, "R").empty
        # 一部要素が壊れていても健全な要素は拾う
        mixed = {
            "status": "OK",
            "data": {
                "ary_item": [
                    "garbage",  # dict でない
                    {"yosoka_id": "1", "mark": None},  # mark 欠損
                    {"goods_kbn": "no1_free", "yosoka_id": "2", "yosoka_name": "甲",
                     "mark": {"5": "1"}},
                ]
            },
        }
        df = parse_pro_yoso_json(mixed, "R")
        assert set(df["predictor_yid"]) == {"2"}


class TestAggregate:
    def test_consensus_counts_and_scores(self):
        # 無料のみで集約を検証（プレミアムを混ぜず期待値を明確化）
        df = parse_pro_yoso_json(_SAMPLE, "202605030611", free_only=True)
        agg = aggregate_consensus(df).set_index("馬番")
        # 馬番11: 本紙○(4) + CP○(4) → 2予想家・◎0・合計8・平均4
        assert agg.loc[11, "yoso_n_marks"] == 2
        assert agg.loc[11, "yoso_n_honmei"] == 0
        assert agg.loc[11, "yoso_score_sum"] == 8
        assert agg.loc[11, "yoso_score_mean"] == 4
        # 馬番13: 本紙◎(5) のみ → ◎1
        assert agg.loc[13, "yoso_n_honmei"] == 1
        assert agg.loc[13, "yoso_score_sum"] == 5
        # 馬番9: 本紙▲(3) + CP◎(5) → ◎1・合計8
        assert agg.loc[9, "yoso_n_honmei"] == 1
        assert agg.loc[9, "yoso_score_sum"] == 8

    def test_free_count_separated_from_premium(self):
        # 既定(premium含む)で集約。馬番2: 本紙☆(free) + 須田▲(premium) → 計2・無料1
        df = parse_pro_yoso_json(_SAMPLE, "R")
        agg = aggregate_consensus(df).set_index("馬番")
        assert agg.loc[2, "yoso_n_marks"] == 2
        assert agg.loc[2, "yoso_n_marks_free"] == 1

    def test_empty_input(self):
        agg = aggregate_consensus(pd.DataFrame())
        assert agg.empty


class TestUmabanMapConversion:
    """予想印 API の mark キーは uma_id。出馬表 tr_<uma_id> で馬番に変換する。"""

    _SHUTUBA = (
        '<table class="Shutuba_Table">'
        '<tr class="HorseList" id="tr_9"><td class="Umaban Num1">1</td></tr>'
        '<tr class="HorseList" id="tr_1"><td class="Umaban Num2">2</td></tr>'
        '<tr class="HorseList" id="tr_16"><td class="Umaban Num5">5</td></tr>'
        "</table>"
    )

    def test_parse_umaban_map(self):
        from src.preparing._yoso_marks import parse_umaban_map

        assert parse_umaban_map(self._SHUTUBA) == {9: 1, 1: 2, 16: 5}
        assert parse_umaban_map("") == {}

    def test_parse_with_umaban_map_converts_and_drops_unknown(self):
        from src.preparing._yoso_marks import parse_pro_yoso_json, parse_umaban_map

        m = parse_umaban_map(self._SHUTUBA)
        payload = {
            "status": "OK",
            "data": {"ary_item": [
                {"yosoka_id": "266994", "yosoka_name": "本紙", "goods_kbn": "no1_free",
                 "mark": {"1": "1", "16": "2", "99": "1"}},  # uma_id 99 は出馬表に無い
            ]},
        }
        df = parse_pro_yoso_json(payload, "R1", umaban_map=m)
        got = {int(r["馬番"]): r["mark"] for _, r in df.iterrows()}
        assert got == {2: "◎", 5: "○"}  # uma_id 1→馬番2(◎), 16→馬番5(○), 99 は除外

    def test_none_map_is_legacy_passthrough(self):
        from src.preparing._yoso_marks import parse_pro_yoso_json

        payload = {"status": "OK", "data": {"ary_item": [
            {"yosoka_id": "x", "yosoka_name": "n", "goods_kbn": "no1_free",
             "mark": {"7": "1"}}]}}
        df = parse_pro_yoso_json(payload, "R1")  # umaban_map 未指定
        assert int(df.iloc[0]["馬番"]) == 7  # キーをそのまま（後方互換）
