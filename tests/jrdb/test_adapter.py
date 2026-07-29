"""JRDB→netkeiba raw スキーマ・アダプタの単体テスト（合成 SED で写像を検証）。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._adapter import (
    build_raw_horse_results,
    build_raw_race_info,
    build_raw_results,
)


def _sed_rows():
    """store.read('SED') 相当（全列文字列でも動くことを含めて検証）。"""
    return pd.DataFrame([
        {"race_id": "202102010101", "umaban": "1", "ketto": "18103588",
         "chakujun": "1", "ijo_kubun": "0", "bamei": "テスト馬 ",
         "futan_juryo": "550", "kishu_name": "テスト騎手", "chokyoshi_name": "テスト師",
         "time": "1234", "corner1": "3", "corner2": "3", "corner3": "2", "corner4": "1",
         "ato3f_time": "345", "kakutei_tansho": "  3.5", "kakutei_ninki": "2",
         "bataijuu": "480", "bataijuu_zougen": "+ 4", "biko": "  ", "honshokin": "5200",
         "kishu_code": "01001", "chokyo_code": "05001"},
        {"race_id": "202102010101", "umaban": "2", "ketto": "18103599",
         "chakujun": "0", "ijo_kubun": "2", "bamei": "取消馬",   # 除外
         "futan_juryo": "560", "kishu_name": "騎手B", "chokyoshi_name": "師B",
         "time": "   ", "corner1": "0", "corner2": "0", "corner3": "0", "corner4": "0",
         "ato3f_time": "   ", "kakutei_tansho": "", "kakutei_ninki": "",
         "bataijuu": "500", "bataijuu_zougen": "", "biko": "", "honshokin": "0",
         "kishu_code": "01002", "chokyo_code": "05001"},
    ])


def test_structural_mapping():
    out = build_raw_results(_sed_rows())
    assert list(out.index) == ["202102010101", "202102010101"]
    r0 = out.iloc[0]
    assert r0["馬番"] == 1
    assert r0["着順"] == "1"
    assert r0["馬名"] == "テスト馬"           # strip
    assert r0["斤量"] == 55.0                # 0.1kg→kg
    assert r0["騎手"] == "テスト騎手"
    assert r0["タイム"] == "1:23.4"          # 1byte分+3byte秒0.1
    assert r0["通過"] == "3-3-2-1"
    assert r0["上り"] == 34.5
    assert r0["単勝"] == 3.5
    assert r0["人気"] == 2
    assert r0["馬体重"] == "480(+4)"
    assert r0["賞金(万円)"] == 5200
    # SED に無い列は欠損で確保
    assert "枠番" in out.columns and pd.isna(r0["枠番"])


def test_kyi_join_populates_waku_and_seirei():
    # KYI（出馬表）を渡すと 枠番/性齢 が (race_id,馬番) で補われる。
    # 性齢: 数え歳 = レース年(2021) − 生年(ketto 18…→2018) = 3。sex 1=牡 / 2=牝。
    kyi = pd.DataFrame([
        {"race_id": "202102010101", "umaban": "1", "wakuban": "1", "sex_code": "1"},
        {"race_id": "202102010101", "umaban": "2", "wakuban": "2", "sex_code": "2"},
    ])
    out = build_raw_results(_sed_rows(), kyi=kyi)
    assert out.iloc[0]["枠番"] == 1 and out.iloc[0]["性齢"] == "牡3"
    assert out.iloc[1]["枠番"] == 2 and out.iloc[1]["性齢"] == "牝3"


def test_kyi_absent_leaves_waku_seirei_nan():
    out = build_raw_results(_sed_rows())            # kyi 無し＝従来挙動
    assert pd.isna(out.iloc[0]["枠番"]) and pd.isna(out.iloc[0]["性齢"])


def test_kyi_partial_match_only_fills_present_keys():
    # KYI に無い (race_id,馬番) は欠損のまま（部分一致でも壊れない）。
    kyi = pd.DataFrame([{"race_id": "202102010101", "umaban": "1",
                         "wakuban": "3", "sex_code": "1"}])
    out = build_raw_results(_sed_rows(), kyi=kyi)
    assert out.iloc[0]["枠番"] == 3
    assert pd.isna(out.iloc[1]["枠番"]) and pd.isna(out.iloc[1]["性齢"])


def test_abnormal_and_blanks():
    out = build_raw_results(_sed_rows())
    r1 = out.iloc[1]
    assert pd.isna(r1["着順"])               # 異常区分2(除外)=非完走→None（netkeiba NaN）
    assert pd.isna(r1["タイム"])             # 空タイム
    # 非完走の time='0000' も None（netkeiba NaN に一致）
    from src.jrdb._adapter import _time_str
    assert _time_str("0000") is None and _time_str("1234") == "1:23.4"
    assert pd.isna(r1["通過"])               # 全コーナー0
    assert r1["馬体重"] == "500"             # 増減空→体重のみ
    assert pd.isna(r1["単勝"])               # 空オッズ


def test_ids_attached():
    jockey = pd.DataFrame({"kishu_code": ["01001"], "jockey_id": ["j641"]})
    trainer = pd.DataFrame({"chokyo_code": ["05001"], "trainer_id": ["t362"]})
    out = build_raw_results(_sed_rows(), jockey_xwalk=jockey, trainer_xwalk=trainer)
    r0 = out.iloc[0]
    assert r0["horse_id"] == "2018103588"    # ketto→ketto_to_horse_id（canonical id）
    assert out.iloc[1]["horse_id"] == "2018103599"
    assert r0["jockey_id"] == "j641"
    assert r0["trainer_id"] == "t362"
    # crosswalk に無い騎手/調教師コードは欠損
    assert pd.isna(out.iloc[1]["jockey_id"])  # 01002 は未対応


def test_raw_horse_results_mapping():
    sed = pd.DataFrame([
        {"race_id": "202205020611", "umaban": "9", "ketto": "18103588", "ymd": "20220614",
         "tenko_code": "2", "race_name": "テストS", "toushuu": "13", "kyori": "1400",
         "shiba_dirt": "1", "baba_state": "10", "chakujun": "11", "ijo_kubun": "0",
         "kishu_name": "テスト騎手", "futan_juryo": "560", "kakutei_tansho": " 51.1",
         "kakutei_ninki": "12", "time": "1218", "chaku1_time_sa": "015",
         "corner1": "0", "corner2": "0", "corner3": "7", "corner4": "8", "ato3f_time": "345",
         "bataijuu": "498", "bataijuu_zougen": "-10", "chaku1_bamei": "マイネルチケット",
         "honshokin": "0"},
    ])
    out = build_raw_horse_results(sed)
    r = out.iloc[0]
    assert r["horse_id"] == "2018103588"   # ketto 18103588 → ketto_to_horse_id
    assert r["日付"] == "2022/06/14"
    assert r["開催"] == "東京"          # 場05
    assert r["天気"] == "曇"
    assert r["R"] == 11
    assert r["距離"] == "芝1400"        # 芝(短縮)+距離
    assert r["馬場"] == "良"
    assert r["タイム"] == "1:21.8"
    assert r["着差"] == 1.5            # 0.1秒→秒（数値）
    assert r["通過"] == "7-8"
    assert r["上り"] == 34.5
    assert r["馬体重"] == "498(-10)"
    assert r["勝ち馬(2着馬)"] == "マイネルチケット"
    assert pd.isna(r["賞金"])          # 0→NaN
    assert pd.isna(r["枠番"])          # SED に無い


def test_empty_input():
    assert build_raw_results(pd.DataFrame()).empty
    assert build_raw_race_info(pd.DataFrame()).empty
    assert build_raw_horse_results(pd.DataFrame()).empty


def test_raw_race_info_mapping():
    sed = pd.DataFrame([
        # 同一レースの2頭（レース条件は同じ→畳んで1行に）
        {"race_id": "201805020201", "umaban": "1", "ymd": "20180712", "hassou_time": "1005",
         "kyori": "1800", "shiba_dirt": "1", "migi_hidari": "2", "tenko_code": "1",
         "baba_state": "10", "shubetsu": "13", "joken": "A3"},
        {"race_id": "201805020201", "umaban": "2", "ymd": "20180712", "hassou_time": "1005",
         "kyori": "1800", "shiba_dirt": "1", "migi_hidari": "2", "tenko_code": "1",
         "baba_state": "10"},
        {"race_id": "201805020202", "umaban": "1", "ymd": "20180712", "hassou_time": "1035",
         "kyori": "1200", "shiba_dirt": "2", "migi_hidari": "1", "tenko_code": "2",
         "baba_state": "30"},
    ])
    ri = build_raw_race_info(sed)
    assert len(ri) == 2                       # レース単位に畳まれる
    r = ri.loc["201805020201"]
    assert r["place_id"] == "5" and r["place"] == "東京"   # 場05
    assert r["times"] == "2" and r["days"] == "2"          # 回02 日02
    assert r["date"] == "2018年07月12日"
    assert r["time"] == "10:05"
    assert r["course_len"] == 1800
    assert r["race_type"] == "芝"
    assert r["around"] == "左"                # 2→左
    assert r["weather"] == "晴"               # 1→晴
    assert r["ground_state1"] == "良" and r["ground_state2"] == "良"  # 10→良
    assert r["age"] == "3" and r["race_class"] == "未勝利"  # 種別13→3歳以上, 条件A3→未勝利
    r2 = ri.loc["201805020202"]
    assert r2["race_type"] == "ダート" and r2["weather"] == "曇" and r2["ground_state1"] == "重"
    # 馬場状態は十の位で going（21→稍重, 40→不良）
    from src.jrdb._adapter import build_raw_race_info as _b
    g = _b(pd.DataFrame([
        {"race_id": "201805020203", "umaban": "1", "baba_state": "21"},
        {"race_id": "201805020204", "umaban": "1", "baba_state": "40"},
    ]))
    assert g.loc["201805020203", "ground_state1"] == "稍重"
    assert g.loc["201805020204", "ground_state1"] == "不良"
