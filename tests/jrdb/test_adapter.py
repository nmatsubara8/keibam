"""JRDB→netkeiba raw スキーマ・アダプタの単体テスト（合成 SED で写像を検証）。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._adapter import build_raw_results


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


def test_abnormal_and_blanks():
    out = build_raw_results(_sed_rows())
    r1 = out.iloc[1]
    assert r1["着順"] == "除"                # 異常区分2→除外マーカー
    assert pd.isna(r1["タイム"])             # 空タイム
    assert pd.isna(r1["通過"])               # 全コーナー0
    assert r1["馬体重"] == "500"             # 増減空→体重のみ
    assert pd.isna(r1["単勝"])               # 空オッズ


def test_crosswalk_ids_attached():
    horse = pd.DataFrame({"ketto": ["18103588"], "horse_id": ["2018103588"]})
    jockey = pd.DataFrame({"kishu_code": ["01001"], "jockey_id": ["j641"]})
    trainer = pd.DataFrame({"chokyo_code": ["05001"], "trainer_id": ["t362"]})
    out = build_raw_results(_sed_rows(), horse_xwalk=horse, jockey_xwalk=jockey,
                            trainer_xwalk=trainer)
    r0 = out.iloc[0]
    assert r0["horse_id"] == "2018103588"    # crosswalk 付与
    assert r0["jockey_id"] == "j641"
    assert r0["trainer_id"] == "t362"
    # 対応の無いコードは欠損
    assert pd.isna(out.iloc[1]["horse_id"])  # 18103599 は未対応


def test_empty_input():
    assert build_raw_results(pd.DataFrame()).empty
