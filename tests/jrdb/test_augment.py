"""JRDB 特徴量付与（attach）の単体テスト。結合・乖離・前走トラブルのリーク無し参照を検証。"""
from __future__ import annotations

import pandas as pd

from src.jrdb._augment import attach


def _featured():
    # 2レース。R1(2020-02-01) の馬1, R2(2020-03-01) の馬1。index=race_id。
    return pd.DataFrame(
        {"馬番": [1, 2, 1], "単勝": [5.0, 8.0, 4.0],
         "date": ["2020-02-01", "2020-02-01", "2020-03-01"]},
        index=["202005010101", "202005010101", "202005010201"],
    )


def _kyi():
    return pd.DataFrame({
        "race_id": ["202005010101", "202005010101", "202005010201"],
        "umaban": [1, 2, 1],
        "ketto": ["20170001", "20170002", "20170001"],  # R1馬1 と R2馬1 は同一馬
        "jrdb_idm": [45.0, 30.0, 46.0],
        "jrdb_kijun_odds": [4.0, 9.0, 3.5],
        "jrdb_kijun_ninki": [1, 3, 1],
    })


def _history():
    # ketto 20170001 は 2020-02-01 に不利(trouble)を受けた（→ R2 の前走トラブル）
    return pd.DataFrame({
        "ketto": ["20170001"],
        "hist_date": pd.to_datetime(["2020-02-01"]),
        "prev_deokure": [0],
        "prev_trouble": [1],
    })


def test_attach_kijun_gap():
    out = attach(_featured(), _kyi(), _history())
    # R1馬1: 基準4.0 / 市場5.0 = 0.8
    assert out.loc["202005010101"].iloc[0]["jrdb_kijun_odds"] == 4.0
    assert abs(out.loc["202005010101"].iloc[0]["jrdb_kijun_gap"] - 0.8) < 1e-9
    assert out.loc["202005010101"].iloc[0]["jrdb_idm"] == 45.0


def test_prev_trouble_leak_safe():
    out = attach(_featured(), _kyi(), _history())
    # R2馬1(2020-03-01): 前走(2020-02-01)で trouble → prev_trouble=1
    r2 = out.loc["202005010201"]
    assert r2["prev_trouble"] == 1
    # R1馬1(2020-02-01): その日のtroubleは「今走」なので参照しない（exact不可）→ 前走なし=NaN
    r1 = out.loc["202005010101"].iloc[0]
    assert pd.isna(r1["prev_trouble"])


def test_attach_asof_handles_japanese_date():
    # 続36 回帰: netkeiba featured の date は 'YYYY年MM月DD日'。既定 pd.to_datetime はこれを
    # 全 NaT にし history/soten の asof が空結合＝prev_*/jrdb_ms_* 全欠測になっていた。
    # robust パーサで日本語表記でも asof が成立し前走トラブルが貼られることを保証する。
    feat = pd.DataFrame(
        {"馬番": [1, 2, 1], "単勝": [5.0, 8.0, 4.0],
         "date": ["2020年02月01日", "2020年02月01日", "2020年03月01日"]},
        index=["202005010101", "202005010101", "202005010201"],
    )
    out = attach(feat, _kyi(), _history())
    assert out.loc["202005010201"]["prev_trouble"] == 1        # 前走(2/1)の trouble が貼られる
    assert pd.isna(out.loc["202005010101"].iloc[0]["prev_trouble"])  # 今走当日は exact不可で除外


def test_kakutei_bataijuu_not_from_kyi():
    # 続36 WRONG_SOURCE: 確定馬体重は KYI 由来にしない（発走前は 0/空・DEAD）。TYB へ移譲。
    from src.jrdb._augment import KYI_FEATURE_MAP
    assert "kakutei_bataijuu" not in KYI_FEATURE_MAP
    assert "jrdb_kakutei_bataijuu" not in KYI_FEATURE_MAP.values()


def test_attach_empty_jrdb_is_safe():
    out = attach(_featured(), pd.DataFrame(), pd.DataFrame())
    assert "jrdb_kijun_odds" in out.columns
    assert out["jrdb_kijun_odds"].isna().all()


def test_attach_is_idempotent_no_xy_dupes():
    # 既に attach 済みの featured へ再適用しても jrdb_*_x/_y 重複列を作らない（冪等・二重マージ防止）。
    once = attach(_featured(), _kyi(), pd.DataFrame())
    assert "jrdb_idm" in once.columns
    twice = attach(once, _kyi(), pd.DataFrame())
    assert not any(str(c).endswith(("_x", "_y")) for c in twice.columns)  # 重複列なし
    assert "jrdb_idm" in twice.columns and "jrdb_idm_x" not in twice.columns
    assert set(once.columns) == set(twice.columns)                        # 列集合は不変
    # 値も一度目と一致（再付与で壊れない）
    assert twice["jrdb_idm"].tolist() == once["jrdb_idm"].tolist()


# df-native builders（store 経路・本線統合で再利用）のテスト
def test_build_kyi_from_df():
    import pandas as pd
    from src.jrdb._augment import build_kyi_from_df
    raw = pd.DataFrame({"race_id": ["R1"], "umaban": [1], "ketto": ["k"],
                        "idm": [55.0], "kishu_idx": [50.0], "joho_idx": [48.0],
                        "ten_idx": [30.0], "pace_yosou": ["H"]})
    out = build_kyi_from_df(raw)
    assert out["jrdb_idm"].iloc[0] == 55.0
    assert out["jrdb_ten_idx"].iloc[0] == 30.0      # 従来 ABSENT だった指数も df から出る
    assert out["jrdb_pace_hms"].iloc[0] == 1.0       # H→+1


def test_build_history_from_dfs():
    import pandas as pd
    from src.jrdb._augment import build_history_from_dfs
    sed = pd.DataFrame({"ketto": ["k"], "ymd": ["20250101"], "deokure": [3.0]})
    skb = pd.DataFrame({"ketto": ["k"], "ymd": ["20250201"], "tokki1": ["955"],
                        "tokki2": ["000"]})     # 955=進路なし(TROUBLE)
    h = build_history_from_dfs(sed, skb)
    assert set(h.columns) >= {"ketto", "hist_date", "prev_deokure", "prev_trouble"}
    assert int(h[h["hist_date"] == pd.Timestamp("2025-01-01")]["prev_deokure"].iloc[0]) == 1
    assert int(h[h["hist_date"] == pd.Timestamp("2025-02-01")]["prev_trouble"].iloc[0]) == 1


def test_build_soten_from_df():
    import pandas as pd
    from src.jrdb._augment import MYSPEED_COLS, build_soten_from_df
    sed = pd.DataFrame({"ketto": ["k", "k", "k"], "ymd": ["20240101", "20240201", "20240301"],
                        "soten": [50.0, 60.0, 70.0]})
    out = build_soten_from_df(sed)
    assert set(MYSPEED_COLS) <= set(out.columns)
    assert len(out) == 3 and out["jrdb_ms_npast"].max() == 3
