"""JRDB 固定長レコードのフィールドレイアウト（相対=1始まりバイト位置, 長さ）。

仕様書（KYI/SED/SKB doc）で確認した優先フィールドのみを定義。cp932。
特記/馬具/脚元など繰り返しは (start, unit_len, count) で表現する。
"""
from __future__ import annotations

# 各項目: name -> (start_1indexed, length)。数値は後段でパース。
KYI = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "ketto": (11, 8),
    "bamei": (19, 36),
    "idm": (55, 5),          # ZZ9.9
    "kyakushitsu": (90, 1),  # 脚質
    "kyori_tekisei": (91, 1),
    "joushoudo": (92, 1),
    "kijun_odds": (96, 5),   # ZZ9.9 基準オッズ（Benter核）
    "kijun_ninki": (101, 2),
    "kishu_code": (336, 5),
    "chokyo_code": (341, 5),
}

SED = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "ketto": (11, 8),
    "ymd": (19, 8),          # YYYYMMDD（過去走の日付）
    "chakujun": (141, 2),    # 着順
    "kakutei_tansho": (175, 6),
    "idm": (183, 3),
    "pace": (192, 3),
    "deokure": (195, 3),     # 出遅
    "ichidori": (198, 3),    # 位置取
    "furi": (201, 3),        # 不利
    "mae_furi": (204, 3),    # 前不利
    "naka_furi": (207, 3),   # 中不利
    "ato_furi": (210, 3),    # 後不利
    "race_pace": (222, 1),   # H/M/S
    "uma_pace": (223, 1),
    "bataijuu": (333, 3),
    "race_kyakushitsu": (341, 1),
}

# SKB: 特記コード 3桁×6 @27、馬具 3桁×8 @45、脚元(総合)@69
SKB = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "ketto": (11, 8),
    "ymd": (19, 8),
}
SKB_REPEAT = {
    "tokki": (27, 3, 6),     # 特記コード（不利/出遅れ等）×6
    "bagu": (45, 3, 8),      # 馬具コード×8
}
SKB_ASHIMOTO = {"ashimoto_sougou": (69, 3)}  # 脚元(総合)

RECORD_LEN = {"KYI": 1024, "SED": 376, "SKB": 304}
