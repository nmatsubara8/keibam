"""JRDB 固定長レコードのフィールドレイアウト（相対=1始まりバイト位置, 長さ）。

仕様書（KYI/SED/SKB doc）で確認した優先フィールドのみを定義。cp932。
特記/馬具/脚元など繰り返しは (start, unit_len, count) で表現する。
"""
from __future__ import annotations

# 各項目: name -> (start_1indexed, length)。数値は後段でパース。
# オフセットは KYI 仕様書 第11版（2023.06.25）で全項目照合済み。レコード長 1024。
KYI = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "ketto": (11, 8),
    "bamei": (19, 36),       # 全角18文字
    # ── 各種指数（JRDB 独自・市場から導けない直交情報）──
    "idm": (55, 5),          # ZZ9.9 総合能力指数（Benter核その1）
    "kishu_idx": (60, 5),    # 騎手指数
    "joho_idx": (65, 5),     # 情報指数（専門紙印等の集約）
    "sougou_idx": (85, 5),   # 総合指数
    # ── 脚質・適性・基準値 ──
    "kyakushitsu": (90, 1),  # 脚質（1逃 2先 3差 4追）
    "kyori_tekisei": (91, 1),
    "joushoudo": (92, 1),    # 上昇度
    "rotation": (93, 3),     # ローテーション（間の金曜数≒間隔の代理）
    "kijun_odds": (96, 5),   # ZZ9.9 基準オッズ（Benter核その2＝JRDBフェアバリュー）
    "kijun_ninki": (101, 2),
    "kijun_fukuodds": (103, 5),   # 基準複勝オッズ
    # ── 指数群（続き）──
    "ninki_idx": (140, 5),   # 人気指数（ZZZZ9）
    "chokyo_idx": (145, 5),  # 調教指数
    "kyusha_idx": (150, 5),  # 厩舎指数
    "chokyo_yajirushi": (155, 1),  # 調教矢印コード（上昇/平行/下降）
    "kyusha_hyoka": (156, 1),      # 厩舎評価コード
    "kishu_kitai_rentai": (157, 4),  # 騎手期待連対率 Z9.9
    "gekiso_idx": (161, 3),  # 激走指数
    "class_code": (167, 2),
    # ── 展開予想データ（第6版・JRDB が z=ペースを直接予測＝P(z) の外部教師）──
    "ten_idx": (359, 5),     # 予想テン指数 ZZZ.9
    "pace_idx": (364, 5),    # 予想ペース指数
    "agari_idx": (369, 5),   # 予想上がり指数
    "ichi_idx": (374, 5),    # 予想位置指数
    "pace_yosou": (379, 1),  # ペース予想 H/M/S（＝潜在状態 z の JRDB 版）
    "dochu_juni": (380, 2),  # 道中順位
    "go3f_juni": (385, 2),   # 後3F順位
    "goal_juni": (390, 2),   # ゴール順位
    # ── コンディション/休養（第6a/10/11版・AbilityFilter の調子項に効く）──
    "kakutei_bataijuu": (397, 3),  # 枠確定馬体重
    "kokyu_flag": (539, 1),  # 降級フラグ（1降級 2二段階 0通常）
    # ── スタート予測（第9版・前走ではなく今走の出遅れ予測）──
    "start_idx": (520, 4),   # 馬スタート指数 Z9.9
    "deokure_rate": (524, 4),  # 馬出遅率 Z9.9
    "manken_idx": (535, 3),  # 万券指数
    # ── 騎手期待（第8版）──
    "kishu_tansho": (461, 4),  # 騎手期待単勝率 Z9.9
    "kishu_3nai": (465, 4),    # 騎手期待3着内率 Z9.9
    # ── 休養明け（第11版）──
    "nyukyu_days": (570, 3),   # 入厩何日前（レース日から遡っての入厩日数）
    # ── リンク用 ID（特徴量ではない）──
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
    # 早期オッズ（SEC仕様の項目順＝確定複勝下→10時単勝→10時複勝 の3連続6byte を、
    # 実データの magnitude 三点一致で特定: @291複勝4.3 / @297単勝19.1 / @303複勝3.8）。
    # 10時単勝オッズ = 発走前朝10時の単勝オッズ＝歴史的な「早期市場」（早期オッズ路線の核）。
    "kakutei_fukusho_shita": (291, 6),  # 確定複勝オッズ下
    "odds_10_tansho": (297, 6),         # 10時単勝オッズ（早期市場）
    "odds_10_fukusho": (303, 6),        # 10時複勝オッズ
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
