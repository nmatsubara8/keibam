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
    "wakuban": (324, 1),     # 枠番（1-8・枠連用。JRA分割規則と一致を確認済み）
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

# TYB 直前情報データ（第4b版・2022.08.22）。レコード長128。発走15分前頃更新。
# 前日KYIに無い直前情報: オッズ指数/パドック指数（馬体・気配の直前評価）＋直前オッズ/馬体重。
TYB = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "idm": (11, 5),           # 前日と同じ
    "kishu_idx": (16, 5),
    "joho_idx": (21, 5),
    "odds_idx": (26, 5),      # オッズ指数（直前）
    "paddock_idx": (31, 5),   # パドック指数（直前・馬体評価）
    "sougou_idx": (41, 5),    # 総合指数（直前）
    "bagu_change": (46, 1),   # 馬具変更情報
    "ashimoto_info": (47, 1), # 脚元情報（0平行/1良化/2疑問/3悪化）
    "torikeshi": (48, 1),     # 取消フラグ
    "tansho_odds": (73, 6),   # 単勝オッズ（直前・ZZZ9.9）
    "fukusho_odds": (79, 6),  # 複勝オッズ下
    "odds_time": (85, 4),     # オッズ取得時間 HHMM
    "bataijuu": (89, 3),      # 馬体重
    "odds_mark": (95, 1),     # オッズ印
    "paddock_mark": (96, 1),  # パドック印
    "chokuzen_mark": (97, 1), # 直前総合印
}

# CYB 調教分析データ（第2版e・2019.05.17）。レコード長96（CRLF含む）。金土 19:00 更新。
# 中間の調教過程を分析。追切指数/仕上指数/調教量評価/調教評価が市場と直交しうる調教シグナル。
CYB = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "chokyo_type": (11, 2),              # 調教タイプ（コード）
    "chokyo_course_shubetsu": (13, 1),   # 調教コース種別（コード）
    "course_saka": (14, 2),              # 坂路 01:有/00:無
    "course_wood": (16, 2),              # ウッドコース
    "course_dirt": (18, 2),              # ダートコース
    "course_shiba": (20, 2),             # 芝コース
    "course_pool": (22, 2),              # プール調教
    "course_shou": (24, 2),              # 障害練習
    "course_poly": (26, 2),              # ポリトラック
    "chokyo_kyori": (28, 1),             # 調教距離 1長め/2普通/3短め/4:2本/0他
    "chokyo_juten": (29, 1),             # 調教重点 1テン/2中間/3終い/4平均/0他
    "oikiri_idx": (30, 3),               # 追切指数（ZZ9・調教時計の指数。左詰め既知バグ→strip で吸収）
    "shiage_idx": (33, 3),               # 仕上指数（仕上り状態の指数）
    "chokyo_ryo_hyoka": (36, 1),         # 調教量評価 A/B/C/D
    "shiage_idx_change": (37, 1),        # 仕上指数変化
    "chokyo_comment": (38, 40),          # 調教コメント（全角）
    "comment_ymd": (78, 8),              # コメント対象の調教日
    "chokyo_hyoka": (86, 1),             # 調教評価 3段階 1:◎/2:○/3:△
    "isshumae_oikiri_idx": (87, 3),      # 一週前追切指数
    "isshumae_oikiri_course": (90, 2),   # 一週前追切コース
    # 予備(92,3) / 改行(95,2) は取り込まない
}

# CHA 調教本追切データ（第1版c・2017.02.10）。レコード長64（CRLF含む）。金土 20:00 更新。
# レース前の本追切を分析。テン/中間/終い の部分別ハロンタイム＋各指数＋併せ馬結果。
CHA = {
    "race_key": (1, 8),
    "umaban": (9, 2),
    "youbi": (11, 2),                 # 曜日（基本 水/木）
    "chokyo_ymd": (13, 8),            # 調教年月日 YYYYMMDD
    "kaisuu": (21, 1),                # 本追切当日の追切回数合計
    "chokyo_course_code": (22, 2),    # 調教コースコード
    "oikiri_shurui": (24, 1),         # 追切種類 1:一杯/2:強目/3:馬なり
    "oi_jotai": (25, 2),              # 追い状態（コード）
    "noriyaku": (27, 1),              # 乗り役 1助手/2調教師/3本番騎手/4調教騎手/5見習
    "chokyo_f": (28, 1),              # 調教ハロンの長さ
    "ten_f": (29, 3),                 # テンＦ（最初の1〜2ハロン）
    "naka_f": (32, 3),                # 中間Ｆ
    "shimai_f": (35, 3),              # 終いＦ（最後の1ハロン）
    "ten_f_idx": (38, 3),             # テンＦ指数
    "naka_f_idx": (41, 3),            # 中間Ｆ指数
    "shimai_f_idx": (44, 3),          # 終いＦ指数
    "oikiri_idx": (47, 3),            # 追切指数
    "awase_kekka": (50, 1),           # 併せ結果 1:先着/2:同入/3:遅れ
    "awase_oikiri_shurui": (51, 1),   # 併せ相手の追切種類
    "awase_nenrei": (52, 2),          # 併せ相手の年齢
    "awase_class": (54, 2),           # 併せ相手のクラス（競走条件コード）
    # 予備(56,7) / 改行(63,2) は取り込まない
}

RECORD_LEN = {"KYI": 1024, "SED": 376, "SKB": 304, "TYB": 128, "CYB": 96, "CHA": 64}
