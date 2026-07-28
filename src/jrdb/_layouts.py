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
    "kijun_fuku_ninki": (108, 2),  # 基準複勝人気順位
    # ── 特定情報印数（◎本命/○対抗/▲単穴/△連下/×消 の専門紙印集計）──
    "tokutei_honmei": (110, 3),
    "tokutei_taikou": (113, 3),
    "tokutei_tanana": (116, 3),
    "tokutei_renka": (119, 3),
    "tokutei_kesi": (122, 3),
    # ── 総合情報印数 ──
    "sougou_honmei": (125, 3),
    "sougou_taikou": (128, 3),
    "sougou_tanana": (131, 3),
    "sougou_renka": (134, 3),
    "sougou_kesi": (137, 3),
    # ── 指数群（続き）──
    "ninki_idx": (140, 5),   # 人気指数（ZZZZ9）
    "chokyo_idx": (145, 5),  # 調教指数
    "kyusha_idx": (150, 5),  # 厩舎指数
    "chokyo_yajirushi": (155, 1),  # 調教矢印コード（上昇/平行/下降）
    "kyusha_hyoka": (156, 1),      # 厩舎評価コード
    "kishu_kitai_rentai": (157, 4),  # 騎手期待連対率 Z9.9
    "gekiso_idx": (161, 3),  # 激走指数
    "hitsume_code": (164, 2),      # 蹄コード
    "omoteki_code": (166, 1),      # 重適正コード（重馬場適性）
    "class_code": (167, 2),
    "blinker": (171, 1),           # ブリンカー
    "kishu_name": (172, 12),       # 騎手名（全角6文字）
    "futan_juryo": (184, 3),       # 負担重量（0.1kg単位）
    "minarai_kubun": (187, 1),     # 見習い区分（減量）
    "chokyoshi_name": (188, 12),   # 調教師名（全角6文字）
    "chokyoshi_shozoku": (200, 4), # 調教師所属
    # ── 前走1-5 競走成績キー / レースキー（過去走リンク）──
    "zenso1_seiseki_key": (204, 16),
    "zenso2_seiseki_key": (220, 16),
    "zenso3_seiseki_key": (236, 16),
    "zenso4_seiseki_key": (252, 16),
    "zenso5_seiseki_key": (268, 16),
    "zenso1_race_key": (284, 8),
    "zenso2_race_key": (292, 8),
    "zenso3_race_key": (300, 8),
    "zenso4_race_key": (308, 8),
    "zenso5_race_key": (316, 8),
    "wakuban": (324, 1),     # 枠番（1-8・枠連用。JRA分割規則と一致を確認済み）
    # ── JRDB 予想印コード（総合/IDM/情報/騎手/厩舎/調教/激走）──
    "mark_sougou": (327, 1),
    "mark_idm": (328, 1),
    "mark_joho": (329, 1),
    "mark_kishu": (330, 1),
    "mark_kyusha": (331, 1),
    "mark_chokyo": (332, 1),
    "mark_gekiso": (333, 1),
    "shiba_tekisei": (334, 1),     # 芝適性コード
    "dirt_tekisei": (335, 1),      # ダ適性コード
    # ── リンク用 ID ──
    "kishu_code": (336, 5),
    "chokyo_code": (341, 5),
    # ── 賞金・条件 ──
    "kakutoku_shokin": (346, 6),   # 獲得賞金（単位:百円）
    "shutoku_shokin": (352, 6),    # 収得賞金
    "joken_class": (358, 1),       # 条件クラス
    # ── 展開予想データ（第6版・JRDB が z=ペースを直接予測＝P(z) の外部教師）──
    "ten_idx": (359, 5),     # 予想テン指数 ZZZ.9
    "pace_idx": (364, 5),    # 予想ペース指数
    "agari_idx": (369, 5),   # 予想上がり指数
    "ichi_idx": (374, 5),    # 予想位置指数
    "pace_yosou": (379, 1),  # ペース予想 H/M/S（＝潜在状態 z の JRDB 版）
    "dochu_juni": (380, 2),  # 道中順位
    "dochu_sa": (382, 2),          # 道中差
    "dochu_uchisoto": (384, 1),    # 道中内外
    "go3f_juni": (385, 2),   # 後3F順位
    "go3f_sa": (387, 2),           # 後3F差
    "go3f_uchisoto": (389, 1),     # 後3F内外
    "goal_juni": (390, 2),   # ゴール順位
    "goal_sa": (392, 2),           # ゴール差
    "goal_uchisoto": (394, 1),     # ゴール内外
    "tenkai_kigou": (395, 1),      # 展開記号
    "kyori_tekisei2": (396, 1),    # 距離適性2
    # ── コンディション/休養（第6a/10/11版・AbilityFilter の調子項に効く）──
    "kakutei_bataijuu": (397, 3),  # 枠確定馬体重
    "kakutei_bataijuu_zougen": (400, 3),  # 枠確定馬体重増減
    "torikeshi_flag": (403, 1),    # 取消フラグ
    "sex_code": (404, 1),          # 性別コード
    "owner_name": (405, 40),       # 馬主名
    "owner_kai_code": (445, 2),    # 馬主会コード
    "umakigou_code": (447, 2),     # 馬記号コード
    # ── 各種指数順位（レース内相対順位）──
    "gekiso_juni": (449, 2),
    "ls_juni": (451, 2),
    "ten_juni": (453, 2),
    "pace_juni": (455, 2),
    "agari_juni": (457, 2),
    "ichi_juni": (459, 2),
    # ── 騎手期待（第8版）──
    "kishu_tansho": (461, 4),  # 騎手期待単勝率 Z9.9
    "kishu_3nai": (465, 4),    # 騎手期待3着内率 Z9.9
    "yusou_kubun": (469, 1),       # 輸送区分
    "souhou": (470, 8),            # 走法
    "taikei": (478, 24),           # 体型（3桁×8部位）
    "taikei_sougou1": (502, 3),
    "taikei_sougou2": (505, 3),
    "taikei_sougou3": (508, 3),
    "uma_tokki1": (511, 3),        # 馬特記
    "uma_tokki2": (514, 3),
    "uma_tokki3": (517, 3),
    # ── スタート予測（第9版・前走ではなく今走の出遅れ予測）──
    "start_idx": (520, 4),   # 馬スタート指数 Z9.9
    "deokure_rate": (524, 4),  # 馬出遅率 Z9.9
    "sankou_zenso": (528, 2),      # 参考前走
    "sankou_zenso_kishu_code": (530, 5),  # 参考前走騎手コード
    "manken_idx": (535, 3),  # 万券指数
    "manken_mark": (538, 1),       # 万券印
    "kokyu_flag": (539, 1),  # 降級フラグ（1降級 2二段階 0通常）
    "gekiso_type": (540, 2),       # 激走タイプ
    "kyuyou_riyu": (542, 2),       # 休養理由分類コード
    "flag": (544, 16),             # フラグ（各種2値フラグ列）
    # ── 休養明け（第11版）──
    "nyukyu_nsoume": (560, 2),     # 入厩何走目
    "nyukyu_ymd": (562, 8),        # 入厩年月日 YYYYMMDD
    "nyukyu_days": (570, 3),   # 入厩何日前（レース日から遡っての入厩日数）
    "houboku_saki": (573, 50),     # 放牧先
    "houboku_rank": (623, 1),      # 放牧先ランク
    "kyusha_rank": (624, 1),       # 厩舎ランク
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

# HJC 払戻情報データ（第4a版・2024.09.29）。レコード長444。**レース単位**（1レース1レコード）。
# 券種ごとに OCC 回の (組合せ, 払戻金) を繰り返す。連系(馬連/ワイド/馬単/三連複/三連単)の
# 払戻を持ち、exotic payoff での ROI 検証に必須。
# グループ: (prefix, 開始相対位置, OCC回数, 組合せ長, 払戻金長)
HJC_GROUPS = [
    ("tansho", 9, 3, 2, 7),        # 単勝  馬番2 + 払戻7
    ("fukusho", 36, 5, 2, 7),      # 複勝  馬番2 + 払戻7
    ("wakuren", 81, 3, 2, 7),      # 枠連  枠組合せ2 + 払戻7
    ("umaren", 108, 3, 4, 8),      # 馬連  馬番組合せ4 + 払戻8
    ("wide", 144, 7, 4, 8),        # ワイド 馬番組合せ4 + 払戻8
    ("umatan", 228, 6, 4, 8),      # 馬単  馬番組合せ4 + 払戻8
    ("sanrenpuku", 300, 3, 6, 8),  # 三連複 馬番組合せ6 + 払戻8
    ("sanrentan", 342, 6, 6, 9),   # 三連単 馬番組合せ6 + 払戻9
]

# KKA 競走馬拡張データ（第2版・2007.10.22）。レコード長324。出走馬単位 (race_id, 馬番)。
# 各「着度数」は 12byte = 4×3(ZZ9) の (1着数,2着数,3着数,着外数)。条件別の勝率/連対率の材料。
_KKA_CHAKU_GROUPS = [
    ("jra", 11), ("kouryu", 23), ("hoka", 35),               # JRA/交流/他
    ("shibada", 47), ("shibada_dist", 59), ("track_dist", 71),  # 芝ダ障害別/距離別/トラック距離
    ("rote", 83), ("mawari", 95), ("kishu", 107),            # ローテ/回り/騎手
    ("baba_ryo", 119), ("baba_yaya", 131), ("baba_omo", 143),  # 良/稍/重
    ("pace_s", 155), ("pace_m", 167), ("pace_h", 179),       # S/M/H ペース
    ("season", 191), ("waku", 203),                          # 季節/枠
    ("kishu_dist", 215), ("kishu_track_dist", 227), ("kishu_chokyoshi", 239),
    ("kishu_banushi", 251), ("kishu_blinker", 263), ("chokyoshi_banushi", 275),
]
KKA = {"race_key": (1, 8), "umaban": (9, 2)}
for _nm, _st in _KKA_CHAKU_GROUPS:
    for _j, _suf in enumerate(("1chaku", "2chaku", "3chaku", "chakugai")):
        KKA[f"{_nm}_{_suf}"] = (_st + _j * 3, 3)
KKA.update({
    "sire_shiba_rentai": (287, 3),      # 父馬産駒芝連対率 %
    "sire_dirt_rentai": (290, 3),       # 父馬産駒ダ連対率 %
    "sire_rentai_avg_dist": (293, 4),   # 父馬産駒連対平均距離
    "bms_shiba_rentai": (297, 3),       # 母父産駒芝連対率 %
    "bms_dirt_rentai": (300, 3),        # 母父産駒ダ連対率 %
    "bms_rentai_avg_dist": (303, 4),    # 母父産駒連対平均距離
})

# UKC 馬基本データ（第3版・2002.08.20）。レコード長292。**馬マスタ**（血統登録番号単位）。
# 血統/毛色/生年月日/父母/馬主/生産者/系統コード。netkeiba 血統と同種だが JRDB 系統コード付き。
UKC = {
    "ketto": (1, 8),                    # 血統登録番号
    "bamei": (9, 36),                   # 馬名
    "sex_code": (45, 1),                # 性別 1牡/2牝/3セン
    "keiro_code": (46, 2),              # 毛色コード
    "umakigou_code": (48, 2),           # 馬記号コード
    "sire_name": (50, 36),              # 父馬名
    "dam_name": (86, 36),               # 母馬名
    "bms_name": (122, 36),              # 母父馬名
    "birth_ymd": (158, 8),              # 生年月日 YYYYMMDD
    "sire_birth_year": (166, 4),        # 父馬生年
    "dam_birth_year": (170, 4),         # 母馬生年
    "bms_birth_year": (174, 4),         # 母父馬生年
    "owner_name": (178, 40),            # 馬主名
    "owner_kai_code": (218, 2),         # 馬主会コード
    "breeder_name": (220, 40),          # 生産者名
    "sanchi_name": (260, 8),            # 産地名
    "massho_flag": (268, 1),            # 登録抹消フラグ 0現役/1抹消
    "data_ymd": (269, 8),              # データ年月日
    "sire_keito_code": (277, 4),        # 父系統コード
    "bms_keito_code": (281, 4),         # 母父系統コード
}

# SRB 成績レースデータ（第2版b・2015.06.01）。レコード長852。**レース単位**（1レース1レコード）。
# SED アーカイブに同梱（SED*.zip/lzh 内に SRB*.txt）。ハロンタイム18・コーナー位置取り・
# トラックバイアス・ペースアップ位置＝連系の順序/展開の実測材料。
SRB = {"race_key": (1, 8)}
for _i in range(18):  # ハロンタイム 18×3（先頭馬の1ハロン毎タイム・0.1秒単位）
    SRB[f"harontime{_i + 1}"] = (9 + _i * 3, 3)
SRB.update({
    "corner1_pos": (63, 64),      # 1コーナー位置取り（馬番の並び）
    "corner2_pos": (127, 64),     # 2コーナー
    "corner3_pos": (191, 64),     # 3コーナー
    "corner4_pos": (255, 64),     # 4コーナー
    "pace_up_pos": (319, 2),      # ペースアップ位置（残りハロン数）
    "bias_1kaku": (321, 3),       # トラックバイアス 1角（内/中/外）
    "bias_2kaku": (324, 3),       # 2角
    "bias_mukou": (327, 3),       # 向正
    "bias_3kaku": (330, 3),       # 3角
    "bias_4kaku": (333, 5),       # 4角（最内〜大外）
    "bias_chokusen": (338, 5),    # 直線
    "race_comment": (343, 500),   # レースコメント
})

def _chaku4(prefix: str, start: int, width: int) -> dict:
    """成績「1-2-3-着外」の4分割（width=3 本年/昨年 ZZ9, 5 通算 ZZZZ9）。"""
    suffixes = ("1chaku", "2chaku", "3chaku", "chakugai")
    return {f"{prefix}_{s}": (start + i * width, width) for i, s in enumerate(suffixes)}


# KSA 騎手データ（第1版b・2016.10.01）。レコード長272。**マスタ**（騎手コード単位）。
# 木曜 19:00 更新。KYI/SED の騎手コード@336 とリンク。年別/通算の平地・障害成績＝
# 騎手の実力・近況の外部指標（市場オッズと部分的に直交しうる）。KZA=全騎手/KSA=今週分。
KSA = {
    "kishu_code": (1, 5),
    "massho_flag": (6, 1),             # 登録抹消フラグ 1抹消/0現役
    "massho_ymd": (7, 8),              # 登録抹消年月日 YYYYMMDD
    "kishu_name": (15, 12),            # 騎手名（全角6文字）
    "kishu_kana": (27, 30),            # 騎手カナ（全角15文字）
    "kishu_ryaku": (57, 6),            # 騎手名略称（全角3文字）
    "shozoku_code": (63, 1),           # 所属コード 1関東/2関西/3他
    "shozoku_chiiki": (64, 4),         # 所属地域名（地方の場合・全角2文字）
    "birth_ymd": (68, 8),              # 生年月日 YYYYMMDD
    "shomen_year": (76, 4),            # 初免許年 YYYY
    "minarai_kubun": (80, 1),          # 見習い区分 1☆(1K)/2△(2K)/3▲(3K)
    "shozoku_chokyoshi_code": (81, 5),  # 所属厩舎（調教師コード）
    "comment": (86, 40),               # 騎手コメント（JRDBスタッフ評価）
    "comment_ymd": (126, 8),           # コメント入力年月日
    "honnen_leading": (134, 3),        # 本年リーディング
    **_chaku4("honnen_heichi", 137, 3),   # 本年平地 1-2-3-着外
    **_chaku4("honnen_shogai", 149, 3),   # 本年障害 1-2-3-着外
    "honnen_tokubetsu": (161, 3),      # 本年特別勝数
    "honnen_juushou": (164, 3),        # 本年重賞勝数
    "sakunen_leading": (167, 3),       # 昨年リーディング
    **_chaku4("sakunen_heichi", 170, 3),
    **_chaku4("sakunen_shogai", 182, 3),
    "sakunen_tokubetsu": (194, 3),
    "sakunen_juushou": (197, 3),
    **_chaku4("tsusan_heichi", 200, 5),   # 通算平地 1-2-3-着外（5桁）
    **_chaku4("tsusan_shogai", 220, 5),   # 通算障害
    "data_ymd": (240, 8),              # データ年月日
}

# CSA 調教師データ（第1版・2001.05.16）。レコード長272。**マスタ**（調教師コード単位）。
# KSA と同型だが「見習い区分・所属厩舎」が無く、コメント以降が 6byte 前詰めになる。
# KYI/SED の調教師コード@341 とリンク。CZA=全調教師/CSA=今週分。
CSA = {
    "chokyoshi_code": (1, 5),
    "massho_flag": (6, 1),
    "massho_ymd": (7, 8),
    "chokyoshi_name": (15, 12),        # 調教師名（全角6文字）
    "chokyoshi_kana": (27, 30),        # 調教師カナ（全角15文字）
    "chokyoshi_ryaku": (57, 6),        # 調教師名略称（全角3文字）
    "shozoku_code": (63, 1),
    "shozoku_chiiki": (64, 4),
    "birth_ymd": (68, 8),
    "shomen_year": (76, 4),
    "comment": (80, 40),               # 調教師コメント（JRDBスタッフの厩舎見解）
    "comment_ymd": (120, 8),
    "honnen_leading": (128, 3),
    **_chaku4("honnen_heichi", 131, 3),
    **_chaku4("honnen_shogai", 143, 3),
    "honnen_tokubetsu": (155, 3),
    "honnen_juushou": (158, 3),
    "sakunen_leading": (161, 3),
    **_chaku4("sakunen_heichi", 164, 3),
    **_chaku4("sakunen_shogai", 176, 3),
    "sakunen_tokubetsu": (188, 3),
    "sakunen_juushou": (191, 3),
    **_chaku4("tsusan_heichi", 194, 5),
    **_chaku4("tsusan_shogai", 214, 5),
    "data_ymd": (234, 8),              # データ年月日 YYYYMMDD
}

RECORD_LEN = {"KYI": 1024, "SED": 376, "SKB": 304, "TYB": 128, "CYB": 96,
              "CHA": 64, "HJC": 444, "KKA": 324, "UKC": 292, "SRB": 852,
              "KSA": 272, "CSA": 272}
