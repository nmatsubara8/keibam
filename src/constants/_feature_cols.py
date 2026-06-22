"""§2b〜§2j 特徴量エンジニアリングの定数。

マジックナンバーをコードに散らさず、変更をこのファイルに局所化する。
"""

# ──────────────────────────────────────────
# §2i: 多窓・多統計量集計
# ──────────────────────────────────────────

# 過去成績の集計窓サイズ（レース数）
N_RACES_LIST: list = [5, 9, 20]

# horse_id ごとに集計する統計量
AGG_STATS: list = ["mean", "std", "max", "min", "median"]

# ──────────────────────────────────────────
# §2c: 騎手・調教師集計特徴量
# ──────────────────────────────────────────

JOCKEY_RECENT_N: int = 30  # 直近N戦

JOCKEY_TRAINER_FEATURE_COLS: list = [
    "jockey_win_rate",
    "jockey_avg_rank",
    "trainer_win_rate",
    "trainer_avg_rank",
    "owner_win_rate",
    "owner_avg_rank",
]

# ──────────────────────────────────────────
# §2d: 脚質集計特徴量
# ──────────────────────────────────────────

PACE_RECENT_N: int = 5  # 直近N戦

# ペース列の文字値→数値マッピング（逃=0, 先=1, 差=2, 追=3）
PACE_CATEGORY_MAP: dict = {
    "逃": 0,
    "先": 1,
    "差": 2,
    "追": 3,
}

PACE_FEATURE_COLS: list = [
    "pace_median",        # 直近N戦の脚質中央値
    "pace_at_distance",   # 同距離帯での脚質中央値
    "leg_type_binary",    # 逃/先=0、差/追=1 の二値フラグ
]

# ──────────────────────────────────────────
# §2e: コース条件別集計特徴量
# ──────────────────────────────────────────

COURSE_CONDITION_FEATURE_COLS: list = [
    "win_rate_at_distance",     # 同距離帯(±100m)での勝率
    "avg_rank_at_course_type",  # 同コース種別での平均着順(相対値)
]

# ──────────────────────────────────────────
# §2j: 種牡馬集計特徴量
# ──────────────────────────────────────────

SIRE_RECENT_YEARS: int = 3  # 直近N年

SIRE_FEATURE_COLS: list = [
    "sire_win_rate",          # 種牡馬産駒の全期間勝率
    "sire_avg_rank",          # 種牡馬産駒の全期間平均着順
    "sire_recent_win_rate",   # 直近N年の種牡馬産駒勝率
]

# 母父（broodmare sire = peds_2）の産駒集計（DataMerger._add_damsire_stats）
DAMSIRE_FEATURE_COLS: list = [
    "damsire_win_rate",         # 母父産駒の全期間勝率
    "damsire_avg_rank",         # 母父産駒の全期間平均着順
    "damsire_recent_win_rate",  # 直近N年の母父産駒勝率
]

# ──────────────────────────────────────────
# §2k: 成長/フォーム・トレンド特徴量
# ──────────────────────────────────────────

GROWTH_FEATURE_COLS: list = [
    "growth_trend",  # 直近3走 − それ以前の平均相対着順（負=上昇基調=成長/復調）
    "n_starts",      # 過去出走数（キャリアの厚み）
]

# ──────────────────────────────────────────
# §2m: 前走比較・行内導出特徴量（Batch A）
# ──────────────────────────────────────────

# 前走との比較（DataMerger._add_prev_race_features）
PREV_RACE_FEATURE_COLS: list = [
    "dist_change",        # 今回 − 前走の距離（正=延長・負=短縮）
    "dist_change_ratio",  # dist_change ÷ 前走距離（相対距離変化）
    "kinryo_delta",       # 今回 − 前走の斤量
    "jockey_change",      # 乗り替わりフラグ（1=替わり）
]

# 相手強度（軽量代理・DataMerger._add_opponent_strength_stats）
# 過去走のレース格(grade)を ordinal 化して集計。名寄せ不要・リーク無し。
OPPONENT_STRENGTH_FEATURE_COLS: list = [
    "faced_grade_max",     # 過去最高グレード（実力の天井）
    "faced_grade_mean",    # 平均グレード（普段の相手レベル）
    "faced_graded_count",  # 重賞(G3+)出走回数
]

# 行内導出（FeatureEngineering.add_derived_features）
DERIVED_FEATURE_COLS: list = [
    "単勝_log",            # log1p(単勝)
    "kinryo_per_weight",  # 斤量 ÷ 馬体重
    "is_layoff",          # 休み明けフラグ
    "is_back_to_back",    # 連闘フラグ
]

# ──────────────────────────────────────────
# §2n: 適性特徴量（Batch B: 馬場・競馬場）
# ──────────────────────────────────────────

APTITUDE_FEATURE_COLS: list = [
    "wet_win_rate",    # 道悪（稍重/重/不良）での勝率
    "wet_rel_rank",    # 道悪での相対着順（着順/頭数）
    "place_win_rate",  # 今回と同じ競馬場での勝率
]

# §2i 多窓集計の対象列（horse_id 単独集計）。着順に加え能力・終盤脚力・実績を集計。
AGG_TARGET_COLS: list = ["着順", "着差", "上り", "賞金"]

# ──────────────────────────────────────────
# §2l: スピード指数（タイム偏差）集計
# ──────────────────────────────────────────

SPEED_FIGURE_FEATURE_COLS: list = [
    "speed_fig_best",    # 過去最高スピード指数（ピーク能力）
    "speed_fig_mean5",   # 直近5走平均（現在の地力・調子）
]

# ──────────────────────────────────────────
# §2g: レース内 Z-score 対象列
# ──────────────────────────────────────────

# グループ1: 現レース特徴量（比較に意味がある数値列）
RACE_LEVEL_ZSCORE_COLS_G1: list = [
    "体重",       # 馬体重
    "体重変化",   # 体重変化
    "斤量",       # 騎手重量
    "単勝",       # 単勝オッズ
    "年齢",       # 年齢
    "interval",   # 前走からの経過日数
    "age_days",   # 日齢
]

# グループ2: 過去成績の集計値（レース内の相対比較を可能にする）
RACE_LEVEL_ZSCORE_COLS_G2: list = [
    # §2i の多窓集計列（_mean サフィックス付き）— FeatureEngineering で動的決定
    # 例: "着順_mean_5R", "着順_mean_9R" など
]

# 両グループを合わせた全対象列（_z サフィックスで追加）
RACE_LEVEL_ZSCORE_COLS: list = RACE_LEVEL_ZSCORE_COLS_G1 + RACE_LEVEL_ZSCORE_COLS_G2

# ──────────────────────────────────────────
# §2b: 交互作用特徴量
# ──────────────────────────────────────────

INTERACTION_FEATURE_COLS: list = [
    "frame_x_course",     # 枠番 × コース種別
    "sex_x_month_sin",    # 性別 × 出走月 (sin)
    "sex_x_month_cos",    # 性別 × 出走月 (cos)
    "distance_x_around",  # 距離 × 回り
]
