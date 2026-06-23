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
    "avg_rank_at_distance",     # 同距離帯での平均着順(相対値)
    "n_runs_at_distance",       # 同距離帯での出走数(経験量)
    "win_rate_at_course_type",  # 同コース種別での勝率
    "avg_rank_at_course_type",  # 同コース種別での平均着順(相対値)
]

# レース種別×馬場状態 別の過去成績（DataMerger._add_type_ground_stats）
TYPE_GROUND_FEATURE_COLS: list = [
    "win_rate_type_ground",  # 同種別×同馬場での勝率
    "avg_rank_type_ground",  # 同種別×同馬場での平均着順(相対値)
    "n_runs_type_ground",    # 同種別×同馬場での出走数
]

# レースクラス（格）別の過去成績（DataMerger._add_race_class_stats）
RACE_CLASS_FEATURE_COLS: list = [
    "win_rate_same_class",    # 今回と同格での勝率
    "avg_rank_same_class",    # 今回と同格での平均着順(相対値)
    "n_runs_same_class",      # 今回と同格での出走数
    "win_rate_higher_class",  # 今回以上の格での勝率(格上で勝てる強さ)
    "best_class_won",         # 勝利した最高クラスの順序値(実績の天井)
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

# 母父（broodmare sire = peds_32）の産駒集計（DataMerger._add_damsire_stats）
DAMSIRE_FEATURE_COLS: list = [
    "damsire_win_rate",         # 母父産駒の全期間勝率
    "damsire_avg_rank",         # 母父産駒の全期間平均着順
    "damsire_recent_win_rate",  # 直近N年の母父産駒勝率
]

# 予想印コンセンサス（DataMerger._merge_yoso_marks）。発走前確定・リーク無し。
YOSO_FEATURE_COLS: list = [
    "yoso_n_marks",            # 印を付けた予想家数（注目度）
    "yoso_n_honmei",           # ◎の数
    "yoso_score_sum",          # 印スコア合計（◎5..☆1）
    "yoso_score_mean",         # 印スコア平均
    "yoso_n_marks_free",       # 無料予想家のみの印数
    "yoso_honmei_skill_sum",   # ◎を付けた予想家の as-of 的中率の合計（方式A 自前計算）
    "yoso_best_skill",         # 同・最大
    "yoso_profile_skill_sum",  # ◎を付けた予想家の profile由来◎1着率の合計（方式B1 prior）
    "yoso_profile_best",       # 同・最大
]

# 人物（騎手/調教師/馬主/生産者）の前年成績（as-of・DataMerger._merge_person_yearly）
PERSON_YEARLY_FEATURE_COLS: list = [
    "jockey_py_勝率", "jockey_py_複勝率", "jockey_py_芝勝率", "jockey_py_ダート勝率",
    "jockey_py_重賞勝利", "jockey_py_出走回数",
    "trainer_py_勝率", "trainer_py_複勝率", "trainer_py_芝勝率", "trainer_py_ダート勝率",
    "trainer_py_重賞勝利", "trainer_py_出走回数",
    "owner_py_勝率", "owner_py_複勝率", "owner_py_芝勝率", "owner_py_ダート勝率",
    "owner_py_重賞勝利", "owner_py_出走回数",
    "breeder_py_勝率", "breeder_py_複勝率", "breeder_py_芝勝率", "breeder_py_ダート勝率",
    "breeder_py_重賞勝利", "breeder_py_出走回数",
]

# 市場の歪み（複勝/三連複/三連単の確定オッズ vs 単勝由来 Harville・DataMerger._merge_odds_signals）
# 発走前確定オッズ由来でリーク無し（``単勝`` と同じ前提）。MARKET_SIGNAL_COLS と一致させる。
MARKET_SIGNAL_FEATURE_COLS: list = [
    "fukusho_implied_p",      # 複勝市場の implied 3着内確率
    "place_overlay",          # 複勝 implied − Harville複勝（市場間ズレ）
    "trio_top3_overlay",      # 三連複 top3 marginal − Harville複勝
    "trifecta_win_overlay",   # 三連単 1着 marginal − Harville勝率（連系のスマートマネー）
    "trifecta_top3_overlay",  # 三連単 top3 marginal − Harville複勝
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

# 開催日の周期性（FeatureEngineering.add_date_cyclical）。うるう年込みの季節符号化。
DATE_CYCLICAL_FEATURE_COLS: list = [
    "sin_date",  # sin(2π·年内通日/365.25) + 1
    "cos_date",  # cos(2π·年内通日/365.25) + 1
]

# 現レースの格の順序値（FeatureEngineering.add_race_class_level）。one-hot(race_class_*)と併用。
# レース内で一定（全頭同値）のため zscore 対象には含めない（レース間の比較に使う）。
RACE_CLASS_LEVEL_COL: str = "race_class_level"

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
