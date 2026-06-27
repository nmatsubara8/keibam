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

# ──────────────────────────────────────────
# §2k: ペアワイズ Elo レーティング（着差補正つき）— Phase 1
# ──────────────────────────────────────────

ELO_INITIAL_RATING: float = 1500.0  # 初出走馬の初期レーティング
ELO_BASE_K: float = 16.0            # 基本更新係数（全対戦相手平均で正規化される）
ELO_MARGIN_REF: float = 5.0         # 着差スケールの基準馬身（margin_k の逓減基準）

ELO_FEATURE_COLS: list = [
    "elo_rating",      # 出走前時点の馬レーティング
    "elo_n_races",     # それまでの出走数（レーティングの信頼度の代理）
    "elo_field_mean",  # 当該レース出走馬レーティングの平均（フィールド強度）
    "elo_vs_field",    # elo_rating - elo_field_mean（レース内相対強さ）
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
