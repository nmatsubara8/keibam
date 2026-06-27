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
# §2l: TrueSkill（多頭順位対応 μ/σ）— Phase 2
# ──────────────────────────────────────────

TS_MU: float = 25.0              # 初期スキル平均 μ0
TS_SIGMA: float = 25.0 / 3.0     # 初期スキル標準偏差 σ0（≈8.333）
TS_BETA: float = 25.0 / 6.0      # パフォーマンスノイズ β（= σ0/2、スキルクラス幅）
TS_TAU: float = 25.0 / 300.0     # 動的変動 τ（= σ0/100、出走ごとに σ² へ加算）
TS_DRAW_MARGIN: float = 0.0      # 引分マージン ε（同着の更新用、既定 0）
TS_CONSERVATIVE_K: float = 3.0   # 保守的スキル μ - k·σ の k

# 列順は compute_trueskill_history の出力配列と一致させること。
TS_FEATURE_COLS: list = [
    "ts_mu",            # スキル平均 μ
    "ts_sigma",         # スキル不確かさ σ
    "ts_conservative",  # 保守的スキル μ - 3σ
    "ts_n_races",       # それまでの出走数
    "ts_field_mean",    # 当該レース保守的スキルの平均（フィールド強度）
    "ts_vs_field",      # ts_conservative - ts_field_mean（レース内相対強さ）
]

# ──────────────────────────────────────────
# §2m: 条件別 TrueSkill（芝/ダ・距離・回り）— Phase 3
# ──────────────────────────────────────────

# 条件次元と参照列（merged_data の生値列。dummify 前に結合するため生値で参照可能）。
COND_DIMENSIONS: list = ["surface", "distance", "around"]
COND_DIMENSION_COLUMN: dict = {
    "surface": "race_type",   # 芝 / ダート / 障害
    "distance": "course_len",  # 100m 単位（meters // 100）
    "around": "around",        # 右 / 左 / 直線
}

# 距離バケット境界（course_len = 100m 単位）。<14:短距離 / 14-17:マイル /
# 18-21:中距離 / >=22:長距離。
COND_DISTANCE_BIN_UNITS: list = [14, 18, 22]
COND_DISTANCE_LABELS: list = ["sprint", "mile", "middle", "long"]

# 各次元の特徴量列（保守的スキル / 当該条件の出走数 / フィールド相対）。
# 列順は compute_conditional_trueskill_history の出力配列と一致させること。
COND_TS_FEATURE_COLS: list = [
    f"ts_{d}_{suffix}"
    for d in COND_DIMENSIONS
    for suffix in ("conservative", "n_races", "vs_field")
]

# ──────────────────────────────────────────
# §2n: 能力 Kalman（局所線形トレンド・成長/疲労）— Phase 4
# ──────────────────────────────────────────

KF_INIT_LEVEL: float = 0.0        # 初期能力（TS 保守的スキルの prior と同じ 0 起点）
KF_INIT_TREND: float = 0.0        # 初期成長率
KF_INIT_VAR_LEVEL: float = 1.0    # 初期 level 分散（事前不確実性）
KF_INIT_VAR_TREND: float = 0.1    # 初期 trend 分散
KF_Q_LEVEL: float = 0.05          # level プロセスノイズ
KF_Q_TREND: float = 0.005         # trend プロセスノイズ
KF_R_OBS: float = 1.0             # 観測ノイズ
KF_TREND_DECAY: float = 0.9       # 成長率の平均回帰係数 ρ（<1）
KF_PERF_SCALE: float = 1.0        # 観測 y における着順正規スコアのスケール
KF_INTERVAL_REF_DAYS: float = 180.0  # 間隔→プロセスノイズ変調の基準日数（休養=不確実性増）
KF_WORKLOAD_HALFLIFE_DAYS: float = 60.0  # 疲労 workload の半減期（連戦指標の減衰）
KF_WINPROB_SCALE: float = 2.0     # Rating Lab の能力式勝率 softmax スケール

# 列順は compute_ability_kalman_history の出力配列と一致させること。
KF_FEATURE_COLS: list = [
    "kf_level",          # 出走前の予測能力（1 ステップ先予測）
    "kf_trend",          # 成長率（正=上昇期 / 負=下降期）
    "kf_level_vs_field",  # kf_level - レース内平均（相対能力）
    "kf_sigma",          # 能力推定の不確実性（状態標準偏差）
    "kf_workload",       # 疲労指標（直近の連戦度・減衰加重出走数）
]

# ──────────────────────────────────────────
# §2o: 階層ベイズ TrueSkill（市場オッズ事前分布・3段）— Phase 5
# ──────────────────────────────────────────

HB_TAU_MARKET: float = 4.0     # 市場事前のスキル標準偏差（小さいほど市場を強く信頼）
HB_TAU_GROUP: float = 6.0      # 群（種牡馬産駒）事前の標準偏差
HB_MARKET_SCALE: float = 3.0   # logit(implied 勝率) → μ スケールへの換算係数
HB_SIGMA_FLOOR: float = 0.5    # 個体精度 1/σ² の発散防止（ts_sigma の下限）

# 列順は compute_hier_bayes_history の出力配列と一致させること。
HB_FEATURE_COLS: list = [
    "hb_skill",       # 3段階層ベイズ事後平均（個体⊕市場⊕種牡馬群の精度加重）
    "hb_vs_market",   # ts_mu - 市場推定スキル（＝エッジ: 我々が市場より高評価する度合い）
    "hb_vs_field",    # hb_skill のレース内相対
    "hb_shrinkage",   # 事前（市場+群）への依存度 ∈[0,1]（コールドスタート指標）
]

# ──────────────────────────────────────────
# レーティング特徴量の集約（On/Off アブレーション用）
# ──────────────────────────────────────────

# 全レーティングファミリー（Phase 1-5 の全列）。
RATING_FEATURE_COLS: list = (
    ELO_FEATURE_COLS
    + TS_FEATURE_COLS
    + COND_TS_FEATURE_COLS
    + KF_FEATURE_COLS
    + HB_FEATURE_COLS
)

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
