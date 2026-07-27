"""§2b〜§2j 特徴量エンジニアリングの定数。

マジックナンバーをコードに散らさず、変更をこのファイルに局所化する。

コース形状マスタ由来の特徴量列（course_*）は _course_master.COURSE_MASTER_FEATURE_COLS
に一元化。レース内定数のため Z-score 対象外で、交互作用のみ INTERACTION_FEATURE_COLS
で特徴量化する（constants は他 src を import しない規約のため再エクスポートしない）。
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

# ペアワイズ Elo レーティング（DataMerger._merge_horse_ratings）。馬の地力を着順履歴から
# 推定した as-of 値（出走前の現行レーティング）。リーク無し（preprocessing._ratings が日付昇順で
# 構築し、結果更新は次レース以降にのみ反映）。retrain --no-rating-features で学習から除外＝A/B 用。
ELO_FEATURE_COLS: list = [
    "elo_rating",      # 出走前の現行レーティング（初出走は 1500）
    "elo_n_races",     # それまでの出走数（レーティングの信頼度の代理）
    "elo_field_mean",  # 当該レース出走馬の平均レーティング（レース内一定）
    "elo_vs_field",    # elo_rating − elo_field_mean（フィールド内の相対地力）
    "elo_win_prob",    # Elo 強度比のレース内正規化勝率（スタンドアロン勝率）
]

# ══════════════════════════════════════════════════════════════════════════
# レーティング Phase 2-5（TrueSkill/条件別/Kalman/階層ベイズ）— 検証用（隔離）
# ------------------------------------------------------------------------
# これらは confident-hypatia ブランチ由来の Phase 2-5 レーティング拡張の定数。
# Phase 1（Elo）は main 統合済みで実データ A/B の結果「冗長（±0.001）」と判明済み。
# Phase 2-5 は production の core（data_merger/feature_engineering/retrain）には
# 未配線で、scripts/rating_ab_check.py（自己完結 A/B ハーネス）からのみ参照する。
# エッジ（Elo 冗長を超える改善）が確認できた場合に本配線を検討する。
# ══════════════════════════════════════════════════════════════════════════

# ── §2l: TrueSkill（多頭順位対応 μ/σ）— Phase 2 ──
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

# ── §2m: 条件別 TrueSkill（芝/ダ・距離・回り）— Phase 3 ──
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

# ── §2n: 能力 Kalman（局所線形トレンド・成長/疲労）— Phase 4 ──
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

# ── §2o: 階層ベイズ TrueSkill（市場オッズ事前分布・3段）— Phase 5 ──
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

# Phase 2-5 の全列（ELO は production 済み・A/B で冗長判明のため含めない）。A/B ハーネス用。
PHASE25_RATING_FEATURE_COLS: list = (
    TS_FEATURE_COLS + COND_TS_FEATURE_COLS + KF_FEATURE_COLS + HB_FEATURE_COLS
)

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

# オッズ由来の派生特徴量（retrain --no-odds-features で学習から除外＝対市場エッジ A/B 用）。
# 生の ``単勝`` は **含めない**: 元から _DROP_FOR_TRAIN で学習特徴から除外済みだが、EV 計算・
# オッズ供給（ExpectedValueScorePolicy.calc / ModelWrapper の X_test.drop(単勝)）が列の存在を
# 前提にするため featured には残す必要がある。市場情報は派生列（単勝_log・市場歪み overlay 群）
# とその _z を通じて入るので、それらだけを落として「市場の写し」でない r̂ を作る。
ODDS_DERIVED_FEATURE_COLS: list = (
    ["単勝_log", "単勝_log_z"]
    + MARKET_SIGNAL_FEATURE_COLS
    + [f"{c}_z" for c in MARKET_SIGNAL_FEATURE_COLS]
)

# 高カーディナリティの生 ID カテゴリ（retrain --no-id-features で学習から除外＝過学習 A/B 用）。
# jockey/trainer/owner/breeder の「汎化する」信号は TE・py 年度成績・各種集計で既に捕捉済み。
# 生 ID は GBDT が訓練データを丸暗記して汎化を下げる（feature_harm の drop 再学習で
# これらを落とすと単一 LGBM の test AUC が +0.011 改善するのを確認）。EV/集計には使わないので
# 学習入力から落として問題ない（featured には残す）。
HIGH_CARD_ID_FEATURE_COLS: list = ["jockey_id", "trainer_id", "owner_id", "breeder_id"]

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

# 直近 N レースの成績「率」（DataMerger._add_recent_form_stats）。N_RACES_LIST から動的生成。
# §2i の分布統計（着順_mean_NR 等）を補完する近走フォーム指標（勝率/連対率/複勝率/平均相対着順）。
RECENT_FORM_FEATURE_COLS: list = [
    f"{stem}_{n}R"
    for n in N_RACES_LIST
    for stem in ("win_rate", "rentai_rate", "place_rate", "avg_rel_rank")
]

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
    # コース形状マスタ由来の交互作用（_course_shape の course_* を使用）
    "legtype_x_straight",  # 脚質 × 直線長（差し馬×長い直線）
    "frame_x_width",       # 枠番 × 幅員（広いコースは外枠不利が緩む）
    "style_course_fit",    # 脚質 × コース脚質バイアス（出走馬×コース相性）
    # 距離別コースガイド由来の交互作用（_course_guide の guide_* を使用）
    "style_guide_fit",     # 脚質 × 距離別脚質バイアス（出走馬×コース×距離相性）
]
