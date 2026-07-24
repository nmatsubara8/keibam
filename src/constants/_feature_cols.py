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

# 多窓集計の対象列（DataMerger / ShutubaDataMerger 共通の単一の正）。
#
# 学習(run_pipeline)とライブ推論(ShutubaDataMerger)がこの同一定数を参照することで、
# 特徴量パリティ事故（学習列とライブ列の不一致）を構造的に防ぐ。従来は
# run_pipeline の builder 2 箇所・integration テスト・main.ipynb に別々のリテラルが
# 散在しており、run_pipeline は ["着順"] だが notebook は着順以外も含む等の
# 潜在的な不整合があった。ここに一元化する。
#
# 拡張時の注意（feature_expansion_plan.md 参照）:
#   - 列名は `_summarize` が {col}_{stat}_{n}R 形式で自動生成する
#   - Z-score は add_race_level_zscore が末尾一致で動的検出するため定数変更は不要
#   - 追加する列はすべて「過去走由来」であること（当該レース結果由来はリーク源）
#
# Phase 1（feature_expansion_plan.md）で着順のみ → タイム/上がり/展開/過去オッズへ拡張。
# ここに挙げた列はいずれも horse_results（馬の過去成績）由来なのでリークしない。
# 当該レースの結果列（results 側の 上り/タイム/人気/単勝）は集計に用いない。
AGG_TARGET_COLS: list = [
    "着順",           # 着順（既存）
    "time_seconds",   # 走破タイム（秒）— 速さの生値（コース標準化は Phase 3 speed_index）
    "上り",           # 上がり3F（終いの脚）
    "first_to_rank",  # 序盤コーナー位置 − 着順（前々での位置取り利得）
    "final_to_rank",  # 最終コーナー位置 − 着順（直線での伸び）
    "first_to_final", # 序盤 − 最終コーナー（道中の押し上げ/後退）
    "オッズ",          # 過去走の単勝オッズ（市場評価の履歴。過去走由来＝リークでない）
    "人気",            # 過去走の人気順
    "speed_index",    # Phase 3: コース基準タイムで標準化した速度指数（IDM 相当）
]

# horse_id と組で集計するグループ列（同上の単一の正）。
#
# 注意: 現状の ResultsProcessor は "騎手" 列を drop しているため、この
# ["騎手"] グループ集計は _merge_aggregates の `if group_col not in columns: continue`
# により実質無効（サイレントに skip）。有効化する場合は現騎手列を results に
# 復活させる必要があるが、列名を "騎手" にすると意図せず有効化して列数が
# 爆発するため、乗り替わり用途では別列名(jockey_name)を使う（Phase 2）。
AGG_GROUP_COLS: list = ["騎手"]

# ──────────────────────────────────────────
# Phase 1: 馬自身の通算成績集計
# ──────────────────────────────────────────

# 過去走（date < 当日）全体からの通算集計。着順 NaN 行は
# HorseResultsProcessor で drop 済みのため「完走ベース」の値。
HORSE_CAREER_FEATURE_COLS: list = [
    "n_career_starts",       # 通算出走回数（完走ベース）
    "career_win_rate",       # 勝率（着順==1）
    "career_quinella_rate",  # 連対率（着順<=2）
    "career_place_rate",     # 複勝率（着順<=3）
]

# ──────────────────────────────────────────
# Phase 2: ローテーション区分 / 乗り替わり
# ──────────────────────────────────────────

# interval（前走からの日数）の区間境界。right=True で (0,7]=連闘 … (56,inf]=休養明け。
# 初出走（interval NaN）は rotation_first_run フラグで別途表現する。
ROTATION_BINS: list = [0, 7, 14, 28, 56, float("inf")]
ROTATION_LABELS: list = ["rento", "naka1_2w", "naka3_4w", "naka5_8w", "kyuyo"]

# 乗り替わり系フラグ（DataMerger._add_jockey_change が生成。二値のため Z-score 対象外）
JOCKEY_CHANGE_FEATURE_COLS: list = [
    "jockey_change",  # 前走騎手と異なる=1
    "first_ride",     # その馬への騎乗歴なし（テン乗り）=1
]

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
# Phase 4: レース展開予測 / 種牡馬 距離適性
# ──────────────────────────────────────────

# 距離帯（course_len は 100m 単位バケット。≤13 短距離 / 14-17 マイル /
# 18-21 中距離 / ≥22 長距離）。pd.cut(right=True) の境界。
DIST_BAND_EDGES: list = [float("-inf"), 13, 17, 21, float("inf")]
DIST_BAND_LABELS: list = ["sprint", "mile", "mid", "long"]

# レース展開予測（レース内の横集計。レース内定数のため Z-score 対象外）
RACE_PACE_FEATURE_COLS: list = [
    "race_front_rate",   # レース内 逃/先（leg_type_binary==0）の割合
    "race_front_count",  # レース内 逃/先の頭数
    "race_pace_mean",    # レース内 pace_median の平均（小さいほどハイペース想定）
    "own_vs_race_pace",  # 自馬 pace_median − race_pace_mean（先行/差し優位の相対位置）
]

# 種牡馬 距離帯別適性（Z-score named 群へ合流）
SIRE_DISTANCE_FEATURE_COLS: list = [
    "sire_win_rate_distband",  # 同距離帯での種牡馬産駒勝率
    "sire_avg_rank_distband",  # 同距離帯での種牡馬産駒平均着順(相対)
    "sire_n_distband",         # 同距離帯でのサンプル数
]

# ──────────────────────────────────────────
# §2g: レース内 Z-score 対象列
# ──────────────────────────────────────────

# グループ1: 現レース特徴量（比較に意味がある数値列）
RACE_LEVEL_ZSCORE_COLS_G1: list = [
    "体重",               # 馬体重
    "体重変化",           # 体重変化
    "weight_change_rate",  # 体重変化率（Phase 2）
    "斤量",               # 騎手重量
    "単勝",               # 単勝オッズ
    "年齢",               # 年齢
    "interval",           # 前走からの経過日数
    "age_days",           # 日齢
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
