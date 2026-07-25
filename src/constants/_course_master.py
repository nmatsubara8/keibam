"""Phase 9: コース形状マスタ（直線長・高低差・坂・1コーナーまで距離）の定数。

競馬場×コース種別×距離ごとの物理特性を (開催, race_type, course_len) キーで持つ。
値は JRA 公式のコースデータ（事実）を scripts/scrape_course_master.py で取得して
data/master/course_master.csv に保存する（手入力しない）。ライブ/学習とも同じ CSV を読む。
"""

# 主キー列（開催=place コード 2桁 str, race_type=芝/ダート/障, course_len=100m バケット int）
COURSE_MASTER_KEY_COLS: list = ["place_code", "race_type", "course_len"]

# 数値属性列（欠損は NaN → LightGBM に委ねる）
COURSE_MASTER_VALUE_COLS: list = [
    "straight_length",     # ゴール前直線長 [m]
    "elevation_diff",      # 最大高低差 [m]
    "has_final_hill",      # ゴール前に上り坂があるか (0/1)
    "first_corner_dist",   # スタート〜1 コーナーまでの距離 [m]
]

COURSE_MASTER_COLS: list = COURSE_MASTER_KEY_COLS + COURSE_MASTER_VALUE_COLS

# マージ後に results へ付与される特徴量列（course_ プレフィックス）。
# レース内定数（直線長等）のため Z-score 対象外。交互作用は _interaction_features で生成。
COURSE_MASTER_FEATURE_COLS: list = [f"course_{c}" for c in COURSE_MASTER_VALUE_COLS]
