"""Phase 9-rev: コース形状マスタ（JRA 公式コースページ由来）の定数。

競馬場×コース種別ごとの物理特性（表由来の幾何）と、コース紹介プロセ由来の
定性プロファイル（脚質バイアス・馬場傾向・芝種など）を (開催, race_type) キーで持つ。
値は scripts/scrape_course_master.py が JRA 10 場を巡回して自動生成する（手入力しない）。

用途:
- 物理シミュレーションの環境パラメータ（周長/直線/幅員/高低差/回り/曲率）
- 出走馬×コースの相性評価（脚質バイアス/馬場スピード傾向/芝種適性）
"""

# 主キー列（開催=place コード 2桁 str, race_type=芝/ダート/障）。
# 幾何は本来コース区分(A/B/C)依存だが、区分差は僅少のため A コース代表値に集約する。
COURSE_MASTER_KEY_COLS: list = ["place_code", "race_type"]

# 幾何（「コースデータ」表から抽出。数値）
COURSE_MASTER_GEOMETRY_COLS: list = [
    "straight_length",  # ゴール前直線長 [m]（A コース）
    "elevation_diff",   # 最大高低差 [m]
    "lap_length",       # 一周距離 [m]（A コース）
    "width_min",        # 幅員の下限 [m]
    "width_max",        # 幅員の上限 [m]
    "turn_direction",   # 回り（0=右, 1=左）
]

# 定性プロファイル（「コース紹介」プロセから抽出。カテゴリ/序数。欠損は NaN）
COURSE_MASTER_PROFILE_COLS: list = [
    "turf_type_code",       # 芝種（0=野芝, 1=洋芝）※芝のみ
    "corner_radius_large",  # コーナー半径が大きい（緩い）=1 / 急=0
    "has_spiral_curve",     # スパイラルカーブ採用=1
    "run_style_bias",       # 脚質バイアス（正=前有利 / 負=差し追込有利）
    "time_bias",            # 時計傾向（-1=時計を要す/タフ, 0, +1=高速）
    "drainage_good",        # 水はけ良（重になりにくい）=1
]

COURSE_MASTER_VALUE_COLS: list = COURSE_MASTER_GEOMETRY_COLS + COURSE_MASTER_PROFILE_COLS
COURSE_MASTER_COLS: list = COURSE_MASTER_KEY_COLS + COURSE_MASTER_VALUE_COLS

# マージ後に results へ付与される特徴量列（course_ プレフィックス）。
# レース内定数のため Z-score 対象外。交互作用は _interaction_features で生成。
COURSE_MASTER_FEATURE_COLS: list = [f"course_{c}" for c in COURSE_MASTER_VALUE_COLS]
