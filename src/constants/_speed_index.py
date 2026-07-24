"""Phase 3: スピード指数（IDM 相当）の定数。

feature_expansion_plan.md 参照。マジックナンバーをコードに散らさず局所化する。
"""

# 基準タイム表のセルに必要な最小サンプル数（未満は粗いキーへフォールバック）
BASE_TIME_MIN_COUNT: int = 30

# 基準タイム表のグルーピングキー（細 → 粗）。
# 細: 競馬場×コース種別×距離バケット×馬場、粗: コース種別×距離バケット。
BASE_TIME_KEYS_FINE: list = ["開催", "race_type", "course_len", "馬場"]
BASE_TIME_KEYS_COARSE: list = ["race_type", "course_len"]

# スピード指数の基準値・スケール（速い＝タイム小 ほど高い値になる）。
#   speed_index = BASE + SCALE * (base_mean - time) / base_std
SPEED_INDEX_BASE: float = 50.0
SPEED_INDEX_SCALE: float = 10.0

# 基準タイムの train/test 境界に用いる既定 test_size（DataSplitter と揃える）。
SPEED_INDEX_TEST_SIZE: float = 0.2
