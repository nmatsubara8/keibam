"""距離別コースガイド（書籍/ガイド由来プロセ）の定数。

course_master（JRA 公式ページ由来・(開催, race_type) キーの静的コース形状）とは別に、
書籍・コースガイドの「コース紹介」文を **距離固有** に持つレイヤ。
主キーは (place_code, race_type, course_len_m) で、同一コースでも距離ごとに
脚質/ペース/波乱傾向が変わる記述を取り込む（例: 東京芝1400m 固有の「二度の上り坂」「馬券が荒れやすい」）。

手入力ソース: data/master/course_guide.csv（place_code, race_type, course_len_m, prose_guide）。
scripts/scrape_course_master.py が prose_guide を要点抽出して course_guide_master.csv を生成する。
src 側は生成物 CSV を読むだけ（プロセ解析は行わない＝constants 純粋性・レイヤ規約を保つ）。

用途: 出走馬×コース×距離の相性評価、EV ベッティングの波乱度補正。
"""

# 主キー列（開催=place コード2桁 str, race_type=芝/ダート/障, course_len_m=距離[m] int）
COURSE_GUIDE_KEY_COLS: list = ["place_code", "race_type", "course_len_m"]

# 手入力ソース列（要点抽出前の素テキスト）
COURSE_GUIDE_SOURCE_COL: str = "prose_guide"

# ガイド文から要点抽出する定性プロファイル（距離固有・欠損は NaN）
# course_master の profile と重なる列は guide_ プレフィックスで別特徴量として共存する。
COURSE_GUIDE_VALUE_COLS: list = [
    "run_style_bias",       # 脚質バイアス（正=前有利 / 負=差し追込有利）※当該距離
    "time_bias",            # 時計傾向（-1=タフ, 0, +1=高速）※当該距離
    "corner_radius_large",  # コーナー半径が大きい（緩い）=1 / 急=0
    "drainage_good",        # 水はけ良（重になりにくい）=1
    "upset_prone",          # 波乱度（1=荒れやすい / 0=堅い）※ガイド文固有
]

COURSE_GUIDE_MASTER_COLS: list = COURSE_GUIDE_KEY_COLS + COURSE_GUIDE_VALUE_COLS

# マージ後に results へ付与される特徴量列（guide_ プレフィックス）。
# レース内定数のため Z-score 対象外。交互作用は _interaction_features で生成。
COURSE_GUIDE_FEATURE_COLS: list = [f"guide_{c}" for c in COURSE_GUIDE_VALUE_COLS]
