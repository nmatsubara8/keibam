import dataclasses
import re
import unicodedata
from typing import ClassVar
from typing import Optional


@dataclasses.dataclass(frozen=True)
class Master:
    # ------------------------------------------------------------------
    # 競馬場コード（netkeiba の place_id）
    # ------------------------------------------------------------------
    # PLACE_DICT: dict = MappingProxyType({
    PLACE_DICT: ClassVar[dict] = {
        "札幌": "01",
        "函館": "02",
        "福島": "03",
        "新潟": "04",
        "東京": "05",
        "中山": "06",
        "中京": "07",
        "京都": "08",
        "阪神": "09",
        "小倉": "10",
        "門別": "30",
        "旭川": "34",
        "盛岡": "35",
        "水沢": "36",
        "浦和": "42",
        "船橋": "43",
        "大井": "44",
        "川崎": "45",
        "金沢": "46",
        "笠松": "47",
        "名古屋": "48",
        "園田": "50",
        "姫路": "51",
        "福山": "53",
        "高知": "54",
        "佐賀": "55",
        "荒尾": "56",
        "札幌(地)": "58",
        "香港": "60",
        "フランス": "61",
        "オースト": "62",
        "イギリス": "63",
        "シャティ": "64",
        "アラブ首": "65",
        "メイダン": "66",
        "ドイツ": "67",
        "アイルラ": "68",
        "アメリカ": "69",
        "ロンシャ": "70",
        "イタリア": "71",
        "シンガポ": "72",
        "カナダ": "73",
        "シャンテ": "74",
        "韓国": "75",
        "フレミン": "76",
        "ニュージ": "77",
        "アスコッ": "78",
        "デルマー": "79",
        "サンタア": "80",
        "コーフィ": "81",
        "ベルモン": "82",
        "ドーヴィ": "83",
        "ランドウ": "84",
        "ヨーク": "85",
        "レパーズ": "86",
        "チャーチ": "87",
        "サンダウ": "88",
    }

    # ------------------------------------------------------------------
    # race_type 正準名（list/dict のインデックス・位置参照を避けるための名前付き定数）
    # ------------------------------------------------------------------
    RACE_TYPE_TURF: ClassVar[str] = "芝"
    RACE_TYPE_DIRT: ClassVar[str] = "ダート"
    RACE_TYPE_HURDLE: ClassVar[str] = "障害"

    # RACE_TYPE_DICT: dict = MappingProxyType({
    RACE_TYPE_DICT: ClassVar[dict] = {
        "芝": RACE_TYPE_TURF,
        "ダ": RACE_TYPE_DIRT,
        "障": RACE_TYPE_HURDLE,
    }

    # ------------------------------------------------------------------
    # 天候
    # ------------------------------------------------------------------
    WEATHER_LIST: tuple = ("晴", "曇", "小雨", "雨", "小雪", "雪")

    # ------------------------------------------------------------------
    # 馬場状態（位置参照の代わりに名前付き定数を使う）
    # ------------------------------------------------------------------
    GROUND_STATE_GOOD: ClassVar[str] = "良"
    GROUND_STATE_YAYA_OMO: ClassVar[str] = "稍重"
    GROUND_STATE_OMO: ClassVar[str] = "重"
    GROUND_STATE_BAD: ClassVar[str] = "不良"
    GROUND_STATE_LIST: tuple = (GROUND_STATE_GOOD, GROUND_STATE_YAYA_OMO, GROUND_STATE_OMO, GROUND_STATE_BAD)

    # ------------------------------------------------------------------
    # 性別
    # ------------------------------------------------------------------
    SEX_LIST: tuple = ("牡", "牝", "セ")

    # ------------------------------------------------------------------
    # コース回り方向
    # ------------------------------------------------------------------
    AROUND_RIGHT: ClassVar[str] = "右"
    AROUND_LEFT: ClassVar[str] = "左"
    AROUND_STRAIGHT: ClassVar[str] = "直線"
    AROUND_LIST: tuple = (AROUND_RIGHT, AROUND_LEFT, AROUND_STRAIGHT)

    # 競馬場コード(int) → コース回り方向（JRA 主要10場の芝の主方向）。回り適性
    # (around_rel_rank)の算出に使う。地方/海外・未定義コードは None＝方向不明で中立扱い。
    PLACE_AROUND: ClassVar[dict] = {
        1: AROUND_RIGHT,   # 札幌
        2: AROUND_RIGHT,   # 函館
        3: AROUND_RIGHT,   # 福島
        4: AROUND_LEFT,    # 新潟
        5: AROUND_LEFT,    # 東京
        6: AROUND_RIGHT,   # 中山
        7: AROUND_LEFT,    # 中京
        8: AROUND_RIGHT,   # 京都
        9: AROUND_RIGHT,   # 阪神
        10: AROUND_RIGHT,  # 小倉
    }

    # ------------------------------------------------------------------
    # レースクラス（位置参照の代わりに名前付き定数を使う）
    # ------------------------------------------------------------------
    RACE_CLASS_SHINBA: ClassVar[str] = "新馬"
    RACE_CLASS_MISHORI: ClassVar[str] = "未勝利"
    RACE_CLASS_1SHO: ClassVar[str] = "1勝クラス"
    RACE_CLASS_2SHO: ClassVar[str] = "2勝クラス"
    RACE_CLASS_3SHO: ClassVar[str] = "3勝クラス"
    RACE_CLASS_LISTED: ClassVar[str] = "リステッド"
    RACE_CLASS_OPEN: ClassVar[str] = "オープン"
    RACE_CLASS_OPEN_SPECIAL: ClassVar[str] = "オープン特別"
    RACE_CLASS_G3: ClassVar[str] = "G3"
    RACE_CLASS_G2: ClassVar[str] = "G2"
    RACE_CLASS_G1: ClassVar[str] = "G1"
    RACE_CLASS_LIST: tuple = (
        RACE_CLASS_SHINBA,
        RACE_CLASS_MISHORI,
        RACE_CLASS_1SHO,
        RACE_CLASS_2SHO,
        RACE_CLASS_3SHO,
        RACE_CLASS_LISTED,
        RACE_CLASS_OPEN,
        RACE_CLASS_OPEN_SPECIAL,
        RACE_CLASS_G3,
        RACE_CLASS_G2,
        RACE_CLASS_G1,
    )
    # 地方競馬(NAR)の格（A/B/C 級）。中央には無い体系で、race_info の「過去の◯○」から抽出する。
    # 低→高: C3 < C2 < C1 < B3 < B2 < B1 < A2 < A1（各級の下に組（十一 等）があるが級のみ採る）。
    RACE_CLASS_NAR_LIST: tuple = ("C3", "C2", "C1", "B3", "B2", "B1", "A2", "A1")

    # 2019年クラス改編前の旧称（〜2019年）— RACE_CLASS_LIST とは別管理
    RACE_CLASS_500: ClassVar[str] = "500万下"
    RACE_CLASS_1000: ClassVar[str] = "1000万下"
    RACE_CLASS_1600: ClassVar[str] = "1600万下"
    RACE_CLASS_LIST_LEGACY: tuple = ("500万下", "1000万下", "1600万下")

    # 2019 年のクラス名称変更以前（〜2018）の旧条件クラス → 現行クラスへの正規化。
    # 賞金条件ベースの旧名称: 500万下=1勝クラス、1000万下=2勝クラス、1600万下=3勝クラス。
    # 900万下は 1984〜1995 年頃の旧体系（2勝クラス相当）。
    RACE_CLASS_LEGACY_ALIASES: ClassVar[dict] = {
        "500万下": RACE_CLASS_1SHO,
        "900万下": RACE_CLASS_2SHO,
        "1000万下": RACE_CLASS_2SHO,
        "1600万下": RACE_CLASS_3SHO,
    }

    # ------------------------------------------------------------------
    # レース条件（タグ）と netkeiba 内部 ID
    # ------------------------------------------------------------------
    RACE_CONDITION_DICT: ClassVar[dict] = {
        "定量": "teiryo",
        "馬齢": "barei",
        "別定": "bettei",
        "国際": "kokusai",
        "指": "shitei",
        "特指": "tokushi",
        "特": "toku",
        "ハンデ": "handi",
        "見習騎手": "minarai",
        "若手騎手": "wakate",
        "障害": "shogai",
        "地": "chiho",
        "外": "gai",
        "混": "kongo",
        "牝": "hinba",
        "勝クラス": "kachi",
        "九州産馬": "kyushu",
    }

    # ── レース当日ノート（調教評価/映像グレード/パドック評価）の符号化定義 ──
    # 映像グレード A/B/C は明確な順序（A=良い）。順序エンコードで数値化する。
    VIDEO_GRADE_ORDINAL: ClassVar[dict] = {"A": 2, "B": 1, "C": 0}
    # パドック評価 A/B/穴 は「穴」が順序軸でない（穴馬＝妙味）ため one-hot 用リスト。
    PADDOCK_EVAL_LIST: tuple = ("A", "B", "穴")
    # 調教評価（追い切りの寸評）。ベストエフォートの順序スコア（高い＝好調）。
    # 既知の語のみ定義し、未知語は NaN（再取得で語彙を拡充して追記する）。
    TRAINING_EVAL_ORDINAL: ClassVar[dict] = {
        # 5: 絶好
        "絶好調": 5,
        "絶好": 5,
        # 4: 好調・上々・上積あり
        "好調キープ": 4,
        "好気配": 4,
        "気配上々": 4,
        "気配良好": 4,
        "元気一杯": 4,
        "上積十分": 4,
        "出来良好": 4,
        "仕上上々": 4,
        "仕上良好": 4,
        "デキ良好": 4,
        # 3: 順調・良化・安定
        "順調": 3,
        "叩き良化": 3,
        "上昇": 3,
        "仕上がる": 3,
        "出来安定": 3,
        "デキ安定": 3,
        "回復": 3,
        # 2: 現状維持・標準
        "平行線": 2,
        "現状維持": 2,
        "標準": 2,
        "変わらず": 2,
        "平年並み": 2,
        # 1: 物足りない・甘さ・太め
        "今ひとつ": 1,
        "物足りない": 1,
        "甘さ": 1,
        "平凡": 1,
        "案外": 1,
        "太め残り": 1,
        "余裕残し": 1,
        # 0: 不安・下降
        "不安": 0,
        "下降": 0,
        "イレ込み": 0,
        # ── 実取得で観測した netkeiba 標準語彙（2026-06 追記。被覆率改善）──
        # 5: 抜群・絶好
        "動き抜群": 5,
        "態勢万全": 5,
        "動き絶好": 5,
        "文句なし": 5,
        "迫力満点": 5,
        "気配抜群": 5,
        # 4: 好調・上々・十分・力強い
        "動き軽快": 4,
        "出来は良": 4,
        "キビキビ": 4,
        "素軽い": 4,
        "素軽さ出": 4,
        "好調持続": 4,
        "好調子": 4,
        "動き上々": 4,
        "伸び上々": 4,
        "反応上々": 4,
        "仕上十分": 4,
        "意欲十分": 4,
        "迫力十分": 4,
        "余力十分": 4,
        "乗込十分": 4,
        "状態良好": 4,
        "脚色良好": 4,
        "態勢整う": 4,
        "好気合": 4,
        "気合乗る": 4,
        "更に上昇": 4,
        "力強い": 4,
        "末脚良し": 4,
        "余力有り": 4,
        "前走以上": 4,
        "手応勝る": 4,
        # 3: 上昇・良化・整え・実戦向き
        "気配上昇": 3,
        "遅れ上々": 3,
        "実戦向き": 3,
        "一歩前進": 3,
        "動き良化": 3,
        "攻馬良化": 3,
        "追毎良化": 3,
        "やや良化": 3,
        "多少良化": 3,
        "体絞れる": 3,
        "攻め熱心": 3,
        "乗込入念": 3,
        "軽め順調": 3,
        "仕上り早": 3,
        "変身注": 3,
        # 2: 平凡・維持・調整程度
        "まずまず": 2,
        "前走並み": 2,
        "目立たず": 2,
        "動き平凡": 2,
        "気配平凡": 2,
        "反応平凡": 2,
        "伸び平凡": 2,
        "先着平凡": 2,
        "終い重点": 2,
        "急仕上げ": 2,
        "調整程度": 2,
        "乗込むも": 2,
        "仕上るも": 2,
        "テン飛す": 2,
        # 1: 一息・薄い・余力なし・うるさい
        "伸び一息": 1,
        "いま一息": 1,
        "良化遅い": 1,
        "良化薄い": 1,
        "上積無し": 1,
        "余力なし": 1,
        "ウルさい": 1,
    }




# ──────────────────────────────────────────────────────────────────────────
# レースクラス（格）の頑健な正規化 — 取りこぼし対策の単一情報源
# ──────────────────────────────────────────────────────────────────────────
#
# netkeiba のクラス表記は揺れが大きい:
#   - グレード: (G1) / (GⅠ) / (GⅢ) / Ｇ３ / (Jpn1) （全角ローマ数字・全角英数・括弧有無）
#   - リステッド: レース名に (L) / （Ｌ） が付く
#   - 条件戦: 1勝クラス / １勝クラス / 500万下 / 3歳未勝利 / 新馬 （全角数字・旧称）
# これらを NFKC 正規化 + 正規表現で 1 箇所に吸収し、Master.RACE_CLASS_* の正準値へ写像する。
# constants 層はフラット（モジュール間 import 禁止）のため、RACE_CLASS_* の定義元である
# 本モジュールに同居させる。スクレイプ時の現レース判定（preparing._raw_parsers）と過去走レース名
# からの格抽出（preprocessing の集計特徴量）が同じ規則を共有する。

# レースクラスの順序（格の大小）。条件戦〜G1 を 1..9 の連続軸で表す。
# Listed は OP より上・G3 より下（ブラックタイプの位置づけ）。
RACE_CLASS_LEVEL: dict = {
    Master.RACE_CLASS_SHINBA: 1,        # 新馬
    Master.RACE_CLASS_MISHORI: 1,       # 未勝利
    Master.RACE_CLASS_1SHO: 2,          # 1勝クラス（旧 500万下）
    Master.RACE_CLASS_2SHO: 3,          # 2勝クラス（旧 1000万下）
    Master.RACE_CLASS_3SHO: 4,          # 3勝クラス（旧 1600万下）
    Master.RACE_CLASS_OPEN: 5,          # オープン
    Master.RACE_CLASS_OPEN_SPECIAL: 5,  # オープン特別
    Master.RACE_CLASS_LISTED: 6,        # リステッド
    Master.RACE_CLASS_G3: 7,
    Master.RACE_CLASS_G2: 8,
    Master.RACE_CLASS_G1: 9,
    # 地方(NAR)の A/B/C 級。中央スケールと直接は比較不能だが、順序性のため 1〜6 に写像する
    # （NAR と中央は同一レースに混在しないため水準の重複は問題ない）。
    "C3": 1, "C2": 2, "C1": 3, "B3": 3, "B2": 4, "B1": 4, "A2": 5, "A1": 6,
}

# グレード検出（NFKC 後 = 全角→半角・ローマ数字 Ⅲ→"III" 化済みの文字列に対して）。
# G/Jpn の直後に III/II/I/3/2/1。前後の英数で誤検出しないよう境界を要求する。
_RACE_GRADE_TOKEN_RE = re.compile(
    r"(?<![A-Z])(JPN|G)\s*(III|II|I|3|2|1)(?![A-Z0-9])",
    re.IGNORECASE,
)
# Listed: 括弧付き L（レース名の "(L)" / "（Ｌ）"）。単独 "L" の誤検出を避け括弧必須。
_RACE_LISTED_RE = re.compile(r"[(（]\s*L\s*[)）]", re.IGNORECASE)

# グレード数字（III/II/I/3/2/1）→ 正準クラス。
_RACE_GRADE_TO_CLASS = {
    "III": Master.RACE_CLASS_G3, "3": Master.RACE_CLASS_G3,
    "II": Master.RACE_CLASS_G2, "2": Master.RACE_CLASS_G2,
    "I": Master.RACE_CLASS_G1, "1": Master.RACE_CLASS_G1,
}

# 条件戦・OP のテキスト判定（NFKC 後・新称/旧称を網羅）。上から優先。
_RACE_CONDITION_RULES: list = [
    ("リステッド", Master.RACE_CLASS_LISTED),  # (L) 以外に文字列「リステッド」で書かれる場合
    ("オープン特別", Master.RACE_CLASS_OPEN_SPECIAL),
    ("オープン", Master.RACE_CLASS_OPEN),
    ("3勝クラス", Master.RACE_CLASS_3SHO),
    ("1600万下", Master.RACE_CLASS_3SHO),
    ("2勝クラス", Master.RACE_CLASS_2SHO),
    ("1000万下", Master.RACE_CLASS_2SHO),
    ("900万下", Master.RACE_CLASS_2SHO),
    ("1勝クラス", Master.RACE_CLASS_1SHO),
    ("500万下", Master.RACE_CLASS_1SHO),
    ("未勝利", Master.RACE_CLASS_MISHORI),
    ("新馬", Master.RACE_CLASS_SHINBA),
    ("メイクデビュー", Master.RACE_CLASS_SHINBA),  # JRA の新馬戦ブランド名（=新馬）
    # 地方(NAR)の A/B/C 級（race_info「過去の◯○」由来の格テキストに現れる。例 "C3十 11"→C3）。
    # 中央テキストにこれらの 2 文字トークンは通常出ないので誤検出リスクは低い。上位級から順に判定。
    ("A1", "A1"), ("A2", "A2"),
    ("B1", "B1"), ("B2", "B2"), ("B3", "B3"),
    ("C1", "C1"), ("C2", "C2"), ("C3", "C3"),
]


def _normalize_race_text(text) -> str:
    """NFKC 正規化 + 連続空白の単一化。非文字列・欠損は空文字。"""
    if text is None:
        return ""
    if isinstance(text, float) and text != text:  # NaN
        return ""
    s = unicodedata.normalize("NFKC", str(text))
    return re.sub(r"\s+", " ", s).strip()


def classify_race_class(text) -> Optional[str]:
    """レース名・条件テキストから正準クラス（Master.RACE_CLASS_*）を判定する。

    優先順位: グレード(G1/G2/G3) > リステッド(L) > 条件戦/オープン。
    判定できなければ None。全角・ローマ数字(GⅢ)・括弧の有無・旧称(500万下)を吸収する。
    """
    s = _normalize_race_text(text)
    if not s:
        return None
    su = s.upper()

    m = _RACE_GRADE_TOKEN_RE.search(su)  # 1) グレード（最優先）。"GⅢ"→NFKC→"GIII"
    if m:
        return _RACE_GRADE_TO_CLASS[m.group(2).upper()]

    if _RACE_LISTED_RE.search(su):  # 2) リステッド（括弧付き L）
        return Master.RACE_CLASS_LISTED

    for keyword, cls in _RACE_CONDITION_RULES:  # 3) 条件戦 / オープン
        if keyword in s:
            return cls
    return None


def race_class_level(race_class) -> Optional[int]:
    """正準クラス文字列、または生のレース名/条件テキストを順序値（1..9）に写像する。不明は None。"""
    if race_class is None:
        return None
    if isinstance(race_class, float) and race_class != race_class:  # NaN
        return None
    level = RACE_CLASS_LEVEL.get(str(race_class).strip())  # まず正準値として直接引く
    if level is not None:
        return level
    canonical = classify_race_class(race_class)  # 旧称・グレード表記は分類してから引く
    if canonical is None:
        return None
    return RACE_CLASS_LEVEL.get(canonical)
