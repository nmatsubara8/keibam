import dataclasses
from typing import ClassVar


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
    # 2019年クラス改編前の旧称（〜2019年）
    RACE_CLASS_500: ClassVar[str] = "500万下"
    RACE_CLASS_1000: ClassVar[str] = "1000万下"
    RACE_CLASS_1600: ClassVar[str] = "1600万下"
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
        # 旧称（2019年以前）
        RACE_CLASS_500,
        RACE_CLASS_1000,
        RACE_CLASS_1600,
    )

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
