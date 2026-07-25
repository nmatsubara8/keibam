"""フィールドカタログ — raw スキーマの「最大フィールド集合（superset）」定義。

方針（ユーザー指定）: 年代・ページ・データソースによって取得できない項目はあるが、
**テーブルの列自体は最大限を用意**し、取得できる項目だけ埋めて残りは欠損（NaN）にする。
これにより年代横断・ソース横断で前方互換なスキーマを保つ。

本モジュールは各論理テーブルの列を `FieldSpec` で列挙し、以下を機械的に取り出せるようにする:
- `columns(table)`            : そのテーブルの全列（superset）
- `feature_safe_columns()`    : 当該レースの特徴量に使ってよい列（リーク無し）
- `premium_columns()`         : プレミアム（要ログイン）列
- `name_fields()`             : 名前→ID 名寄せ対象（resolved_id を持つ列）

各 `FieldSpec`:
- `name`        列名（DataFrame 列名 / 既存は日本語列名を踏襲）
- `source`      取得元ページ種別
- `era_min`     取得可能になる最古年（None=全年代）。古い年代で欠損する列の明示
- `premium`     True=プレミアム（匿名スクレイプ不可、自前近似 or 契約）
- `leak_safe`   True=当該レースの特徴量に使える。False=事後情報/現在累計（リーク）
- `acquired`    True=現状取得済み。False=今回新規に器を用意する列
- `resolved_id` 名前列の場合の解決先 ID 列名（名寄せ対象）。無ければ None
- `note`        補足

レイヤ: constants（最下層・他レイヤ非依存）。
"""

from __future__ import annotations

import dataclasses
from typing import Optional


@dataclasses.dataclass(frozen=True)
class FieldSpec:
    name: str
    source: str
    era_min: Optional[int] = None
    premium: bool = False
    leak_safe: bool = True
    acquired: bool = True
    resolved_id: Optional[str] = None
    note: str = ""


# ページ種別（source）の識別子
SRC_RESULTS = "race_result"      # db.../race/<race_id>
SRC_RACE_INFO = "race_info"      # 同ページのヘッダ/別表
SRC_HORSE = "horse"              # db.../horse/<horse_id>
SRC_PED = "ped"                  # db.../horse/ped/<horse_id>
SRC_TRAINING = "training"        # race.../race/oikiri.html
SRC_PADDOCK = "paddock"          # race.../race/paddock.html
SRC_COMMENT = "comment"          # race.../race/comment.html（厩舎コメント）
SRC_PERSON = "person"            # {jockey,trainer,owner,breeder}/result.html
SRC_YOSO_MARK = "yoso_mark"      # race.../yoso/mark_list.html（印グリッド・JS描画）
SRC_YOSO_PROF = "yoso_profile"   # yoso.../no1/?pid=profile&yid=（予想家実績）
SRC_COURSE_MASTER = "course_master"  # jra.go.jp/facilities/race/<場>/course/（静的コース形状）


# ---------------------------------------------------------------------------
# raw_results（レース結果ページの着順表: 1 レース × 1 頭）
# ---------------------------------------------------------------------------
RESULTS_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_RESULTS, note="レースID（正準キー）"),
    FieldSpec("着順", SRC_RESULTS, leak_safe=False, note="目的変数の素（事後）"),
    FieldSpec("枠番", SRC_RESULTS),
    FieldSpec("馬番", SRC_RESULTS),
    FieldSpec("馬名", SRC_RESULTS, resolved_id="horse_id", note="表示用。結合は horse_id"),
    FieldSpec("性齢", SRC_RESULTS),
    FieldSpec("斤量", SRC_RESULTS),
    FieldSpec("騎手", SRC_RESULTS, resolved_id="jockey_id"),
    FieldSpec("タイム", SRC_RESULTS, leak_safe=False, note="当該レースの走破時計（事後）"),
    FieldSpec("着差", SRC_RESULTS, leak_safe=False),
    FieldSpec("通過", SRC_RESULTS, era_min=1990, leak_safe=False, acquired=False,
              note="当該レースのコーナー通過（事後）。過去走は horse_results で既得"),
    FieldSpec("上り", SRC_RESULTS, era_min=2000, leak_safe=False, acquired=False,
              note="当該レースの上り3F（事後）"),
    FieldSpec("単勝", SRC_RESULTS, note="確定単勝オッズ"),
    FieldSpec("人気", SRC_RESULTS, acquired=False, note="確定人気"),
    FieldSpec("馬体重", SRC_RESULTS, note="体重・増減（発表後は事前確定）"),
    FieldSpec("賞金", SRC_RESULTS, leak_safe=False, acquired=False, note="獲得賞金（事後）"),
    FieldSpec("タイム指数", SRC_RESULTS, premium=True, leak_safe=False, acquired=False,
              note="公式指数。自前 speed_figure で近似"),
    FieldSpec("調教タイム", SRC_RESULTS, premium=True, acquired=False,
              note="results 列のプレミアム。詳細は training ページ"),
    FieldSpec("厩舎コメント", SRC_RESULTS, premium=True, acquired=False),
    FieldSpec("備考", SRC_RESULTS, premium=True, leak_safe=False, acquired=False),
    FieldSpec("horse_id", SRC_RESULTS, note="馬ID"),
    FieldSpec("jockey_id", SRC_RESULTS, note="騎手ID（支配特徴 15.3%）"),
    FieldSpec("trainer_id", SRC_RESULTS, note="調教師ID（支配特徴 13.3%）"),
    FieldSpec("owner_id", SRC_RESULTS, note="馬主ID（支配特徴 40.9%）"),
)

# ---------------------------------------------------------------------------
# raw_race_info（レース条件: 1 レース）
# ---------------------------------------------------------------------------
RACE_INFO_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_RACE_INFO),
    FieldSpec("race_type", SRC_RACE_INFO, note="芝/ダート/障害"),
    FieldSpec("course_len", SRC_RACE_INFO, note="距離"),
    FieldSpec("weather", SRC_RACE_INFO),
    FieldSpec("ground_state", SRC_RACE_INFO, note="馬場状態"),
    FieldSpec("date", SRC_RACE_INFO),
    FieldSpec("around", SRC_RACE_INFO, note="回り（右/左）"),
    FieldSpec("race_class", SRC_RACE_INFO, note="クラス（旧称は LEGACY_ALIASES で正規化）"),
    FieldSpec("発走時刻", SRC_RACE_INFO, acquired=False, note="出馬表/odds に明示"),
    FieldSpec("グレード", SRC_RACE_INFO, acquired=False, note="G1/G2/G3/Jpn/L。レース名から抽出"),
    FieldSpec("本賞金", SRC_RACE_INFO, acquired=False, note="1〜5着配分。レース格の連続値"),
    FieldSpec("ラップタイム", SRC_RACE_INFO, era_min=2000, leak_safe=False, acquired=False,
              note="レース全体ラップ（事後・レース質ラベル）"),
    FieldSpec("ペース", SRC_RACE_INFO, era_min=2000, leak_safe=False, acquired=False),
    FieldSpec("コーナー通過順位", SRC_RACE_INFO, era_min=1990, leak_safe=False, acquired=False,
              note="隊列（事後）"),
    FieldSpec("馬場指数", SRC_RACE_INFO, premium=True, leak_safe=False, acquired=False),
    FieldSpec("トラックバイアス", SRC_RACE_INFO, premium=True, leak_safe=False, acquired=False,
              note="画像ベース・構造化困難"),
)

# ---------------------------------------------------------------------------
# raw_horse_results（馬の過去成績: 1 頭 × 過去 1 走）— 集計特徴の源。多くは過去走=安全
# ---------------------------------------------------------------------------
HORSE_RESULTS_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("horse_id", SRC_HORSE),
    FieldSpec("日付", SRC_HORSE),
    FieldSpec("開催", SRC_HORSE),
    FieldSpec("天気", SRC_HORSE),
    FieldSpec("R", SRC_HORSE),
    FieldSpec("レース名", SRC_HORSE, note="名寄せ: race_master で正準化/グレード抽出"),
    FieldSpec("頭数", SRC_HORSE),
    FieldSpec("枠番", SRC_HORSE),
    FieldSpec("馬番", SRC_HORSE),
    FieldSpec("オッズ", SRC_HORSE),
    FieldSpec("人気", SRC_HORSE),
    FieldSpec("着順", SRC_HORSE),
    FieldSpec("騎手", SRC_HORSE, resolved_id="jockey_id"),
    FieldSpec("斤量", SRC_HORSE),
    FieldSpec("距離", SRC_HORSE),
    FieldSpec("水分量", SRC_HORSE, era_min=2020, acquired=False,
              note="実DOMで確認した無料列（クッション値に類する馬場水分）"),
    FieldSpec("馬場", SRC_HORSE),
    FieldSpec("タイム", SRC_HORSE, note="→ time_seconds → speed_figure（自前）"),
    FieldSpec("着差", SRC_HORSE),
    FieldSpec("通過", SRC_HORSE, era_min=1990, note="→ first/final corner"),
    FieldSpec("ペース", SRC_HORSE, era_min=2000),
    FieldSpec("上り", SRC_HORSE, era_min=2000),
    FieldSpec("馬体重", SRC_HORSE),
    FieldSpec("賞金", SRC_HORSE),
    FieldSpec("備考", SRC_HORSE, acquired=False, note="未選択。脚質メモ等"),
    FieldSpec("勝ち馬", SRC_HORSE, acquired=False, resolved_id="勝ち馬_id",
              note="勝ち馬(2着馬)名。名寄せで ID 化→相手強度特徴"),
    FieldSpec("勝ち馬_id", SRC_HORSE, acquired=False, note="勝ち馬を horse_master で解決"),
    # 🔒 以下は実 DOM 確認でプレミアム（非ログインは空セル）→ 取得しない。器のみ
    FieldSpec("馬場指数", SRC_HORSE, premium=True, acquired=False),
    FieldSpec("タイム指数", SRC_HORSE, premium=True, acquired=False, note="公式指数。自前近似"),
    FieldSpec("スタート指数", SRC_HORSE, premium=True, acquired=False),
    FieldSpec("追走指数", SRC_HORSE, premium=True, acquired=False),
    FieldSpec("上がり指数", SRC_HORSE, premium=True, acquired=False),
    FieldSpec("厩舎コメント", SRC_HORSE, premium=True, acquired=False),
)

# ---------------------------------------------------------------------------
# raw_horse_info（馬プロフィール: 1 頭）— 静的属性は安全 / 現在累計はリーク
# ---------------------------------------------------------------------------
HORSE_INFO_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("horse_id", SRC_HORSE),
    FieldSpec("birthday", SRC_HORSE, note="→ agedays"),
    FieldSpec("owner_id", SRC_HORSE),
    FieldSpec("breeder_id", SRC_HORSE),
    FieldSpec("trainer_id", SRC_HORSE, acquired=False, note="現役厩舎（results にもあり）"),
    FieldSpec("産地", SRC_HORSE, acquired=False, note="静的"),
    FieldSpec("毛色", SRC_HORSE, acquired=False, note="静的・効果未知だが安価"),
    FieldSpec("馬区分", SRC_HORSE, acquired=False, note="抽選/市場/外国産 等の出自"),
    FieldSpec("セリ取引価格", SRC_HORSE, acquired=False, note="静的（空多い）"),
    FieldSpec("近親馬", SRC_HORSE, acquired=False, note="名寄せ対象（一族）"),
    FieldSpec("父", SRC_HORSE, acquired=False, resolved_id="父_id",
              note="血統ページ(23h)不要で取得。名寄せ対象"),
    FieldSpec("父_id", SRC_HORSE, acquired=False),
    FieldSpec("母", SRC_HORSE, acquired=False, resolved_id="母_id"),
    FieldSpec("母_id", SRC_HORSE, acquired=False),
    FieldSpec("母父", SRC_HORSE, acquired=False, resolved_id="母父_id",
              note="broodmare sire。重要属性。安価取得"),
    FieldSpec("母父_id", SRC_HORSE, acquired=False),
    FieldSpec("獲得賞金中央", SRC_HORSE, leak_safe=False, acquired=False,
              note="現在累計＝リーク。as-of 不可。live のみ"),
    FieldSpec("獲得賞金地方", SRC_HORSE, leak_safe=False, acquired=False),
    FieldSpec("通算成績", SRC_HORSE, leak_safe=False, acquired=False, note="現在累計＝リーク"),
    FieldSpec("主な勝鞍", SRC_HORSE, leak_safe=False, acquired=False),
)

# ---------------------------------------------------------------------------
# raw_peds（血統: 1 頭）— 今回取得スキップ（後でバックフィル）。器は用意する
# ---------------------------------------------------------------------------
PEDS_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("horse_id", SRC_PED, acquired=False),
    FieldSpec("peds_0", SRC_PED, acquired=False, note="父。sire 集計に使用"),
    FieldSpec("インブリード係数", SRC_PED, acquired=False, note="近交係数（例 SS 18.75% 3x4）"),
    FieldSpec("牝系キー", SRC_PED, acquired=False, note="ファミリーライン"),
)

# ---------------------------------------------------------------------------
# raw_training（調教/追い切り: 1 レース × 1 頭）【新規・高価値・リーク無し】
# ---------------------------------------------------------------------------
TRAINING_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_TRAINING, acquired=False),
    FieldSpec("馬番", SRC_TRAINING, acquired=False, note="results との結合キー"),
    FieldSpec("horse_id", SRC_TRAINING, acquired=False),
    FieldSpec("調教日", SRC_TRAINING, acquired=False),
    FieldSpec("コース", SRC_TRAINING, acquired=False, note="美浦W/栗東坂 等"),
    FieldSpec("馬場", SRC_TRAINING, acquired=False),
    FieldSpec("乗り役", SRC_TRAINING, acquired=False, note="主戦/助手"),
    FieldSpec("調教ラップ", SRC_TRAINING, premium=True, acquired=False,
              note="🔒 isFreemium（実DOM確認）。ラップ詳細はプレミアム→取得しない"),
    FieldSpec("終い", SRC_TRAINING, premium=True, acquired=False, note="🔒 ラスト1F（プレミアム）"),
    FieldSpec("位置", SRC_TRAINING, acquired=False, note="併走時の追走位置（無料）"),
    FieldSpec("脚色", SRC_TRAINING, acquired=False, note="馬也/一杯/強め（無料）"),
    FieldSpec("調教評価", SRC_TRAINING, acquired=False, note="叩き良化 等テキスト（無料・最重要）"),
    FieldSpec("映像グレード", SRC_TRAINING, acquired=False, note="A/B/C。順序特徴（無料）"),
    FieldSpec("併入相手", SRC_TRAINING, acquired=False, resolved_id="併入相手_id",
              note="名寄せ対象。相手の格で補正"),
    FieldSpec("併入相手_id", SRC_TRAINING, acquired=False),
)

# ---------------------------------------------------------------------------
# raw_paddock（パドック: 1 レース × 1 頭）【新規・注目馬のみ＝欠損多】
# ---------------------------------------------------------------------------
PADDOCK_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_PADDOCK, acquired=False),
    FieldSpec("馬番", SRC_PADDOCK, acquired=False, note="results との結合キー"),
    FieldSpec("horse_id", SRC_PADDOCK, acquired=False),
    FieldSpec("パドック評価", SRC_PADDOCK, acquired=False, note="A/B/穴。順序特徴"),
    FieldSpec("パドックコメント", SRC_PADDOCK, acquired=False, note="馬体・気配の寸評。raw保持(将来TF-IDF)"),
)

# ---------------------------------------------------------------------------
# raw_comment（厩舎コメント: レース × 馬）【新規・無料・リーク無し】
# ---------------------------------------------------------------------------
COMMENT_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_COMMENT, acquired=False),
    FieldSpec("馬番", SRC_COMMENT, acquired=False, note="results との結合キー"),
    FieldSpec("horse_id", SRC_COMMENT, acquired=False),
    FieldSpec("厩舎コメント", SRC_COMMENT, acquired=False,
              note="陣営コメント。raw保持(将来TF-IDF)。事前確定＝リーク無し"),
    FieldSpec("コメント評価", SRC_COMMENT, acquired=False, note="陣営の強気度マーク（あれば）"),
)

# ---------------------------------------------------------------------------
# raw_person_yearly（人物の年度別成績: 関係者 × 年）【新規・as-of でリーク回避】
# ---------------------------------------------------------------------------
PERSON_YEARLY_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("entity_type", SRC_PERSON, acquired=False, note="jockey/trainer/owner/breeder"),
    FieldSpec("entity_id", SRC_PERSON, acquired=False, note="数値ID（ハッシュID対応表で正準化）"),
    FieldSpec("year", SRC_PERSON, acquired=False),
    FieldSpec("出走回数", SRC_PERSON, acquired=False),
    FieldSpec("勝利数", SRC_PERSON, acquired=False),
    FieldSpec("勝率", SRC_PERSON, acquired=False, leak_safe=False,
              note="当該年は集計途中。as-of は『前年まで』で結合すること"),
    FieldSpec("連対率", SRC_PERSON, acquired=False, leak_safe=False),
    FieldSpec("複勝率", SRC_PERSON, acquired=False, leak_safe=False),
    FieldSpec("芝勝率", SRC_PERSON, acquired=False, leak_safe=False),
    FieldSpec("ダート勝率", SRC_PERSON, acquired=False, leak_safe=False),
    FieldSpec("重賞勝利", SRC_PERSON, acquired=False, leak_safe=False),
    FieldSpec("収得賞金", SRC_PERSON, acquired=False, leak_safe=False),
)


# ---------------------------------------------------------------------------
# raw_yoso_marks（予想印グリッド: レース × 馬 × 予想家）【新規・無料・JS描画で要API特定】
# ---------------------------------------------------------------------------
YOSO_MARKS_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("race_id", SRC_YOSO_MARK, acquired=False),
    FieldSpec("馬番", SRC_YOSO_MARK, acquired=False, note="results との結合キー"),
    FieldSpec("predictor_yid", SRC_YOSO_MARK, acquired=False,
              note="予想家ID。raw_yoso_predictor と結合しスキル加重"),
    FieldSpec("predictor_name", SRC_YOSO_MARK, acquired=False, resolved_id="predictor_yid",
              note="本紙/AI予想ビルダー 等。結合は predictor_yid"),
    FieldSpec("goods_kbn", SRC_YOSO_MARK, acquired=False,
              note="no1_free/no1_premium 等。由来(無料/プレミアム指定)を保持し後段で選別可"),
    FieldSpec("mark", SRC_YOSO_MARK, acquired=False, note="◎○▲△☆。内部API(無料+premium取得)"),
    FieldSpec("mark_score", SRC_YOSO_MARK, acquired=False, note="◎5○4▲3△2☆1 に数値化"),
)

# ---------------------------------------------------------------------------
# raw_yoso_predictor（予想家の成績ログ: 予想家 × 過去レース）【新規・無料・as-of でリーク回避】
# ---------------------------------------------------------------------------
YOSO_PREDICTOR_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("predictor_yid", SRC_YOSO_PROF, acquired=False, note="予想家ID（正準キー）"),
    FieldSpec("predictor_name", SRC_YOSO_PROF, acquired=False),
    FieldSpec("date", SRC_YOSO_PROF, acquired=False),
    FieldSpec("場名", SRC_YOSO_PROF, acquired=False),
    FieldSpec("レース番号", SRC_YOSO_PROF, acquired=False),
    FieldSpec("レース名", SRC_YOSO_PROF, acquired=False),
    FieldSpec("結果", SRC_YOSO_PROF, acquired=False, leak_safe=False,
              note="的中/不的中（当該行は事後）。スキルは as-of 集計で使う"),
    FieldSpec("的中配当", SRC_YOSO_PROF, acquired=False, leak_safe=False,
              note="例『３連単 23,640円』→ 回収率算出"),
    FieldSpec("◎馬", SRC_YOSO_PROF, acquired=False, note="その予想家の◎馬名"),
    FieldSpec("◎着順", SRC_YOSO_PROF, acquired=False, leak_safe=False, note="◎の成績（事後）"),
    FieldSpec("◎人気", SRC_YOSO_PROF, acquired=False, leak_safe=False),
    FieldSpec("週間回収率", SRC_YOSO_PROF, acquired=False, leak_safe=False,
              note="pro_yoso_rank。直近スナップショット→as-of 注意"),
)


# alias → フィールド定義
# ---------------------------------------------------------------------------
# course_master（JRA 公式コースページ由来の静的コース形状リファレンス）
# 開催×race_type で 1 行。scripts/scrape_course_master.py が JRA 10 場から取得。
# 幾何=物理シミュレーション環境パラメータ、プロファイル=馬×コース相性評価に用いる。
# ---------------------------------------------------------------------------
COURSE_MASTER_FIELDS: tuple[FieldSpec, ...] = (
    FieldSpec("place_code", SRC_COURSE_MASTER, note="開催コード2桁"),
    FieldSpec("race_type", SRC_COURSE_MASTER, note="芝/ダート"),
    FieldSpec("straight_length", SRC_COURSE_MASTER, note="ゴール前直線長[m]（Aコース）"),
    FieldSpec("elevation_diff", SRC_COURSE_MASTER, note="最大高低差[m]"),
    FieldSpec("lap_length", SRC_COURSE_MASTER, note="一周距離[m]（Aコース）"),
    FieldSpec("width_min", SRC_COURSE_MASTER, note="幅員下限[m]"),
    FieldSpec("width_max", SRC_COURSE_MASTER, note="幅員上限[m]"),
    FieldSpec("turn_direction", SRC_COURSE_MASTER, note="回り（0=右,1=左）"),
    FieldSpec("turf_type_code", SRC_COURSE_MASTER, note="芝種（0=野芝,1=洋芝）※芝のみ"),
    FieldSpec("corner_radius_large", SRC_COURSE_MASTER, note="コーナー半径が大きい=1"),
    FieldSpec("has_spiral_curve", SRC_COURSE_MASTER, note="スパイラルカーブ採用=1"),
    FieldSpec("run_style_bias", SRC_COURSE_MASTER, note="脚質バイアス（正=前有利/負=差し有利）"),
    FieldSpec("time_bias", SRC_COURSE_MASTER, note="時計傾向（-1=タフ,+1=高速）"),
    FieldSpec("drainage_good", SRC_COURSE_MASTER, note="水はけ良（重になりにくい）=1"),
)


CATALOG: dict[str, tuple[FieldSpec, ...]] = {
    "raw_results": RESULTS_FIELDS,
    "raw_race_info": RACE_INFO_FIELDS,
    "course_master": COURSE_MASTER_FIELDS,
    "raw_horse_results": HORSE_RESULTS_FIELDS,
    "raw_horse_info": HORSE_INFO_FIELDS,
    "raw_peds": PEDS_FIELDS,
    "raw_training": TRAINING_FIELDS,
    "raw_paddock": PADDOCK_FIELDS,
    "raw_comment": COMMENT_FIELDS,
    "raw_person_yearly": PERSON_YEARLY_FIELDS,
    "raw_yoso_marks": YOSO_MARKS_FIELDS,
    "raw_yoso_predictor": YOSO_PREDICTOR_FIELDS,
}


def columns(table: str) -> list[str]:
    """テーブルの全列（superset）を定義順で返す。"""
    return [f.name for f in CATALOG[table]]


def feature_safe_columns(table: str) -> list[str]:
    """当該レースの特徴量に使ってよい列（leak_safe=True）。"""
    return [f.name for f in CATALOG[table] if f.leak_safe]


def premium_columns(table: str) -> list[str]:
    """プレミアム（要ログイン）列。"""
    return [f.name for f in CATALOG[table] if f.premium]


def new_columns(table: str) -> list[str]:
    """現状未取得（今回新規に器を用意する）列。"""
    return [f.name for f in CATALOG[table] if not f.acquired]


def name_fields(table: str) -> list[tuple[str, str]]:
    """名寄せ対象 (名前列, 解決先ID列) の一覧。"""
    return [(f.name, f.resolved_id) for f in CATALOG[table] if f.resolved_id]


def all_tables() -> list[str]:
    return list(CATALOG)
