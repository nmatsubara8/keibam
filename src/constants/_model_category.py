"""分析モデルのカテゴリ定義（全国/地方 × 芝/ダート/障害）。

1 つのモデルで全レースを扱う代わりに、主催者区分（全国=JRA 中央 / 地方=NAR）と
馬場種別（芝/ダート/障害）の 2 軸でレースを 6 グループに分け、グループごとに
モデルを分割学習・保存・推論する。この 6 分割の「分類ロジック」だけをここに集約する。

分類の材料はいずれも既存データから導出できる:
- 主催者区分: race_id の 5〜6 桁目（`str(race_id)[4:6]`）が競馬場コード。
  中央（JRA）は 01〜10、それ以外（30 番台〜）は地方（NAR）。
- 馬場種別: race_info の `race_type` 列（正準値 "芝" / "ダート" / "障害"）。

constants レイヤの純粋モジュール（他の src レイヤに依存しない）。
race_id・race_type 値というプリミティブだけを受け取り、slug 文字列を返す。
"""

from __future__ import annotations

from ._master import Master

# ---------------------------------------------------------------------------
# 主催者区分（organizer）
# ---------------------------------------------------------------------------
ORG_CENTRAL = "central"  # 全国（JRA 中央）
ORG_LOCAL = "local"  # 地方（NAR）

# 中央（JRA）競馬場コード = 01〜10（Master.PLACE_DICT の札幌〜小倉）。
# これ以外の 2 桁コード（30 番台〜地方、60 番台〜海外）は「地方(NAR)」扱いとする。
CENTRAL_PLACE_CODES = frozenset(f"{i:02d}" for i in range(1, 11))

# ---------------------------------------------------------------------------
# 馬場種別（race_type slug）
# ---------------------------------------------------------------------------
RT_TURF = "turf"  # 芝
RT_DIRT = "dirt"  # ダート
RT_HURDLE = "hurdle"  # 障害

# 正準 race_type 値（Master 定義）→ slug
RACE_TYPE_TO_SLUG: dict[str, str] = {
    Master.RACE_TYPE_TURF: RT_TURF,
    Master.RACE_TYPE_DIRT: RT_DIRT,
    Master.RACE_TYPE_HURDLE: RT_HURDLE,
}

# ---------------------------------------------------------------------------
# カテゴリ slug（organizer_racetype）
# ---------------------------------------------------------------------------
# 6 分割 slug と表示ラベル。順序は決定的（学習・UI で安定させる）。
ALL_CATEGORIES: list[str] = [
    f"{ORG_CENTRAL}_{RT_TURF}",
    f"{ORG_CENTRAL}_{RT_DIRT}",
    f"{ORG_CENTRAL}_{RT_HURDLE}",
    f"{ORG_LOCAL}_{RT_TURF}",
    f"{ORG_LOCAL}_{RT_DIRT}",
    f"{ORG_LOCAL}_{RT_HURDLE}",
]

# 該当カテゴリのモデルが無い場合に使う統合（全レース）モデルの識別子。
COMBINED = "combined"

_ORG_LABEL = {ORG_CENTRAL: "全国", ORG_LOCAL: "地方"}
_RT_LABEL = {RT_TURF: "芝", RT_DIRT: "ダート", RT_HURDLE: "障害"}

CATEGORY_LABELS: dict[str, str] = {
    f"{org}_{rt}": f"{_ORG_LABEL[org]}・{_RT_LABEL[rt]}"
    for org in (ORG_CENTRAL, ORG_LOCAL)
    for rt in (RT_TURF, RT_DIRT, RT_HURDLE)
}
CATEGORY_LABELS[COMBINED] = "統合（全レース）"


def organizer_of_race_id(race_id) -> str:
    """race_id から主催者区分（central / local）を導出する。

    race_id は 12 桁（先頭 4 桁=年、5〜6 桁目=競馬場コード）。int/str いずれも受ける。
    コードが中央 01〜10 なら central、それ以外は local。桁不足など不正値は local 扱い。
    """
    s = str(race_id).strip()
    # 末尾の ".0"（float 由来）を除去
    if s.endswith(".0"):
        s = s[:-2]
    if len(s) < 6:
        return ORG_LOCAL
    code = s[4:6]
    return ORG_CENTRAL if code in CENTRAL_PLACE_CODES else ORG_LOCAL


# ライブ netkeiba のドメイン（主催者別）。開催カレンダー・レース一覧・出馬表・オッズ等の
# 「当日/予定」ページは主催者でドメインが分かれる。履歴 DB（db.netkeiba.com）は両者共通で
# NAR の race_id/horse_id も同じドメインで引けるため、DB 系は分岐不要（この定数は使わない）。
LIVE_NETKEIBA_DOMAIN: dict[str, str] = {
    ORG_CENTRAL: "race.netkeiba.com",  # 中央（JRA）
    ORG_LOCAL: "nar.netkeiba.com",  # 地方（NAR）
}


def live_netkeiba_base(organizer: str) -> str:
    """主催者区分（central/local）に対応するライブ netkeiba のベース URL（https://…）を返す。

    未知値は中央（JRA）にフォールバックする（既存挙動を変えない安全側）。
    """
    return f"https://{LIVE_NETKEIBA_DOMAIN.get(organizer, LIVE_NETKEIBA_DOMAIN[ORG_CENTRAL])}"


def live_netkeiba_base_for_race_id(race_id) -> str:
    """race_id の主催者区分に応じたライブ netkeiba のベース URL を返す（NAR なら nar.…）。"""
    return live_netkeiba_base(organizer_of_race_id(race_id))


def race_type_to_slug(race_type_value) -> str | None:
    """race_type の正準値（"芝"/"ダート"/"障害"）を slug に変換する。

    未知値・欠損（None/NaN）は None を返す（分類不能＝統合モデルへフォールバック）。
    """
    if race_type_value is None:
        return None
    # NaN 対策（float('nan') は自身と不等）
    if race_type_value != race_type_value:  # noqa: PLR0124
        return None
    return RACE_TYPE_TO_SLUG.get(str(race_type_value).strip())


def make_category(organizer: str, race_type_slug: str) -> str:
    """organizer と race_type slug からカテゴリ slug を合成する。"""
    return f"{organizer}_{race_type_slug}"


def categorize(race_id, race_type_value) -> str | None:
    """race_id と race_type 値からカテゴリ slug を導出する。

    race_type が未知/欠損なら None（分類不能）を返す。呼び出し側は None を
    統合モデルへのフォールバックとして扱う。
    """
    rt = race_type_to_slug(race_type_value)
    if rt is None:
        return None
    return make_category(organizer_of_race_id(race_id), rt)
