"""JRDB TARGET ダウンロードデータ（外厩/展開/番手/IDM 馬印・外厩コメント・成績IDM・各ランク）の
パーサ（STEP5）。

実ファイル解析（docs/jrdb_target_downloads.md）で確定した 8 種を、featured へ結合可能な
正規化 DataFrame にする。標準 JRDB（KYI/SED…）とは別系統だが、レースキー/血統登録番号の
変換は `_keys` を流用して race_id / horse_id を揃える。

種別（zip 名の先頭英字で分類）:
  gaikyu        外厩馬印    固定長10B  [ketto8][外厩コード2B]        レース単位ファイル・キー=ketto
  itidori       展開馬印    固定長10B  [ketto8][位置コード2B]        （ゴール前内外位置）
  bante         番手馬印    固定長9B   [ketto8][番手コード1B]        （ゴール前内外番手）
  idm           IDM馬印     固定長10B  [ketto8][印2B]                上位6頭
  gaikyucomment 外厩コメント CSV        key10,外厩名␣帰厩日␣中N週     キー=(race,馬番)
  idmse         成績IDM     CSV        key18,成績IDM(符号付int)      レース後到達IDM
  tnrank        厩舎ランク  CSV(utf-8) 区分,調教師コード5,ランク,名
  jocrank       騎手ランク  CSV(utf-8) 区分,騎手コード4,ランク,名
"""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from src.jrdb._extract import read_jrdb_bytes
from src.jrdb._keys import ketto_to_horse_id, race_key_to_race_id

_ZEN_SPACE = "　"  # 全角スペース（コメントの区切り）

# 馬印系（固定長 [ketto][コード]）の仕様。reclen=総バイト長、code_col=出力列名。
MARK_SPECS = {
    "gaikyu": {"reclen": 10, "code_col": "gaikyu_code"},
    "itidori": {"reclen": 10, "code_col": "itidori_code"},
    "bante": {"reclen": 9, "code_col": "bante_code"},
    "idm": {"reclen": 10, "code_col": "idm_mark"},
}
# zip 名先頭英字 → 種別。gaikyucomment/idmse を gaikyu/idm より優先判定するため厳密一致で引く。
TARGET_TYPES = (
    "gaikyucomment", "gaikyu", "itidori", "bante", "idmse", "idm", "tnrank", "jocrank",
)

# 種別ごとの自然キー（重複除去の単位）。同一キーの重複は再DLコピーや年次×日次の重なり由来。
# ランクは日次スナップショット＝時系列。source_date を含めて「日付×人」で重複除去する
# （person_code だけで潰すと最新1日に collapse し、レース時点ランクの asof 結合ができなくなる）。
NATURAL_KEYS = {
    "gaikyu": ["race_id", "ketto"], "itidori": ["race_id", "ketto"],
    "bante": ["race_id", "ketto"], "idm": ["race_id", "ketto"],
    "gaikyucomment": ["race_id", "umaban"], "idmse": ["race_id", "umaban"],
    "tnrank": ["source_date", "person_code"], "jocrank": ["source_date", "person_code"],
}


def date_from_name(name: str) -> str | None:
    """ファイル名から YYYYMMDD を抽出（ランクの日次スナップショット日付・zip 名にのみ在る）。"""
    m = re.search(r"(20\d{6})", Path(str(name)).name)
    return m.group(1) if m else None


def dedup_by_keys(df: pd.DataFrame, keys: list[str]) -> tuple[pd.DataFrame, int]:
    """自然キーで重複除去し (dedup後df, 除去件数) を返す。keep='last'＝後勝ち（新しいDL/年）。

    キー列が欠けている/空 df は素通し。入力は日/年ファイル順（=時系列順）を前提とし、
    ランク等の更新系は最後の行を残す。
    """
    key_cols = [k for k in keys if k in df.columns]
    if df.empty or not key_cols:
        return df, 0
    before = len(df)
    out = df.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)
    return out, before - len(out)


def classify(zip_name: str) -> str | None:
    """zip/ファイル名の先頭英字から種別を返す（未知は None）。"""
    m = re.match(r"[a-zA-Z]+", Path(str(zip_name)).name)
    if not m:
        return None
    head = m.group(0).lower()
    return head if head in TARGET_TYPES else None


def _splitlines(data: bytes, encoding: str) -> list[str]:
    text = data.decode(encoding, errors="replace")
    return [ln for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n") if ln]


def parse_mark_file(internal_name: str, data: bytes, mark: str) -> pd.DataFrame:
    """馬印系（固定長 [ketto8][コード]）1 ファイルを [race_id, ketto, horse_id, <code>] へ。

    race_id は内包ファイル名 `場年回日R種別.DAT` の先頭 8=レースキーから復元する。
    """
    spec = MARK_SPECS[mark]
    reclen, code_col = spec["reclen"], spec["code_col"]
    stem = Path(internal_name).stem
    race_id = race_key_to_race_id(stem[:8]) if len(stem) >= 8 else None
    rows = []
    for rec in _split_fixed(data, reclen):
        ketto = rec[:8].decode("ascii", errors="replace").strip()
        code = rec[8:reclen].decode("cp932", errors="replace").strip()
        if not ketto:
            continue
        rows.append({
            "race_id": race_id, "ketto": ketto,
            "horse_id": ketto_to_horse_id(ketto), code_col: code,
        })
    return pd.DataFrame(rows, columns=["race_id", "ketto", "horse_id", code_col])


def _split_fixed(data: bytes, reclen: int) -> list[bytes]:
    """CRLF/LF 区切りを剥がし、reclen 長のレコードだけ返す（改行混在に頑健）。"""
    body = data.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
    return [r for r in body.split(b"\n") if len(r) == reclen]


def parse_gaikyu_comment(data: bytes) -> pd.DataFrame:
    """外厩コメント CSV を [race_id, umaban, gaikyu_name, kikyu_date, interval_weeks] へ。

    行 = `場年回日R馬番(10),外厩名␣帰厩日␣中N週`。キー先頭 8=レースキー、[8:10]=馬番。
    """
    rows = []
    for ln in _splitlines(data, "cp932"):
        key, _, rest = ln.partition(",")
        key = key.strip()
        if len(key) < 10:
            continue
        race_id = race_key_to_race_id(key[:8])
        umaban = _to_int(key[8:10])
        name, kikyu, interval = _split_comment(rest)
        rows.append({
            "race_id": race_id, "umaban": umaban, "gaikyu_name": name,
            "kikyu_date": kikyu, "interval_weeks": _weeks(interval), "interval_raw": interval,
        })
    return pd.DataFrame(
        rows,
        columns=["race_id", "umaban", "gaikyu_name", "kikyu_date", "interval_weeks", "interval_raw"],
    )


def _split_comment(rest: str) -> tuple[str, str, str]:
    """`外厩名␣帰厩日␣間隔` を分解（全角空白区切り・欠落に頑健）。"""
    parts = [p for p in rest.split(_ZEN_SPACE) if p != ""]
    name = parts[0].strip() if parts else ""
    kikyu = parts[1].strip() if len(parts) > 1 else ""
    interval = parts[2].strip() if len(parts) > 2 else ""
    return name, kikyu, interval


def parse_seiseki_idm(data: bytes) -> pd.DataFrame:
    """成績IDM CSV を [race_id, umaban, race_date, seiseki_idm] へ。

    行 = `YYYYMMDD場回日R馬番(18),成績IDM(符号付int)`。空値は NaN。
    """
    rows = []
    for ln in _splitlines(data, "cp932"):
        key, _, val = ln.partition(",")
        key = key.strip()
        if len(key) < 18:
            continue
        race_date = key[0:8]
        race_id = f"{key[0:4]}{key[8:10]}{key[10:12]}{key[12:14]}{key[14:16]}"  # 年+場+回+日+R
        rows.append({
            "race_id": race_id, "umaban": _to_int(key[16:18]),
            "race_date": race_date, "seiseki_idm": _to_int(val),
        })
    return pd.DataFrame(rows, columns=["race_id", "umaban", "race_date", "seiseki_idm"])


def parse_rank(data: bytes, kind: str, source_date: str | None = None) -> pd.DataFrame:
    """厩舎/騎手ランク CSV(utf-8) を [source_date, area, person_code, rank, name, kind] へ。

    行 = `区分,コード,ランク,名前`（tnrank=調教師5桁, jocrank=騎手4桁）。
    source_date（zip 名の YYYYMMDD）は日次スナップショットの日付＝時系列保持に必須。
    """
    rows = []
    for ln in _splitlines(data, "utf-8"):
        parts = ln.split(",")
        if len(parts) < 4:
            continue
        rows.append({
            "source_date": source_date, "area": _to_int(parts[0]),
            "person_code": parts[1].strip(), "rank": _to_int(parts[2]),
            "name": parts[3].strip(), "kind": kind,
        })
    return pd.DataFrame(
        rows, columns=["source_date", "area", "person_code", "rank", "name", "kind"])


def _to_int(s):
    try:
        return int(str(s).strip())
    except (ValueError, TypeError):
        return pd.NA


def _weeks(interval: str):
    """`中4週`→4、`連闘`→0、それ以外/欠落→NA。ローテ間隔の数値化。"""
    if not interval:
        return pd.NA
    if "連闘" in interval:
        return 0
    m = re.search(r"(\d+)", interval)
    return int(m.group(1)) if m else pd.NA


def parse_target_bytes(zip_type: str, entries: list[tuple]) -> pd.DataFrame:
    """1 種別のエントリ群を結合した正規化 DataFrame にする。

    entries は (source_name, internal_name, data) の 3-tuple。後方互換で (internal_name, data)
    の 2-tuple も受ける（その場合 source_name=internal_name とみなす）。
    馬印系は internal_name（レースキー）、ランクは source_name（日付）を使う。
    """
    frames = []
    norm = [(e if len(e) == 3 else (e[0], e[0], e[1])) for e in entries]
    if zip_type in MARK_SPECS:
        frames = [parse_mark_file(name, data, zip_type) for _, name, data in norm]
    elif zip_type == "gaikyucomment":
        frames = [parse_gaikyu_comment(data) for _, _, data in norm]
    elif zip_type == "idmse":
        frames = [parse_seiseki_idm(data) for _, _, data in norm]
    elif zip_type in ("tnrank", "jocrank"):
        frames = [parse_rank(data, zip_type, date_from_name(src)) for src, _, data in norm]
    else:
        raise ValueError(f"未知の TARGET 種別: {zip_type}")
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def parse_target_archive(path: str) -> tuple[str | None, pd.DataFrame]:
    """1 つの zip/lzh/txt を種別判定し正規化 DataFrame を返す（(種別, df)）。"""
    zip_type = classify(path)
    if zip_type is None:
        return None, pd.DataFrame()
    entries = [(Path(path).name, name, data) for name, data in read_jrdb_bytes(path)]
    return zip_type, parse_target_bytes(zip_type, entries)
