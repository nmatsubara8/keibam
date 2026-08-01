"""特徴量の充足監査＝featured のどの特徴ファミリが「未取得（全レース定数）」かを機械検出し根因を絞る。

disagreement 解析で `文脈列が全レースで単一値＝特徴未取得の疑い` として大量除外された
owner_py_* / sire_* / damsire_* / 開催__* 等が「効果なし」でなく「値が入っていない」ことを、
featured 実データ（gitignore の巨大成果物・ローカルにのみ在る）に対して確定させる。

出力:
  ① 特徴ファミリ別の充足（列数・非欠測%・非ゼロ%・unique中央値・DEAD 判定）。
  ② 生ソースの突合（best-effort）:
     - raw_person_yearly の entity_type 別 行数（owner 行が 0 なら owner_py 未取得の直接証拠）。
     - raw_results / raw_horse_info に owner_id / breeder_id / peds_0 / peds_32 列が在るか
       （backfill が id を引けているか＝owner_id が raw_results に無いと owner の scrape が skip される）。

使い方:
  python scripts/audit_feature_coverage.py                    # 既定 featured
  python scripts/audit_feature_coverage.py --featured path.pkl
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# 監査対象の特徴ファミリ（prefix）。disagreement 解析で dead だった族＋比較用の生きている族。
_FAMILIES = [
    "owner_py_", "breeder_py_", "jockey_py_", "trainer_py_",
    "sire_", "damsire_", "開催__", "guide_",
    "owner_win", "owner_avg", "yoso_", "wet_",
]


def profile_columns(df):
    """各列の (非欠測数, 非欠測率, 非ゼロ率, unique数) を返す純関数。"""
    import numpy as np
    import pandas as pd
    n = len(df)
    out = {}
    for c in df.columns:
        x = pd.to_numeric(df[c], errors="coerce")
        nn = int(x.notna().sum())
        nz = int((x.fillna(0) != 0).sum())
        uq = int(x.nunique(dropna=True))
        out[c] = {
            "n_nonnull": nn, "pct_nonnull": (nn / n if n else 0.0),
            "pct_nonzero": (nz / n if n else 0.0), "nunique": uq,
            "dead": (uq <= 1),
        }
    return out


# 定義上ほぼ定数になりうるフラグ（レース条件フラグ等）。JOIN不良でなく TRUE_CONSTANT。
_KNOWN_CONSTANT_FLAGS = {
    "kokusai", "shitei", "minarai", "gai", "chiho", "kongo", "kachi", "kyushu",
    "hinba", "handi", "wakate", "shogai", "toku", "tokushi", "barei", "bettei",
}


def classify_dead(col, prof_entry, *, source_present=None):
    """DEAD 列を SOURCE_MISSING/JOIN_FAILURE/UNSEEN_CATEGORY/TRUE_CONSTANT/UNKNOWN に分類する純関数。

    source_present: 生ソースの有無（True=在る/False=無い/None=不明）。
      - one-hot（列名に "__"）でファミリの一部だけ dead → UNSEEN_CATEGORY（出現しなかったカテゴリ）
      - 既知の条件フラグ名 → TRUE_CONSTANT
      - 生ソース無し → SOURCE_MISSING / 生ソース在り → JOIN_FAILURE / 不明 → UNKNOWN
    """
    if not prof_entry["dead"]:
        return "OK"
    name = str(col)
    if "__" in name:
        return "UNSEEN_CATEGORY"
    if name in _KNOWN_CONSTANT_FLAGS:
        return "TRUE_CONSTANT"
    if source_present is True:
        return "JOIN_FAILURE"
    if source_present is False:
        return "SOURCE_MISSING"
    return "UNKNOWN"


def group_by_prefix(profiles, prefixes):
    """列プロファイルを prefix ファミリ単位に集約する純関数。返す {prefix: {...}}。"""
    import statistics
    groups = {}
    for p in prefixes:
        cols = [c for c in profiles if str(c).startswith(p)]
        if not cols:
            groups[p] = {"n_cols": 0, "n_dead": 0, "all_dead": False,
                         "med_pct_nonnull": 0.0, "med_nunique": 0.0}
            continue
        dead = [c for c in cols if profiles[c]["dead"]]
        groups[p] = {
            "n_cols": len(cols),
            "n_dead": len(dead),
            "all_dead": len(dead) == len(cols),
            "med_pct_nonnull": statistics.median(profiles[c]["pct_nonnull"] for c in cols),
            "med_nunique": statistics.median(profiles[c]["nunique"] for c in cols),
        }
    return groups


def _norm_id(s):
    """ID を比較用に正規化（float の .0 除去・str 化・空白除去）。"""
    import pandas as pd
    return (pd.Series(s).astype(str).str.replace(r"\.0$", "", regex=True).str.strip())


def _owner_py_join_probe(py):
    """owner_py の join 不良を切り分ける: owner_id 形式・ID一致率・year一致率（P1・ユーザSQLの自動化）。"""
    import os

    import pandas as pd

    from src.constants._local_paths import LocalPaths
    print("\n  [owner_py join診断]（生ソースは在るのに coverage≈0 の原因を id/year で切り分け）")
    res_path = getattr(LocalPaths, "RAW_RESULTS_PATH", "")
    if not os.path.exists(res_path):
        print("   raw_results を読めず join診断スキップ")
        return
    res = pd.read_pickle(res_path)
    if "owner_id" not in res.columns:
        print("   raw_results に owner_id 無し")
        return
    r_ids = set(_norm_id(res["owner_id"].dropna()).unique())
    ow = py[py["entity_type"].astype(str) == "owner"]
    eid = ow.index if ow.index.name == "entity_id" else ow.get("entity_id")
    p_ids = set(_norm_id(eid.dropna()).unique()) if eid is not None else set()
    inter = r_ids & p_ids
    print(f"   owner_id ユニーク: results={len(r_ids):,} / person_yearly={len(p_ids):,} / 一致={len(inter):,}"
          f"（一致率 results側 {len(inter)/max(1,len(r_ids)):.1%}）")
    # 例示（形式差を目視で確認）
    print(f"   例 results owner_id : {sorted(list(r_ids))[:5]}")
    print(f"   例 person owner id  : {sorted(list(p_ids))[:5]}")
    if p_ids and len(inter) / max(1, len(r_ids)) < 0.2:
        print("   → ID一致率が低い＝owner_id の形式/型不一致（先頭ゼロ落ち・別ID空間等）が JOIN_FAILURE の主因。")
    # year 一致（results 年-1 が person_yearly の owner year に在るか）
    if "date" in res.columns and "year" in ow.columns:
        ry = set((pd.to_datetime(res["date"], errors="coerce").dt.year - 1).dropna().astype(int))
        py_years = set(pd.to_numeric(ow["year"], errors="coerce").dropna().astype(int))
        print(f"   前年(results年-1) {sorted(ry)[:3]}… ∩ person_yearly owner year {sorted(py_years)[:3]}… "
              f"= {len(ry & py_years)} 年一致")


def _raw_source_probe(featured_profiles=None):
    """生ソース(best-effort)を突合し、owner/peds/guide の取得可否と JOIN 可否を報告する。失敗は握りつぶす。"""
    import os

    import pandas as pd

    from src.constants._local_paths import LocalPaths
    print("\n[② 生ソース突合（best-effort）]")

    def _load(path):
        try:
            return pd.read_pickle(path)
        except Exception:  # noqa: BLE001
            return None

    # owner_py: 生ソースは在る(4683行)→ join診断で id/year 不一致を切り分け（SOURCE_MISSING でなく JOIN_FAILURE 検証）
    py = _load(getattr(LocalPaths, "RAW_PERSON_YEARLY_PATH", ""))
    if py is not None and not py.empty and "entity_type" in py.columns:
        counts = py["entity_type"].astype(str).value_counts().to_dict()
        print(f"  raw_person_yearly 行数/entity_type: {counts}")
        if counts.get("owner", 0):
            _owner_py_join_probe(py)
    else:
        print("  raw_person_yearly を読めず/空")

    # sire/damsire: ソースは peds.pkl（horse_info ではない）。peds_0/peds_32 の有無を正しい場所で確認。
    peds = _load(getattr(LocalPaths, "RAW_PEDS_PATH", ""))
    if peds is not None:
        has0 = "peds_0" in peds.columns
        has32 = "peds_32" in peds.columns
        print(f"\n  raw peds.pkl: 在り shape={peds.shape}  peds_0={has0}  peds_32={has32}")
        if not (has0 and has32):
            ped_like = [c for c in peds.columns if str(c).startswith("peds")][:8]
            print(f"   → peds_0/peds_32 が無い＝列名/フラット化不一致(SOURCE不在でなく列名ズレ)。peds列例: {ped_like}")
        else:
            print("   → 列は在る。sire/damsire DEAD は結合キー(horse_id)不一致 or peds_processor 出力の問題を疑う。")
    else:
        print("\n  raw peds.pkl を読めず＝sire/damsire は SOURCE_MISSING（血統ソース未取得。_scrape_html_ped で取得案件）")

    # guide: course_guide_master.csv（手入力 course_guide.csv からの生成物）。無ければ全 NaN。
    gm = getattr(LocalPaths, "COURSE_GUIDE_MASTER_PATH", "")
    gsrc = getattr(LocalPaths, "COURSE_GUIDE_PATH", "")
    print(f"\n  course_guide_master.csv 在り={os.path.exists(gm)} / 元 course_guide.csv 在り={os.path.exists(gsrc)}")
    if not os.path.exists(gm):
        print("   → guide_* 全 NaN の原因＝master 未生成。course_guide.csv が在れば scripts/scrape_course_master.py で生成、"
              "無ければ手入力データ取得案件（SOURCE_MISSING）。")


def main() -> int:
    import pandas as pd

    from app._model_eval import load_featured_data
    ap = argparse.ArgumentParser(description="特徴量充足監査（未取得ファミリの機械検出）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--top-dead", type=int, default=40, help="DEAD 列を最大何件まで列挙するか")
    ap.add_argument("--no-raw", action="store_true", help="生ソース突合をスキップ")
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（gitignore の巨大成果物。ローカルで実行してください）", file=sys.stderr)
        return 2
    print(f"=== 特徴量充足監査  featured shape={feat.shape} ===")
    prof = profile_columns(feat)
    groups = group_by_prefix(prof, _FAMILIES)

    print("\n[① ファミリ別充足]  (all_dead=全列が単一値＝未取得の疑い)")
    print(f"  {'ファミリ':<14}{'列数':>5}{'DEAD列':>7}{'全DEAD':>7}{'中央非欠測%':>11}{'中央unique':>10}")
    for p in _FAMILIES:
        g = groups[p]
        flag = "★未取得" if g["all_dead"] and g["n_cols"] else ""
        print(f"  {p:<14}{g['n_cols']:>5}{g['n_dead']:>7}{str(g['all_dead']):>7}"
              f"{g['med_pct_nonnull']:>11.1%}{g['med_nunique']:>10.0f} {flag}")

    # DEAD 列を種別分類（SOURCE_MISSING/JOIN_FAILURE/UNSEEN_CATEGORY/TRUE_CONSTANT/UNKNOWN）。
    # 生ソースの有無ヒント: peds/guide=SOURCE系, owner_py=JOIN系（生ソース在り）。
    import os as _os

    from src.constants._local_paths import LocalPaths as _LP
    peds_present = _os.path.exists(getattr(_LP, "RAW_PEDS_PATH", "")) or None
    src_hint = {}
    for c in prof:
        cs = str(c)
        if cs.startswith(("sire_", "damsire_")):
            src_hint[c] = peds_present            # peds.pkl 有無
        elif cs.startswith("guide_"):
            src_hint[c] = _os.path.exists(getattr(_LP, "COURSE_GUIDE_MASTER_PATH", "")) or None
        elif cs.startswith("owner_py"):
            src_hint[c] = True                    # 生ソース在り→ JOIN_FAILURE 側に寄せる
        else:
            src_hint[c] = None
    dead_all = sorted(c for c, v in prof.items() if v["dead"])
    by_cat: dict = {}
    for c in dead_all:
        cat = classify_dead(c, prof[c], source_present=src_hint.get(c))
        by_cat.setdefault(cat, []).append(c)
    print(f"\n  DEAD（単一値）列 合計 {len(dead_all):,}。種別内訳:")
    for cat in ("SOURCE_MISSING", "JOIN_FAILURE", "UNSEEN_CATEGORY", "TRUE_CONSTANT", "UNKNOWN"):
        cols = by_cat.get(cat, [])
        if cols:
            print(f"   [{cat}] {len(cols)}列: {', '.join(cols[: args.top_dead])}"
                  + (" …" if len(cols) > args.top_dead else ""))

    if not args.no_raw:
        _raw_source_probe(prof)
    print("\n※ DEAD は同一視しない: SOURCE_MISSING(血統/guide=ソース未取得)・JOIN_FAILURE(owner_py=生ソース在るが"
          "id/year不一致)・UNSEEN_CATEGORY(開催__/race_class__=未出現one-hot・正常)・TRUE_CONSTANT(条件フラグ)。"
          "投資対効果順: P1 owner_py join → P2 guide master → P3 sire/damsire ソース。修正後 再監査で DEAD 減を確認。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
