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


def _raw_source_probe():
    """生ソース(best-effort)を突合し、owner/breeder/peds の取得可否を報告する。失敗は握りつぶす。"""
    import pandas as pd

    from src.constants._local_paths import LocalPaths
    print("\n[② 生ソース突合（best-effort）]")

    def _load(path):
        try:
            return pd.read_pickle(path)
        except Exception:  # noqa: BLE001
            return None

    py = _load(getattr(LocalPaths, "RAW_PERSON_YEARLY_PATH", ""))
    if py is not None and not py.empty and "entity_type" in py.columns:
        counts = py["entity_type"].astype(str).value_counts().to_dict()
        print(f"  raw_person_yearly 行数/entity_type: {counts}")
        if counts.get("owner", 0) == 0:
            print("  → owner 行が0＝owner_py 未取得の直接証拠。backfill-persons に owner を含め再取得が必要。")
        if counts.get("breeder", 0) and not counts.get("owner", 0):
            print("  → breeder は取得済みだが owner 未取得＝両者で id ソースが違う（owner_id 由来）ことが原因の可能性大。")
    else:
        print("  raw_person_yearly を読めず/空（owner_py/breeder_py の生証拠は取れず）")

    res = _load(getattr(LocalPaths, "RAW_RESULTS_PATH", ""))
    if res is not None:
        for col in ("owner_id", "jockey_id", "trainer_id"):
            print(f"  raw_results.{col} 在り={col in res.columns}"
                  + ("" if col in res.columns else "  ← backfill が owner を skip する直接原因"))
    hi = _load(getattr(LocalPaths, "RAW_HORSE_INFO_PATH", ""))
    if hi is not None:
        for col in ("breeder_id", "peds_0", "peds_32"):
            print(f"  raw_horse_info.{col} 在り={col in hi.columns}")


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

    dead_all = sorted(c for c, v in prof.items() if v["dead"])
    print(f"\n  DEAD（単一値）列 合計 {len(dead_all):,}。先頭 {args.top_dead}:")
    print("   ", ", ".join(dead_all[: args.top_dead]))

    if not args.no_raw:
        _raw_source_probe()
    print("\n※ ★未取得ファミリは『効果なし』でなく FE 段で値が入っていない。生ソース突合で owner 行0 や"
          " raw_results に owner_id 無し等が出たら、backfill/rebuild-featured を修正して再生成する。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
