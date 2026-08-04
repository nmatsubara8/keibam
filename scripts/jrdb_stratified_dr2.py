"""層別 ΔR² — プール平均の「エッジ無し」が層内でも成り立つかを規律つきで検証する。

全レースをプールした ΔR²≈0 は平均であり、特定の層（競馬場/頭数帯/距離帯/馬場種別）で市場が
非効率＝ΔR²>0 の可能性を隠しているかもしれない。ただし層を細かく切ると多重比較で偶然の正が
必ず出る（NAR が in-sample 正→OOS 負の実例）。そこで2段階:

  1. スクリーン（安価）: 各層で **in-sample ΔR²**（同データで blend(α,β) fit）。**≤0 の層は即死**。
  2. OOS 検証: in-sample 正の層だけ、年A で fit → 年B で ΔR² 評価（両方向）。**両方向とも正**の層
     だけ「生存（要追加調査）」。片方向でも負なら過学習ノイズ＝棄却。

層は**事前定義**（後付け禁止で多重比較を抑制）: 競馬場 / 頭数帯 / 距離帯 / race_type。
baseline_jrdb（Win ヘッド）の p̂ を holdout（既定 2024-2025・JRA）で層別に測る。

ΔR² は `_edge_diagnostic`/`_blend` の既存機構を再利用（combining logit=市場×ファンダ）。

使い方:
  python scripts/jrdb_stratified_dr2.py --version baseline_jrdb --years 2024 2025 --jra-only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._local_paths import LocalPaths  # noqa: E402
from src.constants._model_category import central_index_mask  # noqa: E402
from src.policies._blend import blend_diagnostic, fit_blend  # noqa: E402

_PLACE = {"01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
          "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉"}


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def edge_to_races(edge_df: pd.DataFrame) -> list:
    """edge_df（index=race_id・umaban/r_hat/p_mkt/won）→ BlendRace (p_fund,p_public,winner) の列。"""
    races = []
    for _, g in edge_df.groupby(level=0):
        p_fund = {int(u): float(p) for u, p in zip(g["umaban"], g["r_hat"], strict=False) if p > 0}
        p_public = {int(u): float(p) for u, p in zip(g["umaban"], g["p_mkt"], strict=False) if p > 0}
        winners = g.loc[g["won"] == 1, "umaban"]
        if len(winners) != 1 or not p_fund or not p_public:
            continue
        races.append((p_fund, p_public, int(winners.iloc[0])))
    return races


def dr2_in_sample(races: list, min_races: int) -> float | None:
    """同データで blend fit→ΔR²（楽観・スクリーン用）。"""
    if len(races) < min_races:
        return None
    return blend_diagnostic(races, fit_blend(races))["delta_r2"]


def dr2_oos(races_train: list, races_test: list, min_races: int) -> float | None:
    """train で blend(α,β) fit→test で ΔR²（過学習に騙されない本番）。"""
    if len(races_train) < min_races or len(races_test) < min_races:
        return None
    return blend_diagnostic(races_test, fit_blend(races_train))["delta_r2"]


def field_size_band(n: int) -> str:
    return "≤8" if n <= 8 else "9-12" if n <= 12 else "13-16" if n <= 16 else "17-18"


def dist_band(course_len: object) -> str | None:
    if course_len is None or pd.isna(course_len):
        return None
    x = float(course_len)
    return ("sprint≤1400" if x <= 1400 else "mile1401-1800" if x <= 1800
            else "mid1801-2200" if x <= 2200 else "long>2200")


# ── 層キー（race_id 単位の Series を作る） ───────────────────────────
def _race_level_keys(edge_df: pd.DataFrame, featured: pd.DataFrame) -> dict[str, pd.Series]:
    """各層 slug → race_id を index とする層値 Series。"""
    rid = pd.Series(edge_df.index.unique(), index=edge_df.index.unique()).astype(str)
    keys: dict[str, pd.Series] = {}
    # 競馬場
    keys["競馬場"] = rid.str[4:6].map(lambda c: _PLACE.get(c, c))
    # 頭数帯
    size = edge_df.groupby(level=0).size()
    keys["頭数帯"] = size.map(field_size_band)
    # 距離帯（featured の course_len をレース代表値で）
    if "course_len" in featured.columns:
        cl = pd.to_numeric(featured["course_len"], errors="coerce").groupby(level=0).first()
        keys["距離帯"] = cl.map(dist_band)
    # race_type（ダミーから復元）
    try:
        from src.training._category_split import recover_race_type
        rt = recover_race_type(featured).groupby(level=0).first()
        keys["race_type"] = rt
    except Exception:  # noqa: BLE001 — 復元不能なら race_type 層はスキップ
        pass
    return keys


def _analyze(edge_df, featured, min_races, screen_thr):
    """層別 ΔR² を計算して行リストを返す（純関数・テスト可能）。"""
    keys = _race_level_keys(edge_df, featured)
    rows = []
    for strat, kser in keys.items():
        stratum_of = edge_df.index.to_series().map(kser)
        for sval, sub in edge_df.groupby(stratum_of.to_numpy()):
            if sval is None or (isinstance(sval, float) and pd.isna(sval)):
                continue
            races = edge_to_races(sub)
            d_in = dr2_in_sample(races, min_races)
            if d_in is None:
                continue
            row = {"層": strat, "値": sval, "races": len(races),
                   "ΔR²_in": d_in, "ΔR²_A→B": None, "ΔR²_B→A": None, "判定": "死(in≤0)"}
            if d_in > screen_thr:
                sub_yr = pd.Series(sub.index.astype(str).str[:4], index=sub.index)
                yrs = sorted(sub_yr.dropna().unique())
                if len(yrs) >= 2:
                    a, b = yrs[0], yrs[-1]
                    rA = edge_to_races(sub[sub_yr.to_numpy() == a])
                    rB = edge_to_races(sub[sub_yr.to_numpy() == b])
                    d_ab = dr2_oos(rA, rB, min_races)
                    d_ba = dr2_oos(rB, rA, min_races)
                    row["ΔR²_A→B"], row["ΔR²_B→A"] = d_ab, d_ba
                    if d_ab is not None and d_ba is not None and d_ab > 0 and d_ba > 0:
                        row["判定"] = "★生存(OOS両方向+)"
                    elif d_ab is None or d_ba is None:
                        row["判定"] = "OOS不足(層薄)"
                    else:
                        row["判定"] = "棄却(OOSで消失)"
                else:
                    row["判定"] = "単年(OOS不可)"
            rows.append(row)
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="層別 ΔR²（in-sampleスクリーン→OOS検証）")
    ap.add_argument("--version", default="baseline_jrdb", help="評価するモデル版名")
    ap.add_argument("--years", type=int, nargs="+", default=[2024, 2025], help="holdout 年（2年推奨）")
    ap.add_argument("--jra-only", action="store_true", help="中央のみ")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--min-races", type=int, default=200, help="層/年ごとの最小レース数")
    ap.add_argument("--screen-thr", type=float, default=0.0, help="in-sample ΔR² のスクリーン閾値")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from app._data_loader import load_model_from_path, load_win_head_for
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.simulation._edge_diagnostic import run_edge_diagnostic

    path = _resolve_backtest_model_path(args.version)
    place_ai = load_model_from_path(path)
    win_ai = load_win_head_for(path)
    edge_model = (win_ai or place_ai).effective_model
    print(f"[strat] モデル {Path(path).name}（Win ヘッド={'あり' if win_ai else 'なし'}）")

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    yset = {str(y) for y in args.years}
    featured = featured[featured.index.astype(str).str[:4].isin(yset)]
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    if featured.empty:
        print("対象 featured が空（年/jra-only を確認）。", file=sys.stderr)
        return 1
    print(f"[strat] holdout {sorted(yset)} JRA={args.jra_only}: {len(featured):,} 行")

    edge_df = run_edge_diagnostic(edge_model, featured)["edge_df"]
    rows = _analyze(edge_df, featured, args.min_races, args.screen_thr)
    if not rows:
        print("層が最小レース数を満たさず。--min-races を下げるか年を増やしてください。", file=sys.stderr)
        return 1

    df = pd.DataFrame(rows).sort_values(["層", "ΔR²_in"], ascending=[True, False])
    def _f(v):
        return "—" if v is None else f"{v:+.4f}"
    print("\n[strat] 層別 ΔR²（in-sample スクリーン → OOS 検証）")
    print(f"  {'層':<10}{'値':<14}{'races':>7}{'ΔR²_in':>10}{'A→B':>10}{'B→A':>10}  判定")
    for _, r in df.iterrows():
        print(f"  {r['層']:<10}{str(r['値']):<14}{r['races']:>7}{_f(r['ΔR²_in']):>10}"
              f"{_f(r['ΔR²_A→B']):>10}{_f(r['ΔR²_B→A']):>10}  {r['判定']}")
    survivors = df[df["判定"].str.startswith("★")]
    print(f"\n[strat] ★生存（OOS 両方向で ΔR²>0）: {len(survivors)} 層")
    if len(survivors):
        for _, r in survivors.iterrows():
            print(f"    {r['層']}={r['値']}: in {r['ΔR²_in']:+.4f} / OOS {r['ΔR²_A→B']:+.4f},{r['ΔR²_B→A']:+.4f}")
        print("  → これらは要追加調査（条件付き TE 等）。ただし薄い層は次期でも再現するか要確認。")
    else:
        print("  → 生存ゼロ＝層別でも対市場エッジ無し（プール平均の null が層内でも成立）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
