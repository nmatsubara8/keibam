"""卍式「購入ゾーン」再現 — (残差edge帯 × オッズ帯) の2D ROIヒートマップ＋両側打ち切り＋walk-forward。

我々の既存検証は「edge 上位x%（片側テール）」だった。卍式は (指数帯 × オッズ帯) の**中央購入帯**を
使い、極端edge（=モデル過信=欠落変数疑い）を**両側打ち切り**で除外する。これは非重複の未検証軸。

方法（多重検定を規律で抑える）:
  1. 各年までの学習期間で (edge帯 × 複勝オッズ帯) セルごとの複勝ROIを算出
  2. ROI>閾値 かつ n≥min のセルだけ「購入ゾーン」に採用（両側打ち切り＝特定のedge帯×オッズ帯のみ）
  3. 翌年OOSに購入ゾーンを固定適用 → ROI。毎年ウォークフォワード（購入ルールは前年までで確定）
edge = p_model(place GBM) − q_market(複勝オッズ)。精算=JRDB SED 確定複勝払戻(100%)。

成功: OOS walk-forward の購入ゾーンROIが安定して >1（複数年）。0.90前後なら卍式購入面でも控除未満。

使い方:
  python scripts/manji_purchase_zone.py --jra-only --db data/keibam.db
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402

_ORTH = ["deokure_rate", "pace_idx", "chokyo_idx", "gekiso_idx",
         "start_idx", "ten_idx", "agari_idx", "manken_idx"]
_ODDS_EDGES = [0, 2, 4, 7, 15, np.inf]     # 複勝オッズ帯（両側打ち切りの y 軸）


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def odds_band(o: float) -> int:
    for i in range(len(_ODDS_EDGES) - 1):
        if _ODDS_EDGES[i] <= o < _ODDS_EDGES[i + 1]:
            return i
    return len(_ODDS_EDGES) - 2


def edge_band(edge: np.ndarray, n_bands: int = 10) -> np.ndarray:
    """edge を分位で n_bands 帯に（同値は first）。学習・検証で同じ境界を使う想定で分位ランク。"""
    r = pd.Series(edge).rank(method="first")
    return (r / (len(edge) + 1) * n_bands).astype(int).clip(0, n_bands - 1).to_numpy()


def cell_roi_table(df: pd.DataFrame) -> pd.DataFrame:
    """(edge帯,オッズ帯) セルごとの n / 複勝ROI（=payoff·won の平均）を返す。"""
    g = df.groupby(["eb", "ob"])
    return g.apply(lambda x: pd.Series({
        "n": len(x), "roi": float((x["pay"] * x["won"]).mean()), "hit": float(x["won"].mean())
    }), include_groups=False).reset_index()


def select_zone(train_roi: pd.DataFrame, min_roi: float, min_n: int) -> set:
    """学習期間で ROI≥min_roi かつ n≥min_n のセル集合（=購入ゾーン・両側打ち切り）。"""
    ok = train_roi[(train_roi["roi"] >= min_roi) & (train_roi["n"] >= min_n)]
    return set(zip(ok["eb"], ok["ob"], strict=False))


def apply_zone(test_df: pd.DataFrame, zone: set) -> tuple[float, int, float]:
    """購入ゾーンに入る test 行だけ複勝全張り → (ROI, 件数, 払戻合計)。"""
    mask = [(eb, ob) in zone for eb, ob in zip(test_df["eb"], test_df["ob"], strict=False)]
    sub = test_df[mask]
    if len(sub) == 0:
        return float("nan"), 0, 0.0
    pay_sum = float((sub["pay"] * sub["won"]).sum())
    return pay_sum / len(sub), len(sub), pay_sum


def _load_col(engine, table, col):
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))[["rid", "uma", col]]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="卍式 購入ゾーン（2D ROI・両側打ち切り・walk-forward）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--min-roi", type=float, default=1.0, help="購入ゾーン採用の学習期間ROI閾値")
    ap.add_argument("--min-n", type=int, default=300, help="セル採用の最小サンプル数")
    ap.add_argument("--n-edge-bands", type=int, default=10)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from lightgbm import LGBMClassifier
    from scipy.special import logit

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.storage._db import get_engine

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "won": (rank <= 3).astype(float).to_numpy(),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4].astype(int)

    eng = get_engine(args.db)
    df = base
    from sqlalchemy import text
    kyi_cols = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), eng).columns.tolist()
    have = [c for c in _ORTH if c in kyi_cols]
    kyi = pd.read_sql(text(f"SELECT race_id, umaban, {', '.join(have)} FROM raw_jrdb_kyi"), eng)
    kyi["rid"] = kyi["race_id"].astype(str).str.split(".").str[0]
    kyi["uma"] = pd.to_numeric(kyi["umaban"], errors="coerce")
    kyi = kyi.dropna(subset=["uma"])
    kyi["uma"] = kyi["uma"].astype(int)
    for c in have:
        kyi[c] = pd.to_numeric(kyi[c], errors="coerce")
    df = (df.merge(kyi[["rid", "uma", *have]], on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_tyb", "fukusho_odds").rename(columns={"fukusho_odds": "fo"}),
                   on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_sed", "fukusho_payoff").rename(columns={"fukusho_payoff": "fp"}),
                   on=["rid", "uma"], how="inner"))
    df = df[df["fo"] > 0].copy()
    df["pay"] = (df["fp"] / 100.0).fillna(0.0)
    inv = 1.0 / df["fo"]
    df["q"] = (inv / inv.groupby(df["rid"]).transform("sum") * 3).clip(upper=0.99)
    df["ob"] = df["fo"].map(odds_band)
    print(f"[manji] 結合 {len(df):,}頭 / {df['rid'].nunique():,}レース")

    years = sorted(df["year"].unique())
    print(f"[manji] 年 {years}｜walk-forward（前年までで購入ゾーン確定→翌年OOS適用）\n")
    print(f"  {'test年':>7}{'zoneセル数':>10}{'OOS_n':>9}{'OOS_ROI':>10}{'全張りROI(参考)':>16}")
    oos_pay, oos_n = 0.0, 0
    for i in range(2, len(years)):     # 最初の2年は学習下限
        tr = df[df["year"] < years[i]].copy()
        te = df[df["year"] == years[i]].copy()
        if len(tr) < 20000 or len(te) < 3000:
            continue
        # place GBM を学習期間で fit → edge=p_model−q（train境界でedge帯を定義）
        lq_tr = logit(np.clip(tr["q"].to_numpy(), 1e-6, 1 - 1e-6))
        m = LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                           min_child_samples=200, verbose=-1).fit(
            np.column_stack([lq_tr, tr[have].fillna(tr[have].median()).to_numpy()]), tr["won"].to_numpy())
        for d in (tr, te):
            lq = logit(np.clip(d["q"].to_numpy(), 1e-6, 1 - 1e-6))
            pmod = m.predict_proba(np.column_stack([lq, d[have].fillna(tr[have].median()).to_numpy()]))[:, 1]
            d["edge"] = pmod - d["q"].to_numpy()
        tr["eb"] = edge_band(tr["edge"].to_numpy(), args.n_edge_bands)
        # test の edge帯は train の分位境界で（未来情報を使わない）
        qbins = np.quantile(tr["edge"], np.linspace(0, 1, args.n_edge_bands + 1))
        qbins[0], qbins[-1] = -np.inf, np.inf
        te["eb"] = np.clip(np.digitize(te["edge"].to_numpy(), qbins[1:-1]), 0, args.n_edge_bands - 1)
        zone = select_zone(cell_roi_table(tr), args.min_roi, args.min_n)
        roi, n, pay_sum = apply_zone(te, zone)
        flat = float((te["pay"] * te["won"]).mean())
        roi_s = "—" if np.isnan(roi) else f"{roi:.4f}"
        print(f"  {years[i]:>7}{len(zone):>10}{n:>9}{roi_s:>10}{flat:>16.4f}")
        if n > 0:
            oos_pay += pay_sum
            oos_n += n
    overall = oos_pay / oos_n if oos_n else float("nan")
    print(f"\n[manji] walk-forward 購入ゾーン全体 OOS_ROI={overall:.4f}（n={oos_n:,}）")
    if overall > 1.0:
        print("  → 卍式購入ゾーンが控除超え候補。年別一貫性・プラセボ・自票オッズ低下を要精査。")
    else:
        print("  → 購入ゾーンでも <1＝両側打ち切りでも控除未満。卍の当時アルファは減衰/環境差の公算。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
