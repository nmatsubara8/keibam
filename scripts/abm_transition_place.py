"""最小ABM状態遷移モデル（Route C 最終ゲート）— 構造プライアが flat GBM を超えるか。

検証仮説（ユーザ）: ABM の状態遷移(逐次Markov構造)は、GBM が有限データで学べない
「既存情報の組み合わせ爆発」を構造プライアとして表現でき、P_ABM(rank≤3) の対市場エッジが
P_GBM(rank≤3) を超えるか？ 新情報ではなく**構造**の勝負。

最小構成（フル物理ABM不要）: corner1-4(実測位置系列, raw_jrdb_sed)から3つの GBM を学習し
事前状態から決定論ロールアウト:
  start:  pre-race(脚質/ten/pace/idm/枠/頭数/逃げ数) → corner1位置
  trans:  (pos_t, 脚質, ten, pace, idm, 頭数, 逃げ数) → pos_{t+1}   （c1→c2,c2→c3,c3→c4 をプール）
  finish: (pos4, idm, agari, 脚質) → P(rank≤3)
  ロールアウト: pos1=start → pos2=trans(pos1) → ... → pos4 → P_ABM=finish(pos4)
比較（真OOS・複勝ROI／年別／プラセボ）: P_ABM vs P_GBM(静的+相互作用) vs 市場。
成功: ABM の top複勝ROI > GBM かつ >0.93（有望0.97 / 投資1.0）、年別一致、プラセボ崩壊。

使い方:
  python scripts/abm_transition_place.py --jra-only --db data/keibam.db --cutoff-year 2024
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def norm_pos(corner: pd.Series, field_size: pd.Series) -> pd.Series:
    """コーナー順位 → レース内相対位置 [0,1]（0=先頭, 1=最後方）。不正は NaN。"""
    c = pd.to_numeric(corner, errors="coerce")
    return ((c - 1.0) / (field_size - 1.0).clip(lower=1)).where((c >= 1) & (field_size > 1))


def rollout_positions(start_pos: np.ndarray, trans_fn, feats_base: np.ndarray,
                      n_steps: int = 3) -> np.ndarray:
    """start位置から trans_fn を n_steps 回適用（決定論ロールアウト）。最終位置を返す。

    trans_fn(pos_t(1d), feats_base(2d)) -> pos_{t+1}(1d)。feats_base は毎ステップ不変の
    事前特徴（脚質/ten/pace/idm/頭数/逃げ数）。位置だけが遷移する。
    """
    pos = np.clip(start_pos, 0.0, 1.0)
    for _ in range(n_steps):
        pos = np.clip(trans_fn(pos, feats_base), 0.0, 1.0)
    return pos


def place_roi_by_score(score: np.ndarray, won: np.ndarray, payoff_mult: np.ndarray,
                       top_pct: float) -> tuple[float, int]:
    """score 上位 top_pct% を複勝全張りした ROI と件数（精算=確定複勝払戻）。"""
    k = max(1, int(len(score) * top_pct / 100))
    idx = np.argsort(-score)[:k]
    return float((payoff_mult[idx] * won[idx]).mean()), k


def _load_col(engine, table, col):
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))[["rid", "uma", col]]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="最小ABM遷移モデル（Route C 最終ゲート）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024)
    ap.add_argument("--placebo", action="store_true", help="遷移特徴をレース内シャッフル")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from lightgbm import LGBMClassifier, LGBMRegressor

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
    base["year"] = base["rid"].str[:4]

    eng = get_engine(args.db)
    parts = [base]
    for tbl, col in [("raw_jrdb_sed", "corner1"), ("raw_jrdb_sed", "corner2"),
                     ("raw_jrdb_sed", "corner3"), ("raw_jrdb_sed", "corner4"),
                     ("raw_jrdb_kyi", "idm"), ("raw_jrdb_kyi", "kyakushitsu"),
                     ("raw_jrdb_kyi", "ten_idx"), ("raw_jrdb_kyi", "pace_idx"),
                     ("raw_jrdb_kyi", "agari_idx"), ("raw_jrdb_tyb", "fukusho_odds"),
                     ("raw_jrdb_sed", "fukusho_payoff")]:
        parts.append(_load_col(eng, tbl, col))
    df = parts[0]
    for p in parts[1:]:
        df = df.merge(p, on=["rid", "uma"], how="inner")
    df["field_size"] = df.groupby("rid")["uma"].transform("size")
    df["nige_cnt"] = df.assign(_f=(pd.to_numeric(df["kyakushitsu"], errors="coerce") == 1).astype(float)
                               ).groupby("rid")["_f"].transform("sum")
    for t in (1, 2, 3, 4):
        df[f"p{t}"] = norm_pos(df[f"corner{t}"], df["field_size"])
    df = df.dropna(subset=["p1", "p2", "p3", "p4", "idm", "kyakushitsu", "ten_idx"])
    df["pay"] = (df["fukusho_payoff"] / 100.0).fillna(0.0)   # 複勝オッズは実オッズ格納(place検証で×1.0確認)
    # 市場複勝確率 q（Σ=3 正規化）
    inv = 1.0 / df["fukusho_odds"].where(df["fukusho_odds"] > 0)
    df["q"] = (inv / inv.groupby(df["rid"]).transform("sum") * 3).clip(upper=0.99).fillna(0.0)
    print(f"[abm] 結合 {len(df):,}頭 / {df['rid'].nunique():,}レース")

    pre = ["kyakushitsu", "ten_idx", "pace_idx", "idm", "field_size", "nige_cnt"]  # 事前特徴
    tr = df[df["year"].astype(int) < args.cutoff_year]
    te = df[df["year"].astype(int) >= args.cutoff_year]
    if len(tr) < 5000 or len(te) < 5000:
        print("[abm] データ薄。", file=sys.stderr)
        return 1
    print(f"[abm] 学習<{args.cutoff_year}: {len(tr):,} / 真OOS: {len(te):,}\n")

    def _lgbm_reg():
        return LGBMRegressor(n_estimators=300, num_leaves=31, learning_rate=0.05,
                             min_child_samples=200, subsample=0.8, colsample_bytree=0.8, verbose=-1)

    # start: 事前 → corner1位置
    m_start = _lgbm_reg().fit(tr[pre].to_numpy(), tr["p1"].to_numpy())
    # trans: (pos_t + 事前) → pos_{t+1}、c1→c2/c2→c3/c3→c4 をプール
    Xtr_t = np.vstack([np.column_stack([tr[f"p{t}"].to_numpy(), tr[pre].to_numpy()]) for t in (1, 2, 3)])
    ytr_t = np.concatenate([tr[f"p{t+1}"].to_numpy() for t in (1, 2, 3)])
    m_trans = _lgbm_reg().fit(Xtr_t, ytr_t)
    # finish: (pos4 + idm/agari/脚質) → P(rank≤3)
    fin_cols = ["idm", "agari_idx", "kyakushitsu"]
    m_fin = LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                           min_child_samples=200, verbose=-1).fit(
        np.column_stack([tr["p4"].to_numpy(), tr[fin_cols].to_numpy()]), tr["won"].to_numpy())

    def trans_fn(pos, base_feats):
        return m_trans.predict(np.column_stack([pos, base_feats]))

    def abm_predict(sub):
        pf = sub[pre].to_numpy()
        pos1 = np.clip(m_start.predict(pf), 0, 1)
        pos4 = rollout_positions(pos1, trans_fn, pf, n_steps=3)
        return m_fin.predict_proba(np.column_stack([pos4, sub[fin_cols].to_numpy()]))[:, 1]

    if args.placebo:
        te = te.copy()
        rng = np.random.default_rng(0)
        for c in pre:
            te[c] = te.groupby("rid")[c].transform(lambda s: s.to_numpy()[rng.permutation(len(s))])

    p_abm = abm_predict(te)
    # 対照: flat GBM（同じ事前特徴＋市場 logit(q)）→ P(rank≤3）
    from scipy.special import logit as _logit
    lq_tr = _logit(np.clip(tr["q"].to_numpy(), 1e-6, 1 - 1e-6))
    lq_te = _logit(np.clip(te["q"].to_numpy(), 1e-6, 1 - 1e-6))
    m_gbm = LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                           min_child_samples=200, verbose=-1).fit(
        np.column_stack([lq_tr, tr[pre].to_numpy()]), tr["won"].to_numpy())
    p_gbm = m_gbm.predict_proba(np.column_stack([lq_te, te[pre].to_numpy()]))[:, 1]

    from sklearn.metrics import log_loss
    yte, qte, pay = te["won"].to_numpy(), te["q"].to_numpy(), te["pay"].to_numpy()
    print("[abm] OOS logloss（小さいほど良い）")
    print(f"  市場q={log_loss(yte, np.clip(qte,1e-6,1-1e-6)):.5f}  "
          f"flatGBM={log_loss(yte, p_gbm):.5f}  ABM={log_loss(yte, p_abm):.5f}")
    print("\n[abm] 複勝ROI（score上位x%を確定払戻で全張り）: ABM vs flatGBM")
    print(f"  {'上位':>7}{'ABM_ROI':>10}{'GBM_ROI':>10}")
    for pct in (10.0, 5.0, 2.0, 1.0):
        ra, _ = place_roi_by_score(p_abm, yte, pay, pct)
        rg, _ = place_roi_by_score(p_gbm, yte, pay, pct)
        print(f"  {pct:>6.1f}%{ra:>10.4f}{rg:>10.4f}")
    print("\n[abm] 年別 上位5% ABM複勝ROI")
    yr = te["year"].to_numpy()
    for y in sorted(set(yr.tolist())):
        m = yr == y
        if m.sum() > 500:
            ra, _ = place_roi_by_score(p_abm[m], yte[m], pay[m], 5.0)
            print(f"  {y}: ROI={ra:.4f}")
    best_abm, _ = place_roi_by_score(p_abm, yte, pay, 1.0)
    best_gbm, _ = place_roi_by_score(p_gbm, yte, pay, 1.0)
    print(f"\n[abm] 判定: ABM上位1%ROI={best_abm:.4f} / GBM={best_gbm:.4f} / 差={best_abm-best_gbm:+.4f}")
    if best_abm > best_gbm and best_abm > 0.93:
        print("  → 構造プライアが flat GBM を超え 0.93 突破。Monte Carlo化(Step2)の価値あり。")
    else:
        print("  → ABM は flat GBM を超えず(or <0.93)。構造プライアの上乗せ無し＝Route C 実質決着。"
              "残るは Route B(映像=新情報) のみ。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
