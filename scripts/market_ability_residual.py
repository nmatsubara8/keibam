"""Δ_ability ゲート実験（一回限り・事前固定）— 市場と異なる能力表現を足しても改善するか。

研究の締め: 「購入ルールで改善せず(#1厳密クローズ)」に加え「市場と異なる能力表現(過去sotenの市場直交
残差)を足しても改善せず」まで言えるようにする。ROI>1 は要求しない（粗い特徴の増分信号を見る）。

設計（ユーザ承認・2修正込み）:
  A. 過去 soten の事前固定集約（全て当該走除外＝leak-safe）:
       S_last=s_{t-1} / S_mean3 / S_max5 / S_ewm(λ=0.7) / S_trend=S_last−mean(s_{t-2},s_{t-3})
     → 小型GBM で1本に圧縮 MySpeed=Ê[place|S群]（fold内 <Y で学習）
  B. 市場能力写像 g を **fold内学習**（固定式でなく）。特徴: IDM, IDM², log基準オッズ,
       基準人気レース内百分位, 距離。Δ_ability = MySpeed − ĝ（水準差の誤直交化を防ぐ）
  C. 3モデルを真OOS比較: Base(市場q) / Orth(既存直交GBM) / Orth+Δability
     指標: logloss, AUC, ΔlogLoss(Orth+Δ − Orth), 年別ΔlogLoss, プラセボ(Δをレース内シャッフル),
           Δability decile 実現率(単調?), 複勝ROI 上位10/5/2/1%（Orth vs Orth+Δ）。

事前固定 判定:
  クローズ: ΔlogLoss > −0.0005 / 年別で符号不一致 / プラセボと区別不能 / decile非単調。
  本格buildへ: ΔlogLoss ≤ −0.001 かつ 年別方向一致 かつ decile単調 かつ プラセボで消失。

使い方:
  python scripts/market_ability_residual.py --jra-only --db data/keibam.db --cutoff-year 2024
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
_S = ["s_last", "s_mean3", "s_max5", "s_ewm", "s_trend"]


# ── 純ロジック（テスト対象） ─────────────────────────────────────────
def build_hist_soten(df: pd.DataFrame) -> pd.DataFrame:
    """[horse_id, race_id(sortable), soten] → 当該走除外の過去集約（leak-safe）を付与して返す。"""
    d = df.sort_values(["horse_id", "race_id"]).copy()
    sh1 = d.groupby("horse_id")["soten"].shift(1)     # 直前まで（当該走を含めない）
    d["s_last"] = sh1
    d["s_mean3"] = sh1.groupby(d["horse_id"]).transform(lambda x: x.rolling(3, min_periods=1).mean())
    d["s_max5"] = sh1.groupby(d["horse_id"]).transform(lambda x: x.rolling(5, min_periods=1).max())
    d["s_ewm"] = sh1.groupby(d["horse_id"]).transform(
        lambda x: x.ewm(alpha=0.3, min_periods=1).mean())    # λ=0.7 → alpha=1−λ=0.3
    sh2 = d.groupby("horse_id")["soten"].shift(2)
    sh3 = d.groupby("horse_id")["soten"].shift(3)
    d["s_trend"] = d["s_last"] - (sh2 + sh3) / 2.0
    return d


def orth_residual(myspeed: np.ndarray, market_X: np.ndarray, fit_mask: np.ndarray,
                  ridge_lambda: float = 1.0) -> np.ndarray:
    """MySpeed を市場能力特徴 market_X へ回帰(fit_mask=<Y のみ学習)し、全体の残差を返す。"""
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler
    sc = StandardScaler().fit(market_X[fit_mask])
    Xs = sc.transform(market_X)
    g = Ridge(alpha=ridge_lambda).fit(Xs[fit_mask], myspeed[fit_mask])
    return myspeed - g.predict(Xs)


def decile_realized(delta: np.ndarray, won: np.ndarray, n: int = 10) -> np.ndarray:
    """Δ の decile ごとの実現率（単調性チェック用）。"""
    b = pd.qcut(pd.Series(delta).rank(method="first"), n, labels=False)
    return pd.Series(won).groupby(b).mean().to_numpy()


def _load_col(engine, table, col):
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))[["rid", "uma", col]]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Δ_ability ゲート実験（一回限り）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024)
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from lightgbm import LGBMClassifier
    from scipy.special import logit
    from sklearn.metrics import log_loss, roc_auc_score

    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.storage._db import get_engine
    from sqlalchemy import text

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    hid = (featured["horse_id"].astype(str) if "horse_id" in featured.columns
           else featured.index.astype(str).str[:4])   # horse_id 無ければ退避
    clen = pd.to_numeric(featured["course_len"], errors="coerce") if "course_len" in featured.columns \
        else pd.Series(np.nan, index=featured.index)
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "won": (rank <= 3).astype(float).to_numpy(),
        "horse_id": hid.to_numpy(),
        "course_len": clen.to_numpy(),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4].astype(int)

    eng = get_engine(args.db)
    kyi_cols = pd.read_sql(text("SELECT * FROM raw_jrdb_kyi LIMIT 0"), eng).columns.tolist()
    have = [c for c in _ORTH if c in kyi_cols]
    mkt_cols = [c for c in ["idm", "kijun_odds", "kijun_ninki"] if c in kyi_cols]
    kyi = pd.read_sql(text(f"SELECT race_id, umaban, {', '.join(have + mkt_cols)} FROM raw_jrdb_kyi"), eng)
    kyi["rid"] = kyi["race_id"].astype(str).str.split(".").str[0]
    kyi["uma"] = pd.to_numeric(kyi["umaban"], errors="coerce")
    kyi = kyi.dropna(subset=["uma"])
    kyi["uma"] = kyi["uma"].astype(int)
    for c in have + mkt_cols:
        kyi[c] = pd.to_numeric(kyi[c], errors="coerce")
    df = (base.merge(kyi[["rid", "uma", *have, *mkt_cols]], on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_sed", "soten"), on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_tyb", "fukusho_odds").rename(columns={"fukusho_odds": "fo"}),
                   on=["rid", "uma"], how="inner")
            .merge(_load_col(eng, "raw_jrdb_sed", "fukusho_payoff").rename(columns={"fukusho_payoff": "fp"}),
                   on=["rid", "uma"], how="inner"))
    df["horse_id"] = df["horse_id"].astype(str)
    df = build_hist_soten(df)      # leak-safe 過去集約
    df = df[df["fo"] > 0].copy()
    df["pay"] = (df["fp"] / 100.0).fillna(0.0)
    inv = 1.0 / df["fo"]
    df["q"] = (inv / inv.groupby(df["rid"]).transform("sum") * 3).clip(upper=0.99)
    # 基準人気レース内百分位・IDM² を g 用に
    df["ninki_pct"] = df.groupby("rid")["kijun_ninki"].rank(pct=True) if "kijun_ninki" in df else 0.5
    df = df.dropna(subset=["s_last", "idm", "q"])        # 過去走ある馬のみ（デビュー戦は除外）
    print(f"[Δab] 結合(過去走あり) {len(df):,}頭 / {df['rid'].nunique():,}レース")

    tr = df[df["year"] < args.cutoff_year]
    te = df[df["year"] >= args.cutoff_year]
    if len(tr) < 20000 or len(te) < 5000:
        print("[Δab] データ薄。", file=sys.stderr)
        return 1
    med_o = tr[have].median()
    med_s = tr[_S].median()
    print(f"[Δab] 学習<{args.cutoff_year}: {len(tr):,} / 真OOS: {len(te):,}\n")

    def _lgbm():
        return LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                              min_child_samples=200, verbose=-1)

    ytr, yte = tr["won"].to_numpy(), te["won"].to_numpy()
    lqtr = logit(np.clip(tr["q"].to_numpy(), 1e-6, 1 - 1e-6))
    lqte = logit(np.clip(te["q"].to_numpy(), 1e-6, 1 - 1e-6))

    # MySpeed: 過去 soten 群 → place を小型GBMで圧縮（fold内 <Y 学習）
    ms_model = _lgbm().fit(tr[_S].fillna(med_s).to_numpy(), ytr)
    myspeed = ms_model.predict_proba(df[_S].fillna(med_s).to_numpy())[:, 1]
    # 市場能力写像 g（fold内=<Y のみ学習）→ Δability = MySpeed − ĝ
    mktX = np.column_stack([df["idm"].fillna(df["idm"].median()).to_numpy(),
                            df["idm"].fillna(df["idm"].median()).to_numpy() ** 2,
                            np.log(df["kijun_odds"].clip(lower=1.0).fillna(10).to_numpy()),
                            df["ninki_pct"].fillna(0.5).to_numpy(),
                            df["course_len"].fillna(df["course_len"].median()).to_numpy()])
    fit_mask = (df["year"] < args.cutoff_year).to_numpy()
    df["dab"] = orth_residual(myspeed, mktX, fit_mask)
    dab_tr = df.loc[tr.index, "dab"].to_numpy()
    dab_te = df.loc[te.index, "dab"].to_numpy()

    # 3モデル
    def fit_eval(Xtr, Xte):
        m = _lgbm().fit(Xtr, ytr)
        p = m.predict_proba(Xte)[:, 1]
        return p, log_loss(yte, p), roc_auc_score(yte, p)

    Xo_tr = tr[have].fillna(med_o).to_numpy()
    Xo_te = te[have].fillna(med_o).to_numpy()
    _, ll_base, auc_base = fit_eval(lqtr.reshape(-1, 1), lqte.reshape(-1, 1))
    p_orth, ll_orth, auc_orth = fit_eval(np.column_stack([lqtr, Xo_tr]), np.column_stack([lqte, Xo_te]))
    p_od, ll_od, auc_od = fit_eval(np.column_stack([lqtr, Xo_tr, dab_tr]),
                                   np.column_stack([lqte, Xo_te, dab_te]))
    print("[Δab] 3モデル 真OOS")
    print(f"  {'model':<14}{'logloss':>10}{'AUC':>9}")
    print(f"  {'Base(q)':<14}{ll_base:>10.5f}{auc_base:>9.5f}")
    print(f"  {'Orth':<14}{ll_orth:>10.5f}{auc_orth:>9.5f}")
    print(f"  {'Orth+Δab':<14}{ll_od:>10.5f}{auc_od:>9.5f}")
    dll, dauc = ll_od - ll_orth, auc_od - auc_orth
    print(f"  → ΔlogLoss(Orth+Δ − Orth) = {dll:+.5f}（≤−0.001で本格build候補）／ ΔAUC = {dauc:+.5f}")

    # プラセボ: Δab をレース内シャッフル → 増分が消えるか
    rng = np.random.default_rng(0)
    dab_te_pl = pd.Series(dab_te, index=te["rid"].to_numpy()).groupby(level=0).transform(
        lambda s: s.to_numpy()[rng.permutation(len(s))]).to_numpy()
    _, ll_pl, _ = fit_eval(np.column_stack([lqtr, Xo_tr, dab_tr]),
                           np.column_stack([lqte, Xo_te, dab_te_pl]))
    print(f"  プラセボ(Δabシャッフル) ΔlogLoss = {ll_pl - ll_orth:+.5f}（本物なら実測より悪化）")

    # 年別 ΔlogLoss（符号一致?）
    print("\n[Δab] 年別 ΔlogLoss(Orth+Δ − Orth)")
    for y in sorted(te["year"].unique()):
        m = (te["year"] == y).to_numpy()
        if m.sum() > 500:
            print(f"  {y}: {log_loss(yte[m], p_od[m]) - log_loss(yte[m], p_orth[m]):+.5f}")

    # Δab decile 実現率（単調?）
    dec = decile_realized(dab_te, yte)
    print(f"\n[Δab] Δability decile 実現率: {np.round(dec, 4)}")
    mono = np.all(np.diff(dec) >= -0.01)
    print(f"  単調増加(概ね): {mono}")

    print("\n[Δab] 判定:")
    if dll <= -0.001 and (ll_pl - ll_orth) > dll + 0.0005 and mono:
        print("  → 本格build候補: ΔlogLoss≤−0.001・プラセボで消失・decile単調。年別符号を確認し MySpeed 本格化へ。")
    else:
        print("  → クローズ: 市場能力から直交化した過去soten集約に、既存直交特徴を超える増分は無し。")
        print("     （＝JRDB由来能力値の単純な市場直交化に追加価値なし。独自指数全般の否定ではないが、")
        print("       ペース非対称/区間ラップ/コーナーロス等を含む本格buildの費用対効果は低いと確定。）")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
