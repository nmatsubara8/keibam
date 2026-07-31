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
    """[horse_id, rid(時系列sort可), soten] → 当該走除外の過去集約（leak-safe）を付与して返す。"""
    d = df.sort_values(["horse_id", "rid"]).copy()
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


def roi_top_pct(pay: np.ndarray, score: np.ndarray, pct: float) -> float:
    """score 上位 pct% を買った時の複勝ROI（払戻/1）。"""
    k = max(1, int(len(score) * pct / 100.0))
    return float(pay[np.argsort(-score)[:k]].mean())


def roi_excl_top(sub_pay: np.ndarray, k: int = 5) -> float:
    """上位 k 件の払戻を除いた ROI（単一高配当依存の検査）。分母は全件・除外は分子のみ。"""
    if len(sub_pay) <= k:
        return float("nan")
    return float(np.sort(sub_pay)[:-k].sum() / len(sub_pay))


def _load_col(engine, table, col):
    from sqlalchemy import text
    df = pd.read_sql(text(f"SELECT race_id, umaban, {col} FROM {table}"), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))[["rid", "uma", col]]


def _vs_production(df: pd.DataFrame, featured: pd.DataFrame, args) -> int:
    """最終ゲート: 本番全モデル(baseline_jrdb_seirei ~600特徴)に対する Δab/raw の増分を te内CVで測る。

    A. 本番baseline のみ          = [logit(prod_p)]
    B. 本番baseline + raw MySpeed = [logit(prod_p), myspeed]   （過去能力を直交せず素で追加）
    C. 本番baseline + Δability    = [logit(prod_p), dab]       （市場直交した能力残差）
    3モデルとも te(2024+) 内 GroupKFold-OOF で比較。prod_p / myspeed / dab は全て <cutoff で
    学習済みの写像から生成済み（leak-safe）。本番予測が te で in-sample の懸念があっても、
    それは A を過大評価＝Δab増分を過小評価する保守方向なので、GO判定は安全側に出る。
    """
    from lightgbm import LGBMClassifier
    from scipy.special import logit
    from sklearn.metrics import log_loss, roc_auc_score
    from sklearn.model_selection import GroupKFold

    from app._data_loader import load_model_from_path
    from src.constants._results_cols import ResultsCols
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.policies._score_policy import BasicScorePolicy

    # 本番全モデルの place 確率を featured 全体で計算し (rid,uma) で te に結合。
    prod_path = _resolve_backtest_model_path(args.prod_version)
    prod_ai = load_model_from_path(prod_path)
    st = prod_ai.calc_score(featured, BasicScorePolicy)
    prod = pd.DataFrame({
        "rid": st.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(st[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "prod_p": pd.to_numeric(st["score"], errors="coerce").to_numpy(),
    }).dropna(subset=["uma", "prod_p"])
    prod["uma"] = prod["uma"].astype(int)
    prod = prod.drop_duplicates(["rid", "uma"])
    print(f"[vs本番] 本番モデル {Path(prod_path).name} place確率 {len(prod):,}頭")

    g = df.merge(prod, on=["rid", "uma"], how="left")
    te = g[(g["year"] >= args.cutoff_year) & g["prod_p"].notna()].copy()
    if len(te) < 5000:
        print(f"[vs本番] te薄 or prod_p欠損多（te={len(te)}）。", file=sys.stderr)
        return 1
    print(f"[vs本番] te(≥{args.cutoff_year}, prod_p有) {len(te):,}頭 / {te['rid'].nunique():,}レース\n")

    y = te["won"].to_numpy()
    groups = te["rid"].to_numpy()
    lp = logit(np.clip(te["prod_p"].to_numpy(), 1e-6, 1 - 1e-6))
    ms = te["myspeed"].to_numpy()
    dab = te["dab"].to_numpy()
    pay = te["pay"].to_numpy()
    yr = te["year"].to_numpy()

    def _lgbm():
        return LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                              min_child_samples=200, verbose=-1)

    def _oof(X):
        pred = np.zeros(len(y))
        for tri, vai in GroupKFold(n_splits=5).split(X, y, groups):
            pred[vai] = _lgbm().fit(X[tri], y[tri]).predict_proba(X[vai])[:, 1]
        return pred

    XA = lp.reshape(-1, 1)
    XB = np.column_stack([lp, ms])
    XC = np.column_stack([lp, dab])
    pA, pB, pC = _oof(XA), _oof(XB), _oof(XC)
    llA, llB, llC = log_loss(y, pA), log_loss(y, pB), log_loss(y, pC)
    aA, aB, aC = roc_auc_score(y, pA), roc_auc_score(y, pB), roc_auc_score(y, pC)
    d_ab, d_raw, d_specific = llC - llA, llB - llA, llC - llB

    print("[vs本番] te内 GroupKFold-OOF（本番全モデルへの増分）")
    print(f"  {'model':<26}{'logloss':>10}{'AUC':>9}")
    print(f"  {'A: 本番のみ':<24}{llA:>10.5f}{aA:>9.5f}")
    print(f"  {'B: 本番+raw MySpeed':<22}{llB:>10.5f}{aB:>9.5f}")
    print(f"  {'C: 本番+Δability':<23}{llC:>10.5f}{aC:>9.5f}")
    print(f"  → ΔlogLoss(C−A, Δab増分) = {d_ab:+.5f}（GO閾値 ≤−0.0005／強GO ≤−0.001）")
    print(f"    ΔlogLoss(B−A, raw増分) = {d_raw:+.5f}／ΔAUC(C−A) = {aC - aA:+.5f}")
    print(f"    Δab固有(C−B, 直交の価値) = {d_specific:+.5f}（負なら素の追加を上回る）")

    # プラセボ: Δab をレース内シャッフル → 増分が消えるか
    rng = np.random.default_rng(0)
    dab_pl = pd.Series(dab, index=groups).groupby(level=0).transform(
        lambda s: s.to_numpy()[rng.permutation(len(s))]).to_numpy()
    pC_pl = _oof(np.column_stack([lp, dab_pl]))
    ll_pl = log_loss(y, pC_pl)
    print(f"  プラセボ(Δabレース内シャッフル) ΔlogLoss(C'−A) = {ll_pl - llA:+.5f}（本物なら実測より悪化）")

    # 年別 ΔlogLoss(C−A)（≥2/3 年で改善?）
    print("\n[vs本番] 年別 ΔlogLoss(C−A)")
    yr_improve = 0
    yr_total = 0
    for yv in sorted(np.unique(yr)):
        mk = yr == yv
        if mk.sum() > 500:
            dy = log_loss(y[mk], pC[mk]) - log_loss(y[mk], pA[mk])
            yr_improve += int(dy < 0)
            yr_total += 1
            print(f"  {int(yv)}: {dy:+.5f}")

    # ROI 上位x%（A vs C）＋ 除上位5（単一高配当依存の検査）
    print("\n[vs本番] 複勝ROI 上位x%（A:本番のみ vs C:本番+Δab）")
    print(f"  {'上位':>6}{'A':>9}{'C':>9}{'差':>9}{'C除上5':>10}")
    roi_bands_improve = 0
    for pct in (1.0, 2.0, 5.0, 10.0):
        r_a = roi_top_pct(pay, pA, pct)
        r_c = roi_top_pct(pay, pC, pct)
        k = max(1, int(len(te) * pct / 100.0))
        c_excl = roi_excl_top(pay[np.argsort(-pC)[:k]], k=5)
        roi_bands_improve += int(r_c > r_a)
        print(f"  {pct:>5.0f}%{r_a:>9.4f}{r_c:>9.4f}{r_c - r_a:>+9.4f}{c_excl:>10.4f}")

    # Go/No-Go（ユーザ事前固定基準）
    placebo_dies = (ll_pl - llA) > d_ab + 1e-9      # プラセボは実測より悪い
    go = (d_ab <= -0.0005 and yr_improve >= 2 and (aC - aA) >= 0
          and placebo_dies and roi_bands_improve >= 3)
    strong = d_ab <= -0.001 and d_specific < 0
    print("\n[vs本番] 最終Go/No-Go（本番全特徴に対する Δab の増分）:")
    print(f"  ΔLL(C−A)≤−0.0005: {d_ab <= -0.0005} / 年別≥2改善: {yr_improve}/{yr_total}"
          f" / ΔAUC≥0: {(aC - aA) >= 0} / プラセボ死: {placebo_dies} / ROI≥3帯改善: {roi_bands_improve}/4")
    print(f"  Δab固有(C<B): {d_specific < 0}（直交が素の追加を上回るか）")
    if go and strong:
        print("  → 強GO: 本番全特徴に対しても Δab は独立増分を持つ。本格MySpeed build へ進む価値。")
    elif go:
        print("  → GO: 本番全特徴に対し増分あり（弱）。本格buildの期待値はROI除上5の依存度で調整。")
    else:
        print("  → NO-GO: 簡易baselineでの増分は本番全特徴（豊富な過去成績）と重複。")
        print("     『Δabは簡易市場モデルに欠けた過去能力を効率表現するが、本番モデルへの追加情報は限定的』"
              "として一区切り。")
    print("  ※ context記録: 「GO（本番全特徴との重複"
          + ("否定＝独立増分確認" if go else "＝増分無し確認") + "）」")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Δ_ability ゲート実験（一回限り）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024)
    ap.add_argument("--vs-production", action="store_true",
                    help="最終ゲート: 本番全モデル(baseline_jrdb_seirei)に対する Δab/raw の増分を te内CVで測る")
    ap.add_argument("--prod-version", default="baseline_jrdb_seirei")
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
    df["myspeed"] = myspeed
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

    if args.vs_production:
        return _vs_production(df, featured, args)

    # 3モデル
    def fit_eval(Xtr, Xte):
        m = _lgbm().fit(Xtr, ytr)
        p = m.predict_proba(Xte)[:, 1]
        return m, p, log_loss(yte, p), roc_auc_score(yte, p)

    Xo_tr = tr[have].fillna(med_o).to_numpy()
    Xo_te = te[have].fillna(med_o).to_numpy()
    _, _, ll_base, auc_base = fit_eval(lqtr.reshape(-1, 1), lqte.reshape(-1, 1))
    _, p_orth, ll_orth, auc_orth = fit_eval(np.column_stack([lqtr, Xo_tr]), np.column_stack([lqte, Xo_te]))
    m_od, p_od, ll_od, auc_od = fit_eval(np.column_stack([lqtr, Xo_tr, dab_tr]),
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
    _, _, ll_pl, _ = fit_eval(np.column_stack([lqtr, Xo_tr, dab_tr]),
                              np.column_stack([lqte, Xo_te, dab_te_pl]))
    print(f"  プラセボ(Δabシャッフル) ΔlogLoss = {ll_pl - ll_orth:+.5f}（本物なら実測より悪化）")

    # 年別 ΔlogLoss（符号一致?）
    print("\n[Δab] 年別 ΔlogLoss(Orth+Δ − Orth)")
    for y in sorted(te["year"].unique()):
        m = (te["year"] == y).to_numpy()
        if m.sum() > 500:
            print(f"  {y}: {log_loss(yte[m], p_od[m]) - log_loss(yte[m], p_orth[m]):+.5f}")

    # ── 最終ゲート3確認（Go/No-Go・事前固定・ROI>1は非要求）──────────────
    # ① 独立性: Δab を te でシャッフルした時の logloss 悪化(=permutation import)＋LGBM gain 順位
    rng2 = np.random.default_rng(1)
    Xte_od = np.column_stack([lqte, Xo_te, dab_te])
    Xte_perm = Xte_od.copy()
    Xte_perm[:, -1] = dab_te[rng2.permutation(len(dab_te))]
    ll_perm = log_loss(yte, m_od.predict_proba(Xte_perm)[:, 1])
    gains = m_od.feature_importances_
    dab_rank = int((gains > gains[-1]).sum()) + 1
    print("\n[確認①独立性] 既存q+orth があっても Δab が寄与するか")
    print(f"  ablation ΔlogLoss(Orth+Δ − Orth) = {dll:+.5f}")
    print(f"  permutation importance(Δab全体シャッフル) logloss悪化 = {ll_perm - ll_od:+.5f}（正で寄与）")
    print(f"  LGBM gain 順位: Δab は全{len(gains)}特徴中 {dab_rank}位（q・orth含む）")

    # ② ROI 順位付け: Orth vs Orth+Δab の複勝ROI 上位x%（ROI>1は非要求・改善方向を見る）
    pay_te = te["pay"].to_numpy()
    print("\n[確認②ROI順位付け] 予測place確率 上位x% の複勝ROI（Orth vs Orth+Δab）")
    print(f"  {'上位':>6}{'Orth':>9}{'Orth+Δab':>10}{'差':>9}")
    for pct in (1.0, 2.0, 5.0, 10.0):
        k = max(1, int(len(te) * pct / 100))
        r_o = float(pay_te[np.argsort(-p_orth)[:k]].mean())
        r_d = float(pay_te[np.argsort(-p_od)[:k]].mean())
        print(f"  {pct:>5.0f}%{r_o:>9.4f}{r_d:>10.4f}{r_d - r_o:>+9.4f}")

    # ③ 効果形状: Orth が取り残した残差(won−p_orth)を Δab decile で平均（どこで補正するか）
    resid = yte - p_orth
    b = pd.qcut(pd.Series(dab_te).rank(method="first"), 10, labels=False)
    shape = pd.Series(resid).groupby(b).mean().to_numpy()
    dec = decile_realized(dab_te, yte)
    print("\n[確認③効果形状] Δab decile ごとの Orth残差平均(won−p_orth)＝Δabが補正する向き")
    print(f"  decile実現率 : {np.round(dec, 4)}")
    print(f"  Orth残差平均 : {np.round(shape, 4)}（+ =Orthが過小評価→Δabが上げる）")
    lo, mid, hi = shape[:3].mean(), shape[3:7].mean(), shape[7:].mean()
    form = ("両端(U字)" if lo > 0 and hi > 0 and mid < min(lo, hi) else
            "単調" if np.all(np.diff(shape) >= -0.01) else "条件付き/非単調")
    print(f"  形状: 下位平均{lo:+.4f} / 中央{mid:+.4f} / 上位{hi:+.4f} → {form}")

    # Go/No-Go（3条件・decile単調は hard gate にしない＝残差特徴には不適切）
    indep = (dll <= -0.001) and (ll_perm - ll_od > 0.0005)
    roi_dir = float(pay_te[np.argsort(-p_od)[:max(1, len(te) // 100)]].mean()) >= \
        float(pay_te[np.argsort(-p_orth)[:max(1, len(te) // 100)]].mean())
    print("\n[Δab] Go/No-Go 判定（年別は上で全年マイナスを確認）:")
    if indep and roi_dir:
        print("  → GO: ΔlogLoss再現(≤−0.001)・Δab独立寄与(permで悪化)・ROI順位付けも改善方向。")
        print("     本格MySpeed(区間ラップ/ペース非対称/コーナーロス)へ進む価値あり。効果形状が指針。")
    else:
        print("  → NO-GO: 確率改善は確認できたが、独立寄与またはROI順位付けが崩れた。")
        print("     『確率推定は改善したが投資戦略としての実用価値は限定的』として一区切り。")
    print("  ※ ROI>1 はこの段階の判定条件にしない（確率改善が選択へ伝播するかを見る）。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
