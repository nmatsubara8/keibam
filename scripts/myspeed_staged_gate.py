"""本格MySpeed 段階ゲート（Phase 1）— 予測モデル品質改善プロジェクト（回収率エッジ探索とは別枠）。

背景: 最終ゲート(market_ability_residual --vs-production)で確定:
  Δab(市場直交残差)は本番全モデルへ増分なし(NO-GO)。だが raw MySpeed は本番へ ΔLL=−0.00357 の増分。
  ＝過去能力は実在し本番が過小利用。市場直交は誤操作。ROIは控除未満不変(エッジではない)。
本スクリプトはその raw MySpeed を出発点に、MySpeed 自体を段階精緻化し「各段の本番への純増分」だけを測る。
回収率は改善しない前提。目的は勝率/複勝率の確率品質・能力表現の安定化（別券種/分析への再利用基盤）。

段階ラダー（累積・各段は前段の MySpeed を置換）:
  M0 本番のみ                        = [logit(prod_p)]
  M1 +raw MySpeed(素点履歴)          = [logit(prod_p), MySpeed_raw]
  M2 +距離・コース横断正規化          = [logit(prod_p), MySpeed_norm]   （素点を条件内zに置換）
  M3 +ペース非対称補正                = [logit(prod_p), MySpeed_pace]   （正規化速度＋ペース/位置履歴）
各段 te内 GroupKFold-OOF で ΔLL_k=LL(M_k)−LL(M_{k-1}) と 本番比 LL(M_k)−LL(M0) を測る。
区間ラップ/馬場内時間変化/位置取りコーナーロスは Phase1 で増分が続いた場合のみ後続（本スクリプト対象外）。

各段 継続ゲート（事前固定・ユーザ指定）:
  ① ΔLL(M_k−M_{k-1}) ≤ −0.0005
  ② 複数年で同方向（≥2/3 年で改善）
  ③ プラセボ（MySpeed_k をレース内シャッフル）で増分消失
  ④ 欠測/薄履歴馬だけに依存しない（過去走≥3頭の部分集合でも増分）
  ⑤ ROI順位付けが悪化しない（上位1%複勝ROI が前段以上・除上位5併記）
増分が止まった段で終了。ROI>1 は目的でない（確率品質の指標を見る）。

使い方:
  python scripts/myspeed_staged_gate.py --jra-only --db data/keibam.db --cutoff-year 2024
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._model_category import central_index_mask  # noqa: E402

# 素点履歴の集約列（当該走除外＝leak-safe）
_AGG = ["last", "mean3", "max5", "ewm", "trend"]
# 距離帯（条件バケット用・固定境界）
_DIST_EDGES = [0, 1200, 1400, 1600, 1800, 2000, 2200, 2600, np.inf]
_PACE_CODE = {"H": 3.0, "M": 2.0, "S": 1.0}


# ── 純ロジック（テスト対象）────────────────────────────────────────────
def build_hist(df: pd.DataFrame, value_col: str, prefix: str) -> pd.DataFrame:
    """[horse_id, rid, value_col] を時系列sortし、当該走除外の過去集約を {prefix}_* で付与。"""
    d = df.sort_values(["horse_id", "rid"]).copy()
    sh1 = d.groupby("horse_id")[value_col].shift(1)
    d[f"{prefix}_last"] = sh1
    d[f"{prefix}_mean3"] = sh1.groupby(d["horse_id"]).transform(
        lambda x: x.rolling(3, min_periods=1).mean())
    d[f"{prefix}_max5"] = sh1.groupby(d["horse_id"]).transform(
        lambda x: x.rolling(5, min_periods=1).max())
    d[f"{prefix}_ewm"] = sh1.groupby(d["horse_id"]).transform(
        lambda x: x.ewm(alpha=0.3, min_periods=1).mean())
    sh2 = d.groupby("horse_id")[value_col].shift(2)
    sh3 = d.groupby("horse_id")[value_col].shift(3)
    d[f"{prefix}_trend"] = d[f"{prefix}_last"] - (sh2 + sh3) / 2.0
    # 過去走数（当該走除外）= ④薄履歴依存チェック用
    d[f"{prefix}_npast"] = d.groupby("horse_id")[value_col].cumcount()
    return d


def dist_band(kyori: pd.Series) -> pd.Series:
    """距離を固定境界で帯化（条件バケットの一部）。"""
    return pd.cut(pd.to_numeric(kyori, errors="coerce"), bins=_DIST_EDGES,
                  labels=False, right=False)


def bucket_key(df: pd.DataFrame) -> pd.Series:
    """条件バケット鍵 = 芝ダ × 距離帯 × クラス（同素点でも分布が違う条件を吸収）。"""
    sd = df["shiba_dirt"].astype(str).str.strip()
    db = dist_band(df["kyori"]).astype("Int64").astype(str)
    cl = pd.to_numeric(df.get("class_code"), errors="coerce").fillna(-1).astype(int).astype(str)
    return sd + "|" + db + "|" + cl


def bucket_stats(values: np.ndarray, keys: np.ndarray, fit_mask: np.ndarray,
                 min_n: int = 50) -> dict:
    """fit_mask(=<Y) 行だけで バケット→(mean,std) を推定（n≥min_n のみ）。leak-safe。"""
    s = pd.DataFrame({"v": values, "k": keys})[fit_mask].dropna(subset=["v"])
    g = s.groupby("k")["v"].agg(["mean", "std", "count"])
    g = g[g["count"] >= min_n]
    return {k: (float(m), float(sd if sd and sd > 1e-6 else np.nan))
            for k, m, sd in zip(g.index, g["mean"], g["std"], strict=False)}


def condition_zscore(values: np.ndarray, keys: np.ndarray, stats: dict,
                     gmu: float, gsd: float) -> np.ndarray:
    """条件内標準化 z=(v−μ_c)/σ_c。未知/薄バケットは全体(gmu,gsd)へフォールバック。"""
    out = np.full(len(values), np.nan)
    for i, (v, k) in enumerate(zip(values, keys, strict=False)):
        if np.isnan(v):
            continue
        mu, sd = stats.get(k, (gmu, gsd))
        if not np.isfinite(sd) or sd <= 1e-6:
            mu, sd = gmu, gsd
        out[i] = (v - mu) / sd
    return out


def roi_top_pct(pay: np.ndarray, score: np.ndarray, pct: float) -> float:
    """score 上位 pct% を買った時の複勝ROI。"""
    k = max(1, int(len(score) * pct / 100.0))
    return float(pay[np.argsort(-score)[:k]].mean())


def roi_excl_top(sub_pay: np.ndarray, k: int = 5) -> float:
    """上位 k 件の払戻を除いた ROI（分母は全件）。単一高配当依存の検査。"""
    if len(sub_pay) <= k:
        return float("nan")
    return float(np.sort(sub_pay)[:-k].sum() / len(sub_pay))


def _load_sed(engine, cols):
    from sqlalchemy import text
    q = "SELECT race_id, umaban, " + ", ".join(cols) + " FROM raw_jrdb_sed"
    df = pd.read_sql(text(q), engine)
    df["rid"] = df["race_id"].astype(str).str.split(".").str[0]
    df["uma"] = pd.to_numeric(df["umaban"], errors="coerce")
    return df.dropna(subset=["uma"]).assign(uma=lambda x: x["uma"].astype(int))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="本格MySpeed 段階ゲート（Phase 1）")
    ap.add_argument("--featured-path", default=None)
    ap.add_argument("--jra-only", action="store_true")
    ap.add_argument("--db", default=None)
    ap.add_argument("--cutoff-year", type=int, default=2024)
    ap.add_argument("--prod-version", default="baseline_jrdb_seirei")
    args = ap.parse_args(argv if argv is not None else sys.argv[1:])

    from lightgbm import LGBMClassifier
    from scipy.special import logit
    from sklearn.metrics import log_loss, roc_auc_score
    from sklearn.model_selection import GroupKFold

    from app._data_loader import load_model_from_path
    from src.constants._local_paths import LocalPaths
    from src.constants._results_cols import ResultsCols
    from src.pipeline._ingestion import load_raw
    from src.pipeline.commands._evaluate import _resolve_backtest_model_path
    from src.policies._score_policy import BasicScorePolicy
    from src.storage._db import get_engine

    featured = load_raw(args.featured_path or LocalPaths.FEATURED_DATA_PATH)
    if args.jra_only:
        featured = featured[central_index_mask(featured.index)]
    rank = pd.to_numeric(featured[ResultsCols.RANK], errors="coerce")
    hid = (featured["horse_id"].astype(str) if "horse_id" in featured.columns
           else featured.index.astype(str).str[:4])
    base = pd.DataFrame({
        "rid": featured.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(featured[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "won": (rank <= 3).astype(float).to_numpy(),
        "horse_id": hid.to_numpy(),
    }).dropna(subset=["uma"])
    base["uma"] = base["uma"].astype(int)
    base["year"] = base["rid"].str[:4].astype(int)

    # SED: 素点＋条件（距離/芝ダ/クラス）＋ペース/位置（当該走の属性・履歴化して使う）
    sed_cols = ["soten", "kyori", "shiba_dirt", "class_code",
                "pace_idx", "ten_idx", "agari_idx", "uma_pace", "corner1", "toushuu",
                "fukusho_payoff"]
    eng = get_engine(args.db)
    sed = _load_sed(eng, sed_cols)
    for c in ["soten", "kyori", "class_code", "pace_idx", "ten_idx", "agari_idx",
              "corner1", "toushuu", "fukusho_payoff"]:
        sed[c] = pd.to_numeric(sed[c], errors="coerce")
    df = base.merge(sed, on=["rid", "uma"], how="inner")
    df["horse_id"] = df["horse_id"].astype(str)
    df["pay"] = (df["fukusho_payoff"] / 100.0).fillna(0.0)
    print(f"[MySpeed] 結合 {len(df):,}頭 / {df['rid'].nunique():,}レース")

    cutoff = args.cutoff_year
    fit_mask = (df["year"] < cutoff).to_numpy()
    if fit_mask.sum() < 20000:
        print("[MySpeed] 学習データ薄。", file=sys.stderr)
        return 1

    # ── 素点変換 3種（各走の速度指標）──
    # raw
    df["x_raw"] = df["soten"]
    # 距離・コース横断正規化: 条件内z（バケット統計は<cutoffのみ・未知は全体へ）
    keys = bucket_key(df).to_numpy()
    gmu = float(np.nanmean(df["soten"].to_numpy()[fit_mask]))
    gsd = float(np.nanstd(df["soten"].to_numpy()[fit_mask]))
    stats = bucket_stats(df["soten"].to_numpy(), keys, fit_mask)
    df["x_norm"] = condition_zscore(df["soten"].to_numpy(), keys, stats, gmu, gsd)
    print(f"[MySpeed] 条件バケット {len(stats):,}個(n≥50, <{cutoff}) / 全体μ={gmu:.2f} σ={gsd:.2f}")

    # ペース/位置の各走スカラー（履歴化して M3 の追加入力に）
    df["asym"] = df["agari_idx"] - df["ten_idx"]          # 差し脚 vs テン（+=末脚型）
    df["posr"] = df["corner1"] / df["toushuu"].clip(lower=1)   # 1角位置の頭数比（小=前）
    df["upace"] = df["uma_pace"].astype(str).str.strip().map(_PACE_CODE)

    # ── 履歴集約（leak-safe）──
    df = build_hist(df, "x_raw", "raw")
    df = build_hist(df, "x_norm", "norm")
    for c in ["asym", "posr", "pace_idx", "upace"]:
        df = build_hist(df, c, f"h_{c}")

    S_raw = [f"raw_{a}" for a in _AGG]
    S_norm = [f"norm_{a}" for a in _AGG]
    S_pace = S_norm + [f"h_{c}_{a}" for c in ["asym", "posr", "pace_idx", "upace"]
                       for a in ("last", "mean3")]

    df = df.dropna(subset=["raw_last"]).copy()      # 過去走ある馬（デビュー戦除外）
    tr_mask = (df["year"] < cutoff).to_numpy()
    ytr = df["won"].to_numpy()[tr_mask]
    print(f"[MySpeed] 過去走あり {len(df):,}頭 / 学習<{cutoff}: {int(tr_mask.sum()):,}\n")

    def _lgbm():
        return LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                              min_child_samples=200, verbose=-1)

    def _compress(feat_cols):
        """素点履歴群 → place を小型GBMで1スカラーに圧縮（<cutoff 学習）。"""
        med = df.loc[tr_mask, feat_cols].median()
        X = df[feat_cols].fillna(med).to_numpy()
        m = _lgbm().fit(X[tr_mask], ytr)
        return m.predict_proba(X)[:, 1]

    df["ms_raw"] = _compress(S_raw)
    df["ms_norm"] = _compress(S_norm)
    df["ms_pace"] = _compress(S_pace)

    # ── 本番全モデル place 確率（M0 baseline）──
    prod_path = _resolve_backtest_model_path(args.prod_version)
    st = load_model_from_path(prod_path).calc_score(featured, BasicScorePolicy)
    prod = pd.DataFrame({
        "rid": st.index.astype(str).str.split(".").str[0].to_numpy(),
        "uma": pd.to_numeric(st[ResultsCols.UMABAN], errors="coerce").to_numpy(),
        "prod_p": pd.to_numeric(st["score"], errors="coerce").to_numpy(),
    }).dropna(subset=["uma", "prod_p"]).drop_duplicates(["rid", "uma"])
    prod["uma"] = prod["uma"].astype(int)
    df = df.merge(prod, on=["rid", "uma"], how="left")

    te = df[(df["year"] >= cutoff) & df["prod_p"].notna()].copy()
    if len(te) < 5000:
        print(f"[MySpeed] te薄 or prod_p欠損(te={len(te)})。", file=sys.stderr)
        return 1
    print(f"[MySpeed] te(≥{cutoff}) {len(te):,}頭 / {te['rid'].nunique():,}レース"
          f" / 本番 {Path(prod_path).name}\n")

    y = te["won"].to_numpy()
    groups = te["rid"].to_numpy()
    yr = te["year"].to_numpy()
    pay = te["pay"].to_numpy()
    lp = logit(np.clip(te["prod_p"].to_numpy(), 1e-6, 1 - 1e-6))
    rich = te["raw_npast"].to_numpy() >= 3        # ④薄履歴依存チェック用（過去≥3走）

    def _oof(X):
        pred = np.zeros(len(y))
        for tri, vai in GroupKFold(n_splits=5).split(X, y, groups):
            pred[vai] = _lgbm().fit(X[tri], y[tri]).predict_proba(X[vai])[:, 1]
        return pred

    stages = [
        ("M0 本番のみ", None),
        ("M1 +raw MySpeed", "ms_raw"),
        ("M2 +距離コース正規化", "ms_norm"),
        ("M3 +ペース非対称", "ms_pace"),
    ]
    preds, lls, aucs = {}, {}, {}
    for name, col in stages:
        X = lp.reshape(-1, 1) if col is None else np.column_stack([lp, te[col].to_numpy()])
        p = _oof(X)
        preds[name], lls[name], aucs[name] = p, log_loss(y, p), roc_auc_score(y, p)

    m0 = "M0 本番のみ"
    print("[MySpeed] 段階 te内 GroupKFold-OOF")
    print(f"  {'stage':<22}{'logloss':>10}{'AUC':>9}{'ΔLL vs M0':>12}{'ΔLL vs 前段':>13}")
    prev = m0
    order = [s[0] for s in stages]
    for name in order:
        d_m0 = lls[name] - lls[m0]
        d_pr = lls[name] - lls[prev]
        print(f"  {name:<22}{lls[name]:>10.5f}{aucs[name]:>9.5f}"
              f"{d_m0:>+12.5f}{d_pr:>+13.5f}")
        prev = name

    # ── 各段の継続ゲート（前段比の増分を検証）──
    rng = np.random.default_rng(0)
    for i in range(1, len(order)):
        name, prev = order[i], order[i - 1]
        col = stages[i][1]
        d_pr = lls[name] - lls[prev]
        p_k, p_prev = preds[name], preds[prev]
        print(f"\n[ゲート] {name}（vs {prev}）")
        print(f"  ① ΔLL(前段比) = {d_pr:+.5f}（≤−0.0005で継続）")
        # ② 年別 同方向
        yr_imp, yr_tot = 0, 0
        ybits = []
        for yv in sorted(np.unique(yr)):
            mk = yr == yv
            if mk.sum() > 500:
                dy = log_loss(y[mk], p_k[mk]) - log_loss(y[mk], p_prev[mk])
                yr_imp += int(dy < 0)
                yr_tot += 1
                ybits.append(f"{int(yv)}:{dy:+.5f}")
        print(f"  ② 年別 {'  '.join(ybits)} → 改善 {yr_imp}/{yr_tot}")
        # ③ プラセボ（当段 MySpeed をレース内シャッフル→前段特徴に載せ替え）
        ms_pl = pd.Series(te[col].to_numpy(), index=groups).groupby(level=0).transform(
            lambda s: s.to_numpy()[rng.permutation(len(s))]).to_numpy()
        p_pl = _oof(np.column_stack([lp, ms_pl]))
        d_pl = log_loss(y, p_pl) - lls[prev]
        print(f"  ③ プラセボ ΔLL(前段比) = {d_pl:+.5f}（本物なら実測 {d_pr:+.5f} より悪化＝0寄り）")
        # ④ 薄履歴依存でない（過去≥3走の部分集合でも増分）
        d_rich = log_loss(y[rich], p_k[rich]) - log_loss(y[rich], p_prev[rich])
        print(f"  ④ 過去≥3走({int(rich.sum()):,}頭) ΔLL = {d_rich:+.5f}（薄履歴のみ依存でないか）")
        # ⑤ ROI順位付け（悪化しないか）＋除上位5
        print(f"  ⑤ 複勝ROI上位x%  {'前段':>9}{'当段':>9}{'差':>9}{'当段除上5':>11}")
        roi_ok = True
        for pct in (1.0, 2.0, 5.0):
            r_prev = roi_top_pct(pay, p_prev, pct)
            r_k = roi_top_pct(pay, p_k, pct)
            k = max(1, int(len(te) * pct / 100.0))
            excl = roi_excl_top(pay[np.argsort(-p_k)[:k]], k=5)
            if pct == 1.0 and r_k < r_prev - 1e-9:
                roi_ok = False
            print(f"     {pct:>4.0f}%       {r_prev:>9.4f}{r_k:>9.4f}{r_k - r_prev:>+9.4f}{excl:>11.4f}")
        # 判定
        placebo_dies = d_pl > d_pr + 1e-9
        cont = (d_pr <= -0.0005 and yr_imp >= max(2, yr_tot - 1)
                and placebo_dies and d_rich < 0 and roi_ok)
        print(f"  → {'継続' if cont else '停止'}: "
              + ("この段の精緻化は本番へ純増分あり（次段へ）。" if cont
                 else "純増分が閾値未満/年不一致/プラセボと同等/薄履歴依存/ROI悪化 のいずれか。ここで打ち切り。"))

    print("\n[MySpeed] Phase1 総括: M0→M3 の各 ΔLL と継続判定を上に列挙。継続が M3 まで続けば"
          " 区間ラップ/馬場内時間変化/コーナーロスへ（別段階）。止まった段の直前が現時点の最良MySpeed。")
    print("  ※ 目的は確率品質（勝率/複勝率・校正・能力表現）。ROIは控除未満不変＝回収率エッジではない。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
