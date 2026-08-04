"""本格MySpeed 段階ゲート（Phase 1）— 予測モデル品質改善プロジェクト（回収率エッジ探索とは別枠）。

背景: 最終ゲート(market_ability_residual --vs-production)で確定:
  Δab(市場直交残差)は本番全モデルへ増分なし(NO-GO)。だが raw MySpeed は本番へ ΔLL=−0.00357 の増分。
  ＝過去能力は実在し本番が過小利用。市場直交は誤操作。ROIは控除未満不変(エッジではない)。
本スクリプトはその raw MySpeed を出発点に、MySpeed 自体を段階精緻化し「各段の本番への純増分」だけを測る。
回収率は改善しない前提。目的は勝率/複勝率の確率品質・能力表現の安定化（別券種/分析への再利用基盤）。

分岐設計（累積一本線でなく M1 からの分岐＝帰属を分離）:
  M0 本番のみ    = [logit(prod_p)]
  M1 +raw        = [logit(prod_p), MySpeed_raw]                       （素点履歴を圧縮）
  M2n M1+正規化  = [logit(prod_p), MySpeed_raw, MySpeed_norm]         （条件内z履歴を別軸で追加）
  M2p M1+ペース  = [logit(prod_p), MySpeed_raw, MySpeed_pace]         （ペース/位置履歴を別軸で追加）★最重要
  M2np M1+両方   = [logit(prod_p), MySpeed_raw, MySpeed_norm, MySpeed_pace]
最重要比較 = LL(M2p) − LL(M1)：ペース非対称が raw MySpeed の上に直接 純増分を足すか。
（累積置換だと正規化の水準破壊が後段へ伝播し帰属が混ざる。分岐なら各精緻化の純増分を分離できる。）
区間ラップ/馬場内時間変化/位置取りコーナーロスは M2p が採用候補になった場合のみ後続（本スクリプト対象外）。

M2p 採用ゲート（確率品質・事前固定）:
  ① ΔLL(vs M1) ≤ −0.0005  ② ≥2/3 年で改善  ③ プラセボ(ペース軸シャッフル)で消失
  ④ 過去走≥3頭の部分集合でも改善（薄履歴依存でない）  ⑤ AUC・ECE(校正)が悪化しない
ROI は hard gate から外しガードレール（複数帯で極端悪化しない・除上位5で崩れない・単一1%帯で採否を決めない）。
目的はモデル品質（ROI最適化でない）。ROI>1 は要求しない。

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

    # 独立した3軸を各々スカラーに圧縮し、M1(=本番+raw)へ「別軸として追加」する分岐設計。
    #   累積置換だと正規化の水準破壊が後段へ伝播し帰属が混ざる。分岐なら各精緻化の純増分を分離できる。
    S_raw = [f"raw_{a}" for a in _AGG]
    S_norm = [f"norm_{a}" for a in _AGG]                 # 条件内z 履歴（単独圧縮）
    S_pace = [f"h_{c}_{a}" for c in ["asym", "posr", "pace_idx", "upace"]
              for a in ("last", "mean3")]                # ペース/位置 履歴（単独圧縮）

    df = df.dropna(subset=["raw_last"]).copy()      # 過去走ある馬（デビュー戦除外）
    tr_mask = (df["year"] < cutoff).to_numpy()
    ytr = df["won"].to_numpy()[tr_mask]
    print(f"[MySpeed] 過去走あり {len(df):,}頭 / 学習<{cutoff}: {int(tr_mask.sum()):,}\n")

    def _lgbm():
        return LGBMClassifier(n_estimators=300, num_leaves=31, learning_rate=0.05,
                              min_child_samples=200, verbose=-1)

    def _compress(feat_cols):
        """履歴群 → place を小型GBMで1スカラーに圧縮（<cutoff 学習）。"""
        med = df.loc[tr_mask, feat_cols].median()
        X = df[feat_cols].fillna(med).to_numpy()
        m = _lgbm().fit(X[tr_mask], ytr)
        return m.predict_proba(X)[:, 1]

    df["ms_raw"] = _compress(S_raw)     # raw MySpeed（M1 の追加軸）
    df["ms_norm"] = _compress(S_norm)   # 条件正規化 MySpeed（M2n の追加軸）
    df["ms_pace"] = _compress(S_pace)   # ペース非対称 MySpeed（M2p の追加軸・最重要）

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

    def _oof(cols):
        X = np.column_stack([lp] + [te[c].to_numpy() for c in cols]) if cols else lp.reshape(-1, 1)
        pred = np.zeros(len(y))
        for tri, vai in GroupKFold(n_splits=5).split(X, y, groups):
            pred[vai] = _lgbm().fit(X[tri], y[tri]).predict_proba(X[vai])[:, 1]
        return pred

    def _ece(p, n=10):
        """期待校正誤差（10ビン・|平均予測−実現率|の頻度加重平均）。校正の悪化検査用。"""
        b = np.clip((p * n).astype(int), 0, n - 1)
        e = 0.0
        for k in range(n):
            mk = b == k
            if mk.any():
                e += mk.mean() * abs(p[mk].mean() - y[mk].mean())
        return e

    # ── 分岐設計（累積一本線でなく M1 からの分岐）── ユーザ指定の4モデル ──
    #   M0 本番のみ / M1 +raw / M2n M1+条件正規化 / M2p M1+ペース非対称 / M2np M1+両方
    #   最重要比較 = LL(M2p) − LL(M1)（ペース非対称が raw MySpeed の上に直接足すか）
    models = {
        "M0 本番のみ": [],
        "M1 +raw": ["ms_raw"],
        "M2n M1+正規化": ["ms_raw", "ms_norm"],
        "M2p M1+ペース": ["ms_raw", "ms_pace"],
        "M2np M1+両方": ["ms_raw", "ms_norm", "ms_pace"],
    }
    preds, lls, aucs, eces = {}, {}, {}, {}
    for name, cols in models.items():
        p = _oof(cols)
        preds[name] = p
        lls[name], aucs[name], eces[name] = log_loss(y, p), roc_auc_score(y, p), _ece(p)

    m0, m1 = "M0 本番のみ", "M1 +raw"
    print("[MySpeed] 分岐 te内 GroupKFold-OOF")
    print(f"  {'model':<16}{'logloss':>10}{'AUC':>9}{'ECE':>8}{'ΔLL vs M1':>12}{'ΔLL vs M0':>12}")
    for name in models:
        print(f"  {name:<16}{lls[name]:>10.5f}{aucs[name]:>9.5f}{eces[name]:>8.4f}"
              f"{lls[name] - lls[m1]:>+12.5f}{lls[name] - lls[m0]:>+12.5f}")

    # ── M2p（最重要）を確率品質ゲートで判定。ROI はガードレール（hard gate から除外）──
    rng = np.random.default_rng(0)

    def _eval_vs_m1(name, add_col):
        p_k, base = preds[name], preds[m1]
        d = lls[name] - lls[m1]
        print(f"\n[ゲート] {name}（vs M1 +raw・確率品質）")
        print(f"  ① ΔLL(vs M1) = {d:+.5f}（≤−0.0005 で採用候補）")
        yr_imp, yr_tot, ybits = 0, 0, []
        for yv in sorted(np.unique(yr)):
            mk = yr == yv
            if mk.sum() > 500:
                dy = log_loss(y[mk], p_k[mk]) - log_loss(y[mk], base[mk])
                yr_imp += int(dy < 0)
                yr_tot += 1
                ybits.append(f"{int(yv)}:{dy:+.5f}")
        print(f"  ② 年別 {'  '.join(ybits)} → 改善 {yr_imp}/{yr_tot}")
        ms_pl = pd.Series(te[add_col].to_numpy(), index=groups).groupby(level=0).transform(
            lambda s: s.to_numpy()[rng.permutation(len(s))]).to_numpy()
        te_pl = te.assign(**{add_col: ms_pl})
        X_pl = np.column_stack([lp] + [te_pl[c].to_numpy() for c in models[name]])
        p_pl = np.zeros(len(y))
        for tri, vai in GroupKFold(n_splits=5).split(X_pl, y, groups):
            p_pl[vai] = _lgbm().fit(X_pl[tri], y[tri]).predict_proba(X_pl[vai])[:, 1]
        d_pl = log_loss(y, p_pl) - lls[m1]
        print(f"  ③ プラセボ({add_col}レース内シャッフル) ΔLL(vs M1) = {d_pl:+.5f}"
              f"（本物なら実測 {d:+.5f} より0寄り）")
        d_rich = log_loss(y[rich], p_k[rich]) - log_loss(y[rich], base[rich])
        print(f"  ④ 過去≥3走({int(rich.sum()):,}頭) ΔLL(vs M1) = {d_rich:+.5f}（薄履歴のみ依存でないか）")
        d_auc = aucs[name] - aucs[m1]
        d_ece = eces[name] - eces[m1]
        print(f"  ⑤ AUC {aucs[m1]:.5f}→{aucs[name]:.5f}({d_auc:+.5f}) / "
              f"ECE {eces[m1]:.4f}→{eces[name]:.4f}({d_ece:+.4f})（悪化しないか）")
        # ROI はガードレール（採否を決めない・複数帯の極端悪化と除上位5崩壊のみ監視）
        print("  [ガードレール] 複勝ROI上位x%（採否非依存・単一帯で判断しない）")
        print(f"    {'上位':>6}{'M1':>9}{name.split()[0]:>9}{'差':>9}{'除上5':>10}")
        worse_bands = 0
        for pct in (1.0, 2.0, 5.0):
            r0 = roi_top_pct(pay, base, pct)
            r1 = roi_top_pct(pay, p_k, pct)
            k = max(1, int(len(te) * pct / 100.0))
            excl = roi_excl_top(pay[np.argsort(-p_k)[:k]], k=5)
            worse_bands += int(r1 < r0 - 0.01)     # 1pt超の悪化のみカウント
            print(f"    {pct:>5.0f}%{r0:>9.4f}{r1:>9.4f}{r1 - r0:>+9.4f}{excl:>10.4f}")
        guard_ok = worse_bands < 2                 # 複数帯で極端悪化しない
        # 確率品質ゲート（ROI は hard gate に含めない）
        adopt = (d <= -0.0005 and yr_imp >= 2 and d_pl > d + 1e-9
                 and d_rich < 0 and d_auc >= 0 and d_ece <= 1e-4)
        print(f"  → {'採用候補' if adopt else '非採用'}: "
              + ("ペース非対称は raw MySpeed の上に純増分あり。" if adopt and "ペース" in name
                 else "純増分＋年一致＋プラセボ死＋薄履歴非依存＋AUC/ECE非悪化 を満たす。" if adopt
                 else "確率品質ゲートのいずれか未達＝raw への純増分は未確認。")
              + (f"（ROIガードレール: {'OK' if guard_ok else '複数帯悪化に注意'}）"))
        return adopt

    # 帰属確認: 正規化単独 / ペース単独（最重要）/ 両方
    adopt_norm = _eval_vs_m1("M2n M1+正規化", "ms_norm")
    adopt_pace = _eval_vs_m1("M2p M1+ペース", "ms_pace")
    _eval_vs_m1("M2np M1+両方", "ms_pace")

    print("\n[MySpeed] Phase1 分岐 総括:")
    print(f"  正規化(M2n) vs M1: {'採用候補' if adopt_norm else '非採用（z-score置換版は破棄）'}")
    print(f"  ペース非対称(M2p) vs M1【最重要】: {'採用候補' if adopt_pace else '非採用'}")
    if adopt_pace:
        print("  → ペース非対称は raw への純増分を確認。次の高コスト段=区間ラップ(ペース表現の精緻化)へ進む価値。")
        print("     優先: 1)区間ラップ 2)馬場内時間変化 3)位置取り・コーナーロス。")
    else:
        print("  → M3−M2 の改善は正規化の誤差修復/相互作用だった公算。現時点の最良は M1 raw MySpeed。")
        print("     区間ラップ以降へは進まない判断が妥当。Phase1 は raw MySpeed 採用で終了。")
    print("  ※ 目的は確率品質（勝率/複勝率・校正・能力表現）。ROIは控除未満不変＝回収率エッジではない。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
