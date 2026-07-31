"""Issue #22 採用検証：featured へ配線した raw MySpeed（jrdb_ms_*）の確率品質増分を測る。

myspeed_staged_gate.py は DB＋本番予測(prod_p)依存で「素点シグナル自体」を検証した。
本スクリプトは **jrdb_build_features.py が焼き込んだ featured_jrdb.pkl だけ**を使い、
「配線した jrdb_ms_* を本番特徴量セットに足すと OOS 確率品質が改善するか」を測る
（＝Issue #22 の採用チェックリストに対応。ROI は評価しない・目的は確率品質）。

方式（leak-safe な前進分割）:
  - target = 複勝(着順≤3)。--win で単勝(着順==1)。
  - 分割 = 年 < cutoff で学習 / 年 ≥ cutoff で評価（未来を見ない）。
  - baseline = featured の数値特徴量（rank/date/単勝 と jrdb_ms_* を除く）。
    treatment = baseline + jrdb_ms_*。同じ LightGBM・同じ列順で学習/推論。
  - 指標 = logloss / AUC / ECE(10ビン)。checklist に対応した診断:
      ① ΔlogLoss / ΔAUC（treatment − baseline）  ② 年別改善数
      ③ プラセボ（jrdb_ms_* をレース内シャッフル→増分消失を確認）
      ④ 過去≥3走（jrdb_ms_npast≥3）部分集合での ΔlogLoss（薄履歴のみ依存でない）
      ⑤ ECE 悪化なし  ⑥ jrdb_ms_* の feature importance  ⑦ 列順パリティ（学習=推論）

使い方:
  python scripts/myspeed_adoption_check.py --featured data/featured_jrdb.pkl --cutoff-year 2024
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.constants._feature_cols import MYSPEED_FEATURE_COLS  # noqa: E402

# 学習ターゲット・分割キー・リーク源は特徴量から除外（src.constants._nn_cols.NN_DROP_COLS 準拠）。
_DROP_ALWAYS = ["rank", "date", "単勝", "着順", "won", "year"]


def make_target(rank: pd.Series, *, win: bool) -> np.ndarray:
    """着順 → 二値ターゲット。既定は複勝(≤3)、win=True で単勝(==1)。"""
    r = pd.to_numeric(rank, errors="coerce")
    thr = 1 if win else 3
    y = (r <= thr).astype(float)
    return y.where(r.notna(), np.nan).to_numpy()  # 着順欠損(出走取消等)は NaN で残し main で除外


def ece(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> float:
    """期待校正誤差（|平均予測−実現率| の頻度加重・myspeed_staged_gate と同定義）。"""
    b = np.clip((p * n_bins).astype(int), 0, n_bins - 1)
    e = 0.0
    for k in range(n_bins):
        mk = b == k
        if mk.any():
            e += mk.mean() * abs(p[mk].mean() - y[mk].mean())
    return float(e)


def select_feature_cols(df: pd.DataFrame, myspeed_cols: list[str]) -> list[str]:
    """数値特徴量のうち drop 対象・MySpeed 以外を baseline 特徴量として返す（列順固定）。"""
    num = df.select_dtypes(include=["number"]).columns
    drop = set(_DROP_ALWAYS) | set(myspeed_cols)
    return [c for c in num if c not in drop]


def _fit_predict(tr_x, tr_y, te_x):
    """LightGBM を学習し te の陽性確率を返す（列順は tr_x=te_x 前提）。"""
    from lightgbm import LGBMClassifier
    clf = LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                         subsample=0.8, colsample_bytree=0.8, random_state=0,
                         n_jobs=-1, verbose=-1)
    clf.fit(tr_x, tr_y)
    return clf.predict_proba(te_x)[:, 1], clf


def main(argv=None) -> int:
    from sklearn.metrics import log_loss, roc_auc_score

    ap = argparse.ArgumentParser(description="Issue #22 採用検証（raw MySpeed 確率品質増分）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl",
                    help="jrdb_build_features.py の出力（jrdb_ms_* を含む featured）")
    ap.add_argument("--cutoff-year", type=int, default=2024,
                    help="この年以降を OOS 評価に回す（未満で学習）")
    ap.add_argument("--win", action="store_true", help="単勝(着順==1)。既定は複勝(≤3)")
    args = ap.parse_args(argv)

    p = Path(args.featured)
    if not p.exists():
        print(f"featured がありません: {p}", file=sys.stderr)
        return 1
    df = pd.read_pickle(p)
    ms_cols = [c for c in MYSPEED_FEATURE_COLS if c in df.columns]
    if not ms_cols:
        print("jrdb_ms_* 列が featured にありません（配線ブランチで再生成が必要）", file=sys.stderr)
        return 1

    if "rank" not in df.columns or "date" not in df.columns:
        print("rank / date 列が featured にありません", file=sys.stderr)
        return 1
    y = make_target(df["rank"], win=args.win)
    year = pd.to_datetime(df["date"], errors="coerce").dt.year.to_numpy()
    groups = df.index.to_numpy()
    ok = np.isfinite(y) & np.isfinite(year)
    df, y, year, groups = df[ok].copy(), y[ok], year[ok], groups[ok]

    base_cols = select_feature_cols(df, ms_cols)
    trt_cols = base_cols + ms_cols                       # ⑦ 列順固定（baseline を先頭に MySpeed を後置）
    tr = year < args.cutoff_year
    te = ~tr
    label = "単勝(≤1)" if args.win else "複勝(≤3)"
    print(f"[採用検証] {label} / 特徴 baseline {len(base_cols)} + MySpeed {len(ms_cols)}"
          f" / 学習 {int(tr.sum()):,} 行(<{args.cutoff_year}) / 評価 {int(te.sum()):,} 行")
    if te.sum() < 2000 or tr.sum() < 2000:
        print("学習/評価データが薄すぎます（cutoff-year を見直してください）", file=sys.stderr)
        return 1

    yte = y[te]
    # baseline / treatment（同一 LightGBM・列順固定・NaN は LightGBM がネイティブ処理）
    p_base, _ = _fit_predict(df.loc[tr, base_cols], y[tr], df.loc[te, base_cols])
    p_trt, clf = _fit_predict(df.loc[tr, trt_cols], y[tr], df.loc[te, trt_cols])

    ll_b, ll_t = log_loss(yte, p_base), log_loss(yte, p_trt)
    auc_b, auc_t = roc_auc_score(yte, p_base), roc_auc_score(yte, p_trt)
    ece_b, ece_t = ece(p_base, yte), ece(p_trt, yte)
    print("\n[結果] OOS（未来分割）")
    print(f"  {'model':<12}{'logloss':>10}{'AUC':>9}{'ECE':>8}")
    print(f"  {'baseline':<12}{ll_b:>10.5f}{auc_b:>9.5f}{ece_b:>8.4f}")
    print(f"  {'+MySpeed':<12}{ll_t:>10.5f}{auc_t:>9.5f}{ece_t:>8.4f}")
    print(f"\n① ΔlogLoss = {ll_t - ll_b:+.5f}（負=改善・目安 ≈ −0.0036）"
          f" / ΔAUC = {auc_t - auc_b:+.5f}（正=改善・目安 ≈ +0.006）")

    # ② 年別改善
    ybits, imp, tot = [], 0, 0
    for yv in sorted(np.unique(year[te])):
        mk = year[te] == yv
        if mk.sum() > 500:
            d = log_loss(yte[mk], p_trt[mk]) - log_loss(yte[mk], p_base[mk])
            imp += int(d < 0)
            tot += 1
            ybits.append(f"{int(yv)}:{d:+.5f}")
    print(f"② 年別 {'  '.join(ybits)} → 改善 {imp}/{tot}")

    # ③ プラセボ（jrdb_ms_* をレース内シャッフル → 増分が消えれば本物）
    rng = np.random.default_rng(0)
    df_pl = df.copy()
    for c in ms_cols:
        df_pl[c] = df_pl.groupby(level=0)[c].transform(
            lambda s: s.to_numpy()[rng.permutation(len(s))])
    p_pl, _ = _fit_predict(df_pl.loc[tr, trt_cols], y[tr], df_pl.loc[te, trt_cols])
    d_pl = log_loss(yte, p_pl) - ll_b
    print(f"③ プラセボ(レース内シャッフル) ΔlogLoss = {d_pl:+.5f}"
          f"（本物なら実測 {ll_t - ll_b:+.5f} より0寄り）")

    # ④ 過去≥3走（薄履歴のみ依存でないか）
    if "jrdb_ms_npast" in df.columns:
        rich = pd.to_numeric(df.loc[te, "jrdb_ms_npast"], errors="coerce").to_numpy() >= 3
        if rich.sum() > 500:
            d_rich = log_loss(yte[rich], p_trt[rich]) - log_loss(yte[rich], p_base[rich])
            print(f"④ 過去≥3走({int(rich.sum()):,}行) ΔlogLoss = {d_rich:+.5f}")

    # ⑤⑥ 校正・importance
    print(f"⑤ ECE {ece_b:.4f}→{ece_t:.4f}（{ece_t - ece_b:+.4f}・悪化なし=非正が望ましい）")
    fi = pd.Series(clf.feature_importances_, index=trt_cols)
    print("⑥ jrdb_ms_* importance（gain 相当・全特徴中の位置）:")
    ranks = fi.rank(ascending=False).astype(int)
    for c in ms_cols:
        print(f"   {c:<16} imp={int(fi[c]):>6}  rank {int(ranks[c])}/{len(trt_cols)}")

    # ⑦ 列順パリティ
    ok_order = list(df.loc[tr, trt_cols].columns) == list(df.loc[te, trt_cols].columns) == trt_cols
    print(f"⑦ 学習=推論の列順パリティ: {'OK' if ok_order else 'NG'}")

    print("\n判定の目安: ①が負(≈−0.0036)かつ②が過半年で改善、③でほぼ消失、④でも負、"
          "⑤非悪化 → 採用可（回収率は評価対象外）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
