"""Issue #22 採用検証：featured へ配線した raw MySpeed（jrdb_ms_*）の確率品質増分を測る。

myspeed_staged_gate.py は DB＋本番予測(prod_p)依存で「素点シグナル自体」を検証した。
本スクリプトは **jrdb_build_features.py が焼き込んだ featured_jrdb.pkl だけ**を使い、
「配線した jrdb_ms_* を本番特徴量セットに足すと OOS 確率品質が改善するか」を測る
（＝Issue #22 の採用チェックリストに対応。ROI は評価しない・目的は確率品質）。

方式（leak-safe な前進分割）:
  - target = 複勝(rank=着順<4)。--win で単勝(rank_win=1着)。どちらも featured の二値目的変数を直用。
  - 特徴量 = featured の数値列から目的変数・ID・事後情報を除外（src.training._residual_head の
    _ALWAYS_DROP に準拠）。baseline は jrdb_ms_* も除外、treatment は含める。
  - 分割 = 年 < cutoff で学習 / 年 ≥ cutoff で評価（未来を見ない）。同一 LightGBM・同一列順。
  - 指標 = logloss / AUC / ECE(10ビン)。checklist 対応の診断:
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

# 目的変数・ID・事後情報（決して特徴量にしない）。本番 src.training._data_splitter._DROP_FOR_TRAIN
# に準拠（rank/rank_win/date/horse_id/着順/通過 と、市場オッズ 単勝）。
# 単勝(市場オッズ)は本番が学習から落とす（モデルに市場の写経をさせない）。既定で除外し、
# baseline を本番と同条件にする。--keep-odds で残せる（市場込み baseline との比較用）。
_ALWAYS_DROP = ["着順", "rank", "rank_win", "date", "horse_id", "race_id", "通過", "単勝"]
_TARGET_COL = {"place": "rank", "win": "rank_win"}


def target_series(df: pd.DataFrame, *, win: bool) -> np.ndarray:
    """featured の二値目的変数を返す（複勝=rank / 単勝=rank_win）。欠損は NaN で残す。"""
    col = _TARGET_COL["win" if win else "place"]
    if col not in df.columns:
        raise KeyError(col)
    return pd.to_numeric(df[col], errors="coerce").to_numpy()


def ece(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> float:
    """期待校正誤差（|平均予測−実現率| の頻度加重・myspeed_staged_gate と同定義）。"""
    b = np.clip((p * n_bins).astype(int), 0, n_bins - 1)
    e = 0.0
    for k in range(n_bins):
        mk = b == k
        if mk.any():
            e += mk.mean() * abs(p[mk].mean() - y[mk].mean())
    return float(e)


def select_feature_cols(df: pd.DataFrame, *, drop_prefixes: tuple[str, ...] = (),
                        keep_odds: bool = False) -> list[str]:
    """数値特徴量のうち目的変数・ID・事後情報・drop_prefixes を除いた列（df の列順を保存）。

    keep_odds=True で市場オッズ '単勝' を特徴量に残す（本番は既定で除外＝False）。
    """
    drop = set(_ALWAYS_DROP)
    if keep_odds:
        drop.discard("単勝")
    cols = []
    for c in df.columns:
        cs = str(c)
        if c in drop or any(cs.startswith(p) for p in drop_prefixes):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            cols.append(c)
    return cols


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

    def _ll(y_true, p):  # 薄いスライスで単一クラスでも落ちないよう labels を明示
        return log_loss(y_true, p, labels=[0, 1])

    ap = argparse.ArgumentParser(description="Issue #22 採用検証（raw MySpeed 確率品質増分）")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl",
                    help="jrdb_build_features.py の出力（jrdb_ms_* を含む featured）")
    ap.add_argument("--cutoff-year", type=int, default=2024,
                    help="この年以降を OOS 評価に回す（未満で学習）")
    ap.add_argument("--win", action="store_true", help="単勝(rank_win)。既定は複勝(rank)")
    ap.add_argument("--keep-odds", action="store_true",
                    help="市場オッズ 単勝 を特徴量に残す（本番は既定で除外）。市場込み baseline 比較用")
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
    try:
        y = target_series(df, win=args.win)
    except KeyError as e:
        print(f"目的変数列 {e} が featured にありません", file=sys.stderr)
        return 1
    if "date" not in df.columns:
        print("date 列が featured にありません", file=sys.stderr)
        return 1

    # race_id はレース内プラセボ用のグルーピング鍵。featured では index=race_id（列でない）
    # ことがあるため、列があれば列を、無ければ index を採用してから reset する。
    race = (df["race_id"] if "race_id" in df.columns else pd.Series(df.index)).to_numpy()
    year = pd.to_datetime(df["date"], errors="coerce").dt.year.to_numpy()
    ok = np.isfinite(y) & np.isfinite(year)
    df, y, year, race = df[ok].reset_index(drop=True), y[ok], year[ok], race[ok]

    base_cols = select_feature_cols(df, drop_prefixes=("jrdb_ms_",), keep_odds=args.keep_odds)
    trt_cols = select_feature_cols(df, keep_odds=args.keep_odds)   # jrdb_ms_* を含む（+ 既存 jrdb_*）
    tr = year < args.cutoff_year
    te = ~tr
    label = "単勝(rank_win)" if args.win else "複勝(rank)"
    odds_note = "市場オッズ込み" if args.keep_odds else "市場オッズ除外(本番準拠)"
    base_rate = float(np.mean(y[te])) if te.any() else float("nan")
    print(f"[採用検証] {label} / {odds_note} / baseline {len(base_cols)}列 + MySpeed {len(ms_cols)}列"
          f" / 学習 {int(tr.sum()):,}行(<{args.cutoff_year}) / 評価 {int(te.sum()):,}行"
          f" / 評価陽性率 {base_rate:.3f}")
    if te.sum() < 2000 or tr.sum() < 2000:
        print("学習/評価データが薄すぎます（cutoff-year を見直してください）", file=sys.stderr)
        return 1

    yte = y[te]
    p_base, _ = _fit_predict(df.loc[tr, base_cols], y[tr], df.loc[te, base_cols])
    p_trt, clf = _fit_predict(df.loc[tr, trt_cols], y[tr], df.loc[te, trt_cols])

    ll_b, ll_t = _ll(yte, p_base), _ll(yte, p_trt)
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
            d = _ll(yte[mk], p_trt[mk]) - _ll(yte[mk], p_base[mk])
            imp += int(d < 0)
            tot += 1
            ybits.append(f"{int(yv)}:{d:+.5f}")
    print(f"② 年別 {'  '.join(ybits)} → 改善 {imp}/{tot}")

    # ③ プラセボ（jrdb_ms_* をレース内シャッフル → 増分が消えれば本物）
    rng = np.random.default_rng(0)
    df_pl = df.copy()
    for c in ms_cols:
        df_pl[c] = df_pl.groupby(race)[c].transform(
            lambda s: s.to_numpy()[rng.permutation(len(s))])
    p_pl, _ = _fit_predict(df_pl.loc[tr, trt_cols], y[tr], df_pl.loc[te, trt_cols])
    d_pl = _ll(yte, p_pl) - ll_b
    print(f"③ プラセボ(レース内シャッフル) ΔlogLoss = {d_pl:+.5f}"
          f"（本物なら実測 {ll_t - ll_b:+.5f} より0寄り）")

    # ④ 過去≥3走（薄履歴のみ依存でないか）
    if "jrdb_ms_npast" in df.columns:
        rich = pd.to_numeric(df.loc[te, "jrdb_ms_npast"], errors="coerce").to_numpy() >= 3
        if rich.sum() > 500:
            d_rich = _ll(yte[rich], p_trt[rich]) - _ll(yte[rich], p_base[rich])
            print(f"④ 過去≥3走({int(rich.sum()):,}行) ΔlogLoss = {d_rich:+.5f}")

    # ⑤⑥ 校正・importance
    print(f"⑤ ECE {ece_b:.4f}→{ece_t:.4f}（{ece_t - ece_b:+.4f}・悪化なし=非正が望ましい）")
    fi = pd.Series(clf.feature_importances_, index=trt_cols)
    ranks = fi.rank(ascending=False).astype(int)
    print("⑥ jrdb_ms_* importance（split 数・全特徴中の順位）:")
    for c in ms_cols:
        print(f"   {c:<16} imp={int(fi[c]):>6}  rank {int(ranks[c])}/{len(trt_cols)}")

    # ⑦ 列順パリティ（学習=推論で同一列・同一順）
    ok_order = list(df.loc[tr, trt_cols].columns) == list(df.loc[te, trt_cols].columns) == trt_cols
    print(f"⑦ 学習=推論の列順パリティ: {'OK' if ok_order else 'NG'}")

    print("\n判定の目安: ①が負(≈−0.0036)かつ②が過半年で改善、③でほぼ消失、④でも負、"
          "⑤非悪化 → 採用可（回収率は評価対象外）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
