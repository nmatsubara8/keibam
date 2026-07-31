"""外厩 raw 特徴（G1）採用検証：featured に外厩当日情報だけを足し確率品質増分を測る。

`myspeed_adoption_check.py` と同方式（本番準拠 baseline・未来分割・確率品質のみ、ROI 非評価）。
外厩コメント pkl を featured へ (race_id,umaban) 左結合し、履歴集約・target encoding を入れず
**raw 4 特徴だけ**（has_gaikyu / days_since_return / interval_weeks / gaikyu_name）を足す。

特徴（`src.jrdb._target.attach_gaikyu_raw` が付与・接頭辞 jrdb_gaikyu_）:
  jrdb_gaikyu_has   外厩利用の有無(0/1)   jrdb_gaikyu_days  帰厩→今走日数(年跨ぎ補完)
  jrdb_gaikyu_weeks 中N週(ローテ間隔)      jrdb_gaikyu_name  外厩名(LightGBM native categorical)

診断（checklist）: ①ΔlogLoss/ΔAUC ②年別改善 ③プラセボ(レース内シャッフル) ④利用馬のみ部分集合
⑤ECE ⑥importance ⑦列順パリティ。判定は myspeed と同基準（①負・②過半年・③消失・⑤非悪化）。

使い方:
  python scripts/gaikyu_adoption_check.py \
    --featured data/featured_jrdb.pkl \
    --gaikyu-pkl data/jrdb_target/jrdb_target_gaikyucomment.pkl --cutoff-year 2024
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._target import attach_gaikyu_raw  # noqa: E402

_PREFIX = "jrdb_gaikyu_"
_NAME_COL = _PREFIX + "name"
_ALWAYS_DROP = ["着順", "rank", "rank_win", "date", "horse_id", "race_id", "通過", "単勝"]
_TARGET_COL = {"place": "rank", "win": "rank_win"}


def target_series(df: pd.DataFrame, *, win: bool) -> np.ndarray:
    col = _TARGET_COL["win" if win else "place"]
    if col not in df.columns:
        raise KeyError(col)
    return pd.to_numeric(df[col], errors="coerce").to_numpy()


def ece(p: np.ndarray, y: np.ndarray, n_bins: int = 10) -> float:
    b = np.clip((p * n_bins).astype(int), 0, n_bins - 1)
    e = 0.0
    for k in range(n_bins):
        mk = b == k
        if mk.any():
            e += mk.mean() * abs(p[mk].mean() - y[mk].mean())
    return float(e)


def select_feature_cols(df: pd.DataFrame, *, drop_prefixes: tuple[str, ...] = (),
                        keep_odds: bool = False) -> list[str]:
    """数値特徴量のうち目的変数・ID・事後情報・drop_prefixes を除いた列（列順保存）。"""
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


def _fit_predict(tr_x, tr_y, te_x, *, categorical=None):
    from lightgbm import LGBMClassifier
    clf = LGBMClassifier(n_estimators=300, learning_rate=0.05, num_leaves=31,
                         subsample=0.8, colsample_bytree=0.8, random_state=0,
                         n_jobs=-1, verbose=-1)
    fit_kw = {}
    if categorical:
        fit_kw["categorical_feature"] = categorical
    clf.fit(tr_x, tr_y, **fit_kw)
    return clf.predict_proba(te_x)[:, 1], clf


def main(argv=None) -> int:
    from sklearn.metrics import log_loss, roc_auc_score

    def _ll(y_true, p):
        return log_loss(y_true, p, labels=[0, 1])

    ap = argparse.ArgumentParser(description="外厩 raw 特徴（G1）採用検証")
    ap.add_argument("--featured", default="data/featured_jrdb.pkl", help="本番 featured")
    ap.add_argument("--gaikyu-pkl", default="data/jrdb_target/jrdb_target_gaikyucomment.pkl",
                    help="jrdb_target_ingest.py の外厩コメント pkl")
    ap.add_argument("--cutoff-year", type=int, default=2024, help="この年以降を OOS 評価")
    ap.add_argument("--win", action="store_true", help="単勝(rank_win)。既定は複勝(rank)")
    ap.add_argument("--keep-odds", action="store_true", help="市場オッズ 単勝 を特徴に残す")
    ap.add_argument("--no-name", action="store_true", help="gaikyu_name(カテゴリ)を使わない")
    args = ap.parse_args(argv)

    fp, gp = Path(args.featured), Path(args.gaikyu_pkl)
    if not fp.exists():
        print(f"featured がありません: {fp}", file=sys.stderr)
        return 1
    if not gp.exists():
        print(f"外厩 pkl がありません: {gp}（先に jrdb_target_ingest.py）", file=sys.stderr)
        return 1
    df = pd.read_pickle(fp)
    gaikyu = pd.read_pickle(gp)
    df = attach_gaikyu_raw(df, gaikyu)

    cov = float((df[_PREFIX + "has"] == 1).mean())
    print(f"[外厩付与] 利用率 {cov:.1%} / days 非欠損 {df[_PREFIX + 'days'].notna().mean():.1%}"
          f" / 外厩名ユニーク {df[_NAME_COL].nunique()}")

    try:
        y = target_series(df, win=args.win)
    except KeyError as e:
        print(f"目的変数列 {e} が featured にありません", file=sys.stderr)
        return 1
    if "date" not in df.columns:
        print("date 列が featured にありません", file=sys.stderr)
        return 1

    race = (df["race_id"] if "race_id" in df.columns else pd.Series(df.index)).to_numpy()
    year = pd.to_datetime(df["date"], errors="coerce").dt.year.to_numpy()
    ok = np.isfinite(y) & np.isfinite(year)
    df, y, year, race = df[ok].reset_index(drop=True), y[ok], year[ok], race[ok]

    base_cols = select_feature_cols(df, drop_prefixes=(_PREFIX,), keep_odds=args.keep_odds)
    num_trt = select_feature_cols(df, keep_odds=args.keep_odds)   # jrdb_gaikyu_ の数値3種を含む
    cat = [] if args.no_name else [_NAME_COL]
    trt_cols = num_trt + cat
    gaikyu_cols = [c for c in df.columns if c.startswith(_PREFIX)]

    tr, te = year < args.cutoff_year, year >= args.cutoff_year
    label = "単勝(rank_win)" if args.win else "複勝(rank)"
    odds_note = "市場オッズ込み" if args.keep_odds else "市場オッズ除外(本番準拠)"
    print(f"[G1採用検証] {label} / {odds_note} / baseline {len(base_cols)}列 + 外厩 "
          f"{len(trt_cols) - len(base_cols)}列（name={'なし' if args.no_name else 'あり'}）"
          f" / 学習 {int(tr.sum()):,}(<{args.cutoff_year}) / 評価 {int(te.sum()):,}")
    if te.sum() < 2000 or tr.sum() < 2000:
        print("学習/評価データが薄すぎます（cutoff-year を見直し）", file=sys.stderr)
        return 1

    yte = y[te]
    p_base, _ = _fit_predict(df.loc[tr, base_cols], y[tr], df.loc[te, base_cols])
    p_trt, clf = _fit_predict(df.loc[tr, trt_cols], y[tr], df.loc[te, trt_cols], categorical=cat or None)

    ll_b, ll_t = _ll(yte, p_base), _ll(yte, p_trt)
    auc_b, auc_t = roc_auc_score(yte, p_base), roc_auc_score(yte, p_trt)
    ece_b, ece_t = ece(p_base, yte), ece(p_trt, yte)
    print("\n[結果] OOS（未来分割）")
    print(f"  {'model':<12}{'logloss':>10}{'AUC':>9}{'ECE':>8}")
    print(f"  {'baseline':<12}{ll_b:>10.5f}{auc_b:>9.5f}{ece_b:>8.4f}")
    print(f"  {'+外厩':<12}{ll_t:>10.5f}{auc_t:>9.5f}{ece_t:>8.4f}")
    print(f"\n① ΔlogLoss = {ll_t - ll_b:+.5f}（負=改善） / ΔAUC = {auc_t - auc_b:+.5f}（正=改善）")

    ybits, imp, tot = [], 0, 0
    for yv in sorted(np.unique(year[te])):
        mk = year[te] == yv
        if mk.sum() > 500:
            d = _ll(yte[mk], p_trt[mk]) - _ll(yte[mk], p_base[mk])
            imp += int(d < 0)
            tot += 1
            ybits.append(f"{int(yv)}:{d:+.5f}")
    print(f"② 年別 {'  '.join(ybits)} → 改善 {imp}/{tot}")

    rng = np.random.default_rng(0)
    df_pl = df.copy()
    for c in gaikyu_cols:
        df_pl[c] = df_pl.groupby(race)[c].transform(
            lambda s: s.to_numpy()[rng.permutation(len(s))])
    if _NAME_COL in df_pl.columns:      # transform でカテゴリ→object になるため再キャスト
        df_pl[_NAME_COL] = df_pl[_NAME_COL].astype("category")
    p_pl, _ = _fit_predict(df_pl.loc[tr, trt_cols], y[tr], df_pl.loc[te, trt_cols],
                           categorical=cat or None)
    print(f"③ プラセボ(レース内シャッフル) ΔlogLoss = {_ll(yte, p_pl) - ll_b:+.5f}"
          f"（本物なら実測 {ll_t - ll_b:+.5f} より0寄り）")

    used = df.loc[te, _PREFIX + "has"].to_numpy() == 1
    if used.sum() > 500:
        d_used = _ll(yte[used], p_trt[used]) - _ll(yte[used], p_base[used])
        print(f"④ 外厩利用馬のみ({int(used.sum()):,}行) ΔlogLoss = {d_used:+.5f}")

    print(f"⑤ ECE {ece_b:.4f}→{ece_t:.4f}（{ece_t - ece_b:+.4f}・非悪化が望ましい）")
    fi = pd.Series(clf.feature_importances_, index=trt_cols)
    ranks = fi.rank(ascending=False).astype(int)
    print("⑥ 外厩特徴 importance（split 数・全特徴中の順位）:")
    for c in gaikyu_cols:      # gaikyu_cols は name も含む（prefix 一致）
        if c in fi.index:
            print(f"   {c:<20} imp={int(fi[c]):>6}  rank {int(ranks[c])}/{len(trt_cols)}")

    ok_order = list(df.loc[tr, trt_cols].columns) == list(df.loc[te, trt_cols].columns) == trt_cols
    print(f"⑦ 学習=推論の列順パリティ: {'OK' if ok_order else 'NG'}")
    print("\n判定の目安: ①が負かつ②過半年で改善・③でほぼ消失・④でも負・⑤非悪化 → 次段（外厩履歴集約）へ。"
          "通らなければ外厩の高度化に進まず終了できる（回収率は評価対象外）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
