"""P2: VOI+CMI カテゴリ・アブレーション（市場に無い直交情報が「どこ」に在るかの帰属）。

**目的**: JRDB を 5 つの事前登録カテゴリ（調教/ペース/ラップ/厩舎/脚質）に分け、各カテゴリが
市場(単勝オッズ)に対して持つ**直交情報**を、完全 OOS で category-vs-market の入れ子比較として
測る。residual-head(B) と同じ機構を各カテゴリに適用する（θ≡0→P≡q＝市場が帰無）。

**事前登録の規律（結果を見る前に固定・見てから動かさない）**:
  - カテゴリ→特徴の帰属はキーワードで **a-priori** に決め、実行前に必ず印字（下の print membership）。
  - 全カテゴリ共通で固定: JRA限定(provenance nar_rows=0)・同一 rolling-origin fold・
    レース内 z-score・residual-head モデルクラス・L2=1.0・MES=0.001・ECE許容+0.005・
    開催場×日 block bootstrap を主推定に使う。
  - 市場由来（単勝/odds/impl/kijun_odds）は残差ヘッドに入れない（直交でないため）。
  - **中間結果を見て特徴を足す/閾値を動かす/再検定するのは禁止**（多重探索）。

**主判定 (Primary)**: カテゴリ別 OOS ΔNLL の 開催場×日 block bootstrap 片側 p を集め、
5 検定に **Holm 補正** をかける（family-wise error 制御）。
**副次 (Secondary・採用トリガでない/説明のみ)**: CMI I(X_category;Y|market)＝各カテゴリの
残差ヘッド出力 r が市場を条件づけた後に勝敗へ残す情報量[bit]。
**累積前進系列 (Secondary)**: 市場+調教+ペース+ラップ+厩舎+脚質 の順で ΔNLL を積む（参考のみ）。

**境界（本タスクでやらないこと）**: P2 の勝者から 2018-2026 上でチューニング済み合成モデルを
作らない（新仮説＝新期間で検証すべき。frozen 2027 で確認する）。

featured はローカル成果物（gitignore）。純部（block_bootstrap_ci/holm_correction/
edge_decomposition/fit_residual_head/build_residual_records）は単体テスト済。

使い方:
  python scripts/run_voi_cmi.py --list-features      # 実名確認（事前登録の前に）
  python scripts/run_voi_cmi.py                      # 5カテゴリ確定運用
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# --- 事前登録カテゴリ（キーワードで a-priori 帰属。実行時に実名解決して必ず印字する）---------
# 各カテゴリは include キーワードのいずれかを含む列を候補にし、市場由来と outcome は除外する。
# 順序は固定（累積前進系列と first-match 帰属の決定性のため）。
CATEGORY_ORDER = ["調教", "ペース", "ラップ", "厩舎", "脚質"]
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    # 調教: 追い切り・調教指数（JRDB CYB 系）
    "調教": ["chokyo", "oikiri", "oik", "追切", "調教", "cyb", "chokyo_idx", "chokyosi_ten"],
    # ペース: 展開/ペース予想・逃げ率・テン脚（発走前の予想ペース。実現ペースは outcome なので除外側で弾く）
    "ペース": ["pace_yosou", "pace_forecast", "tenkai", "展開", "ペース予想", "nige_ratio",
               "senko_ratio", "ten_idx", "pace_pred"],
    # ラップ: 上がり/前半後半の脚（過去走ラップ由来の集約。当該レースの実現ラップは outcome 側で除外）
    "ラップ": ["mae3f", "ato3f", "agari", "lap", "furlong", "上がり", "3f_", "last3f",
               "sec_rel", "time_rel"],
    # 厩舎: 厩舎・調教師（勝率/回収/コンビ）
    "厩舎": ["kyusha", "trainer", "厩舎", "chokyosi", "stable", "chomu", "cho_win", "cho_roi"],
    # 脚質: 脚質（逃げ/先行/差し/追込・自在）— 予想脚質・脚質適性
    "脚質": ["kyakushitsu", "脚質", "nige", "senko", "sashi", "oikomi", "jizai",
             "running_style", "leg_type"],
}
# 市場由来（残差ヘッドに入れない＝直交でない）と、outcome/leak 由来を弾く共通除外キーワード。
MARKET_KEYWORDS = ["単勝", "odds", "impl", "kijun_odds", "ninki", "人気", "支持"]
# 当該レースの結果由来（realized）を弾く: 確定着順/払戻/実現ペース/実現ラップ等が特徴に紛れた場合の保険。
OUTCOME_KEYWORDS = ["rank_win", "payoff", "haraimodo", "着順", "race_pace", "realized",
                    "result_", "chaku"]


def _market_or_outcome(col: str) -> bool:
    s = str(col).lower()
    return any(k.lower() in s for k in MARKET_KEYWORDS) or any(k.lower() in s for k in OUTCOME_KEYWORDS)


def resolve_membership(columns) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """列名からカテゴリ帰属を a-priori 解決する（first-match で相互排他化・決定的）。

    返す (membership, overlaps):
      membership[cat] = そのカテゴリに確定帰属した列（first-match）。
      overlaps = 複数カテゴリのキーワードに当たった列（監査用・実際の帰属は first-match）。
    市場/outcome 由来列は全カテゴリから除外する。数値/bool 列のみ対象は呼び出し側で絞る。
    """
    cols = [str(c) for c in columns if not _market_or_outcome(str(c))]
    membership: dict[str, list[str]] = {c: [] for c in CATEGORY_ORDER}
    overlaps: dict[str, list[str]] = {}
    for col in cols:
        low = col.lower()
        hit = [cat for cat in CATEGORY_ORDER
               if any(kw.lower() in low for kw in CATEGORY_KEYWORDS[cat])]
        if not hit:
            continue
        if len(hit) > 1:
            overlaps[col] = hit
        membership[hit[0]].append(col)  # first-match で相互排他
    return membership, overlaps


def _numeric_columns(feat):
    import numpy as np
    return [str(c) for c in feat.select_dtypes(include=[np.number, "bool"]).columns]


# ---- 結果不変の診断（再検定でない・ΔNLL/Holm/CMI を一切変えない）------------------------------
# 目的: 調教の近年限定性・ラップの構造的ゼロ・厩舎内の raw/z 重複を、再検定せずに確定する。

def _nonmissing_rate(values) -> float:
    """有限値の割合（欠測率の裏返し）。純関数。"""
    import numpy as np
    v = np.asarray(values, dtype=float)
    return float(np.isfinite(v).mean()) if v.size else 0.0


def _within_race_var_fraction(values, race_ids) -> float:
    """レース内で分散>0 を持つレースの割合（＝馬間差を持つ特徴かの構造診断）。純関数。

    course_lap_length のようなレース内定数は全レースで var=0 → 0.0 を返す（残差ヘッドに効かない）。
    各レースは有限値2つ以上かつ分散>0 のとき「分散あり」と数える。
    """
    import numpy as np
    from collections import defaultdict
    v = np.asarray(values, dtype=float)
    r = np.asarray(race_ids)
    races: dict = defaultdict(list)
    for val, rid in zip(v, r):
        if np.isfinite(val):
            races[rid].append(val)
    if not races:
        return 0.0
    hit = sum(1 for vals in races.values() if len(vals) >= 2 and np.var(vals) > 0)
    return hit / len(races)


def _effective_rank(X) -> dict:
    """設計行列の有効rank（raw/z 重複や共線性を再検定せず可視化）。純関数（**直接SVD**）。

    返す {n_features, numerical_rank, effective_rank(exp(entropy(特異値))∈[1,nf]), cond}。
    厩舎の raw と _z（レース内z-score後に一致）は有効rankを名目より下げる＝冗長の直接証拠。
    Gram(X.T@X)は条件数を二乗し微小特異値の数値誤差を拡大するため、QR で (nf×nf) に縮約してから
    **X の特異値を直接** SVD で得る（QR は数値安定・特異値は不変）。rank は matrix_rank と同じ
    既定 tol（smax·max(m,n)·eps）で数え、rank<min(m,n) なら cond=inf（微小値を隠さない正直な条件数）。
    """
    import numpy as np
    X = np.asarray(X, dtype=float)
    nf = int(X.shape[1]) if X.ndim == 2 else 0
    empty = {"n_features": nf, "numerical_rank": 0, "effective_rank": 0.0, "cond": float("inf")}
    if X.ndim != 2 or nf == 0:
        return empty
    X = X[np.isfinite(X).all(axis=1)]
    m = X.shape[0]
    if m < 2:
        return empty
    r = np.linalg.qr(X, mode="r") if m >= nf else X      # QR で nf×nf に縮約（特異値不変）
    s = np.linalg.svd(r, compute_uv=False)               # X の特異値を直接（Gram を作らない）
    if s.size == 0 or float(s[0]) <= 0:
        return empty
    smax = float(s[0])
    tol = smax * max(X.shape) * np.finfo(float).eps      # np.linalg.matrix_rank と同じ既定 tol
    numerical = int((s > tol).sum())
    cond = float(smax / s[-1]) if numerical >= len(s) and s[-1] > 0 else float("inf")
    pos = s[s > 0]
    p = pos / pos.sum()
    eff = float(np.exp(-(p * np.log(p)).sum()))
    return {"n_features": nf, "numerical_rank": numerical, "effective_rank": eff, "cond": cond}


def _print_membership(membership, overlaps) -> None:
    print("=" * 78)
    print("[事前登録] カテゴリ→特徴の帰属（結果を見る前に固定・以下が pre-registration 記録）")
    print("  共通設定: JRA限定 / rolling-origin / レース内z-score / residual-head / "
          "L2=1.0 / MES=0.001 / ECE許容+0.005 / 開催場×日 block bootstrap")
    for cat in CATEGORY_ORDER:
        cols = membership[cat]
        print(f"  [{cat}] n={len(cols)}: {cols[:25]}" + (" …" if len(cols) > 25 else ""))
    if overlaps:
        print(f"  [注意] 複数カテゴリに当たった列（first-match で先頭へ帰属）: "
              f"{ {k: v for k, v in list(overlaps.items())[:15]} }")
    print("=" * 78)


def _category_dnll(records, feat_cols, *, l2, min_train_years, n_boot, seed=0):
    """1カテゴリの OOS ΔNLL(開催場×日 block) と CMI 用 OOS (r,y,m) を返す。

    market(θ≡0) vs market+category-residual を rolling-origin で回し、テスト各レースの
    per-race ΔNLL とブロック(race_id[:10]) を集める。同時に CMI 用に、テスト馬ごとの
    残差ヘッド出力 r・勝敗 y・市場implied m を OOS で貯める（副次のCMI推定に使う）。
    block bootstrap は全カテゴリ同一の (B, seed) で再現性を担保する。
    """
    import numpy as np

    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_predict, residual_win_probs
    from src.simulation._model_compare import block_bootstrap_ci, race_nll
    from src.simulation._rolling_origin import rolling_origin_compare, rolling_origin_folds

    def fit_baseline(_train):
        return None

    def prob_baseline(_p, race):
        return market_probs(race["odds"])

    def fit_challenger(train):
        return fit_residual_head(train, feat_cols, l2=l2)

    def prob_challenger(theta, race):
        if not theta or all(v == 0 for v in theta.values()):
            return market_probs(race["odds"])
        return residual_win_probs(race["odds"], race["feats"], theta)

    res = rolling_origin_compare(
        records, fit_baseline, prob_baseline, fit_challenger, prob_challenger,
        min_train_years=min_train_years, k_extra_params=len(feat_cols), n_boot=n_boot, seed=seed)

    dnll, blocks = [], []
    r_oos, y_oos, m_oos = [], [], []
    theta_norms: list[float] = []
    fold_rank: list[dict] = []          # **fold毎**の学習行列 rank（pooled でなく実際の推定問題）
    for train, test, _y in rolling_origin_folds(records, min_train_years=min_train_years):
        theta = fit_challenger(train)
        theta_norms.append(
            float(np.sqrt(sum(float(v) ** 2 for v in theta.values()))) if theta else 0.0)
        fold_rank.append(_effective_rank(_design_matrix(train, feat_cols)))
        for r in test:
            if r.get("winner") is None:
                continue
            pb, pc = prob_baseline(None, r), prob_challenger(theta, r)
            if not (pb and pc):
                continue
            dnll.append(race_nll(pc, r["winner"]) - race_nll(pb, r["winner"]))
            blocks.append(str(r["race_id"])[:10])
            q = market_probs(r["odds"])
            for h, feats_h in r["feats"].items():
                if h not in q:
                    continue
                r_oos.append(residual_predict(feats_h, theta) if theta else 0.0)
                y_oos.append(1 if h == r["winner"] else 0)
                m_oos.append(float(q[h]))
    b_used = max(2000, n_boot)
    bb = block_bootstrap_ci(dnll, blocks, n_boot=b_used, seed=seed)
    return {
        "pooled": res["pooled"], "folds": res["folds"], "n_folds": res["n_folds"],
        "bb": bb, "r_oos": np.asarray(r_oos), "y_oos": np.asarray(y_oos),
        "m_oos": np.asarray(m_oos), "fold_rank": fold_rank, "b_used": b_used, "seed": seed,
        "theta_norm_mean": float(np.mean(theta_norms)) if theta_norms else 0.0,
        "theta_norm_last": float(theta_norms[-1]) if theta_norms else 0.0,
    }


def source_diagnostics(feat, membership, active, *, jra_only=True) -> dict:
    """カテゴリ×年度の非欠測率/最大unique/レース内分散あり率（raw featured 由来・結果不変）。

    records（z-score後・欠測は0埋め済）でなく **raw featured** を読むので、調教の近年限定性
    （2018-2023 の非欠測率≈0）・ラップの構造的ゼロ（分散あり率=0）を再検定せず確定できる。
    返す {cat: {year: {nonmissing, max_unique, var_frac, n_rows, n_races}}}。
    """
    import numpy as np
    import pandas as pd

    rid = pd.Series(feat.index.astype(str), index=feat.index)
    if jra_only:
        mask = rid.str[4:6].isin({f"{i:02d}" for i in range(1, 11)}).to_numpy()
    else:
        mask = np.ones(len(feat), dtype=bool)
    df = feat[mask]
    rid = df.index.astype(str)
    year = pd.Series(rid, index=df.index).str[:4]
    out: dict = {}
    for cat in active:
        cols = [c for c in membership[cat] if c in df.columns]
        per_year: dict = {}
        for y in sorted(set(year)):
            ysel = (year == y).to_numpy()
            sub = df.loc[ysel]
            rids = sub.index.astype(str).to_numpy()
            nrows = int(len(sub))
            if not cols or nrows == 0:
                per_year[y] = {"nonmissing": 0.0, "max_unique": 0, "var_frac": 0.0,
                               "n_rows": nrows, "n_races": len(set(rids))}
                continue
            nm = float(np.mean([_nonmissing_rate(pd.to_numeric(sub[c], errors="coerce"))
                                for c in cols]))
            uq = int(max(pd.to_numeric(sub[c], errors="coerce").dropna().nunique() for c in cols))
            vf = float(max(_within_race_var_fraction(
                pd.to_numeric(sub[c], errors="coerce").to_numpy(), rids) for c in cols))
            per_year[y] = {"nonmissing": nm, "max_unique": uq, "var_frac": vf,
                           "n_rows": nrows, "n_races": len(set(rids))}
        out[cat] = per_year
    return out


def _design_matrix(records, cols):
    """records（z-score後）から (行=馬, 列=特徴) の設計行列を積む（有効rank診断用）。"""
    import numpy as np
    rows = [[float(rec["feats"][h].get(c, 0.0)) for c in cols]
            for rec in records for h in rec["feats"]]
    return np.asarray(rows, dtype=float) if rows else np.zeros((0, len(cols)))


def _minmedmax(xs):
    """fold 横断の (最小, 中央, 最大)。nan は除外・inf は保持（条件数の rank落ちを隠さない）。"""
    import numpy as np
    a = np.asarray([x for x in xs if x == x], dtype=float)   # x==x で nan だけ除外
    if a.size == 0:
        return (float("nan"), float("nan"), float("nan"))
    return float(np.min(a)), float(np.median(a)), float(np.max(a))


def main() -> int:
    import numpy as np

    from app._model_eval import load_featured_data
    from scripts.run_residual_head import build_residual_records
    from src.simulation._information import edge_decomposition
    from src.simulation._model_compare import block_bootstrap_ci, holm_correction, race_nll
    from src.policies._market_residual import market_probs
    from src.policies._residual_head import fit_residual_head, residual_win_probs
    from src.simulation._rolling_origin import rolling_origin_folds
    from src.training._provenance import assert_jra_only

    ap = argparse.ArgumentParser(description="P2 VOI+CMI カテゴリ・アブレーション（事前登録・完全OOS）")
    ap.add_argument("--featured", default=None)
    ap.add_argument("--list-features", action="store_true",
                    help="featured の数値列を出して終了（事前登録の帰属確認に使う）")
    ap.add_argument("--l2", type=float, default=1.0, help="残差ヘッドL2（事前登録・全カテゴリ共通）")
    ap.add_argument("--mes", type=float, default=1e-3, help="最小実用効果量（結果後に下げない）")
    ap.add_argument("--min-train-years", type=int, default=3)
    ap.add_argument("--n-boot", type=int, default=2000,
                    help="block bootstrap 反復 B（全カテゴリ共通・最終台帳用に増やすなら固定Bで一度限り）")
    ap.add_argument("--seed", type=int, default=0, help="bootstrap seed（全カテゴリ共通・再現性）")
    ap.add_argument("--allow-nar", action="store_true", help="NAR も含める（既定は JRA 限定）")
    args = ap.parse_args()

    feat = load_featured_data(args.featured) if args.featured else load_featured_data()
    if feat is None or feat.empty:
        print("featured を読めません（ローカルで実行）", file=sys.stderr)
        return 2

    num_cols = _numeric_columns(feat)
    membership, overlaps = resolve_membership(num_cols)

    if args.list_features:
        print(f"[featured 数値列 {len(num_cols)}] 市場/outcome 由来は帰属から除外済み。")
        for cat in CATEGORY_ORDER:
            print(f"  [{cat}] {membership[cat][:40]}")
        excluded = [c for c in num_cols if _market_or_outcome(c)]
        print(f"  [除外: 市場/outcome 由来] {excluded[:40]}")
        return 0

    # --- 事前登録の印字（結果を見る前に membership を確定・監査可能にする）---
    _print_membership(membership, overlaps)

    active = [c for c in CATEGORY_ORDER if membership[c]]
    empty = [c for c in CATEGORY_ORDER if not membership[c]]
    if empty:
        print(f"[SOURCE_MISSING] このローカル featured で 0 特徴のカテゴリ（検定不能・正直に除外）: {empty}")
    if not active:
        print("有効カテゴリが無い（全カテゴリ 0 特徴）。--list-features で実名を確認せよ。", file=sys.stderr)
        return 3

    # 全カテゴリの union で 1 度だけ records を作る（z-score は特徴独立なので後で部分集合化して良い）
    union = sorted({c for cat in active for c in membership[cat]})
    records, feat_cols_all = build_residual_records(feat, union, jra_only=not args.allow_nar)
    if not records:
        print("records が空（JRA限定/3頭以上/勝ち馬確定で 0）。", file=sys.stderr)
        return 3

    # provenance: JRA限定を実データで強制（fail-closed）。records は JRA だけのはずだが二重確認。
    if not args.allow_nar:
        rid_all = [r["race_id"] for r in records]
        try:
            nar = assert_jra_only(rid_all)
            print(f"[provenance] JRA限定確定 nar_rows={nar}（実データ由来・fail-closed）")
        except RuntimeError as e:
            print(f"[FAIL-CLOSED] {e}", file=sys.stderr)
            return 4

    have = set(feat_cols_all)
    membership = {cat: [c for c in membership[cat] if c in have] for cat in active}
    active = [c for c in active if membership[c]]
    print(f"[records] {len(records):,} レース（JRA限定={not args.allow_nar}・3頭以上・レース内z-score）")
    print(f"[有効カテゴリ] {active}")

    # === Primary: カテゴリ別 OOS ΔNLL（開催場×日 block）+ Secondary CMI ===
    results: dict[str, dict] = {}
    for cat in active:
        cols = membership[cat]
        r = _category_dnll(records, cols, l2=args.l2,
                           min_train_years=args.min_train_years, n_boot=args.n_boot, seed=args.seed)
        ed = edge_decomposition(r["r_oos"], r["y_oos"], r["m_oos"]) \
            if len(r["r_oos"]) else {"mi": float("nan"), "cmi": float("nan"),
                                     "redundant": float("nan"), "edge_ratio": float("nan")}
        results[cat] = {**r, "cmi": ed, "n_feat": len(cols)}

    # Holm 補正は **事前登録 5 カテゴリ** の family で行う（検定できた数に縮めない）。
    # SOURCE_MISSING / 検定不能カテゴリは「検定した」ことにせず、多重補正の帳簿上のみ p=1.0 を置く
    # （非棄却で family を保つ・事前登録との対応が明瞭で最も保守的）。
    not_tested = [c for c in CATEGORY_ORDER if c not in active]
    pairs = [(cat, results[cat]["bb"].get("p_improve", float("nan"))) for cat in active]
    pairs += [(cat, 1.0) for cat in not_tested]     # p=1.0 帳簿（NOT TESTED・検定したことにしない）
    holm = holm_correction(pairs, alpha=0.05)       # m=5（事前登録どおり）
    holm_by_cat = {h["name"]: h for h in holm}

    b_used = results[active[0]].get("b_used", max(2000, args.n_boot))
    min_p = 1.0 / (b_used + 1)
    nblocks = results[active[0]]["bb"].get("n_blocks", 0)
    print("\n" + "=" * 78)
    print("=== Bootstrap 再現性（全カテゴリ共通・固定）===")
    print(f"  反復 B={b_used:,}  seed={args.seed}  ブロック数={nblocks:,}  "
          f"最小到達可能 p=1/(B+1)={min_p:.2e}  estimand=race-weighted 平均ΔNLL")
    if min_p > 5e-3:
        print(f"  [注意] 最小到達可能 p({min_p:.2e}) が大きい＝Monte Carlo 誤差が p≈0.007-0.01 帯に影響し得る。"
              f"最終台帳は --n-boot を増やし固定B/seedで一度だけ再計算（特徴/モデルは不変）。")
    print("\n" + "=" * 78)
    print("=== Primary: カテゴリ別 市場+残差 vs 市場（OOS ΔNLL・開催場×日 block・Holm補正 m=5 事前登録）===")
    print(f"{'カテゴリ':<8}{'n_feat':>7}{'ΔNLL(mean)':>13}{'95%CI':>26}{'p':>9}{'p_Holm':>9}{'判定':>16}")
    for cat in active:
        bb = results[cat]["bb"]
        pooled = results[cat]["pooled"]
        de = pooled.get("d_ece")
        h = holm_by_cat.get(cat, {})
        sig = (bb["hi"] < 0) and h.get("reject", False)
        practical = abs(bb["mean"]) >= args.mes
        ece_ok = (de is None) or (de <= ECE_TOL)
        if sig and practical and ece_ok:
            verdict = "✅採用候補"
        elif sig and ece_ok:
            verdict = "🟡統計のみ<MES"
        else:
            verdict = "❌直交情報なし"
        ci = f"[{bb['lo']:+.6f},{bb['hi']:+.6f}]"
        print(f"{cat:<8}{results[cat]['n_feat']:>7}{bb['mean']:>+13.6f}{ci:>26}"
              f"{h.get('p', float('nan')):>9.4f}{h.get('p_holm', float('nan')):>9.4f}{verdict:>16}")
    for cat in not_tested:      # 事前登録だが検定不能（SOURCE_MISSING）＝family維持で p=1.0 帳簿
        h = holm_by_cat.get(cat, {})
        print(f"{cat:<8}{0:>7}{'—':>13}{'NOT TESTED/SOURCE_MISSING':>26}"
              f"{h.get('p', 1.0):>9.4f}{h.get('p_holm', 1.0):>9.4f}{'NOT TESTED':>16}")
    print(f"  ※ Holm family=事前登録5カテゴリ（m=5）。NOT TESTED は検定せず p=1.0 を帳簿に置くだけ"
          f"（非棄却で family を保つ）。有効={active} / 未検定={not_tested}")

    print("\n--- fold 別 ΔNLL（特定年だけ効く不安定性の確認）---")
    for cat in active:
        cells = []
        for f in results[cat]["folds"]:
            v = f.get("d_nll")
            cells.append(f"{f['year']}:{v:+.5f}" if isinstance(v, float) else f"{f['year']}:n/a")
        print(f"  [{cat}] {'  '.join(cells)}")

    print("\n--- ECE（較正非悪化の確認・許容 +{:.3f} 事前固定）---".format(ECE_TOL))
    for cat in active:
        de = results[cat]["pooled"].get("d_ece")
        print(f"  [{cat}] ΔECE={de:+.6f}" if isinstance(de, float) else f"  [{cat}] ΔECE=n/a")

    # === Secondary: CMI I(X_category;Y|market)（説明のみ・採用トリガでない）===
    print("\n" + "=" * 78)
    print("=== Secondary: CMI I(残差_category ; 勝敗 | 市場)[bit]（説明のみ・採用判定に使わない）===")
    print(f"{'カテゴリ':<8}{'I(X;Y)':>12}{'I(X;Y|市場)':>14}{'冗長':>12}{'edge_ratio':>12}")
    for cat in active:
        c = results[cat]["cmi"]
        print(f"{cat:<8}{c['mi']:>12.5f}{c['cmi']:>14.5f}{c['redundant']:>12.5f}{c['edge_ratio']:>12.3f}")
    print("  ※ CMI は情報量であり正しさではない。採用は上の Primary(ΔNLL×Holm×MES×ECE) のみで決める。")

    # === 診断（結果不変・再検定でない）: カテゴリ×年度の source 健全性＋設計行列 rank＋係数ノルム ===
    print("\n" + "=" * 78)
    print("=== 診断（結果不変・ΔNLL/Holm/CMI を変えない）: source 健全性・共線性・係数ノルム ===")
    diag = source_diagnostics(feat, membership, active, jra_only=not args.allow_nar)
    print("  [カテゴリ×年度] 非欠測率 / 最大unique / レース内分散あり率（raw featured 由来）")
    years = sorted({y for cat in active for y in diag.get(cat, {})})
    hdr = "  " + f"{'カテゴリ':<8}" + "".join(f"{y:>18}" for y in years)
    print(hdr)
    for cat in active:
        cells = []
        for y in years:
            d = diag[cat].get(y, {})
            cells.append(f"{d.get('nonmissing', 0):.2f}/{d.get('max_unique', 0)}/"
                         f"{d.get('var_frac', 0):.2f}")
        print("  " + f"{cat:<8}" + "".join(f"{c:>18}" for c in cells))
    print("  （凡例: 非欠測率=有限値割合 / 最大unique=カテゴリ内最多ユニーク値数 / "
          "分散あり率=レース内で馬間差>0 のレース割合）")
    print("\n  [設計行列 有効rank（fold毎の学習行列・pooled でない）: 中央値[最小,最大]]")
    print(f"  {'カテゴリ':<8}{'n_feat':>7}{'数値rank(med[min,max])':>24}{'有効rank(med)':>14}{'cond(med)':>14}")
    for cat in active:
        fr = results[cat].get("fold_rank", [])
        nr = _minmedmax([f["numerical_rank"] for f in fr])
        ef = _minmedmax([f["effective_rank"] for f in fr])
        cd = _minmedmax([f["cond"] for f in fr])
        cond_med = "inf" if cd[1] == float("inf") else f"{cd[1]:.1f}"
        nf = fr[0]["n_features"] if fr else len(membership[cat])
        print(f"  {cat:<8}{nf:>7}{f'{nr[1]:.0f}[{nr[0]:.0f},{nr[2]:.0f}]':>24}"
              f"{ef[1]:>14.2f}{cond_med:>14}")
    print("\n  [OOS 残差logit補正 Xθ の実効量（係数の大きさでなく“市場をどれだけ動かしたか”）]")
    print(f"  {'カテゴリ':<8}{'n_feat':>7}{'‖θ‖平均':>11}{'‖θ‖/√nf':>11}{'std(Xθ)':>12}{'mean|Xθ|':>12}")
    for cat in active:
        r = results[cat]
        nf = max(1, r.get("n_feat", 1))
        ro = r["r_oos"]
        std_x = float(np.std(ro)) if ro.size else 0.0
        mabs_x = float(np.mean(np.abs(ro))) if ro.size else 0.0
        tn = r.get("theta_norm_mean", 0.0)
        print(f"  {cat:<8}{r.get('n_feat', 0):>7}{tn:>11.4f}{tn / (nf ** 0.5):>11.4f}"
              f"{std_x:>12.5f}{mabs_x:>12.5f}")
    print("  ※ 有効rank≪n_feat＋cond巨大/inf＝raw/z 等の冗長（厩舎）＝21自由度ぶんの独立情報でない。")
    print("    分散あり率0＝レース内定数で検定不能（ラップ）。非欠測率が近年のみ高い＝coverage 依存（調教）。")
    print("    std(Xθ) が小さい＝L2下でほぼ縮退し市場をほとんど動かしていない（多特徴でも効かない説明）。")

    # === Secondary: 累積前進系列（市場+調教+…+脚質・参考のみ）===
    print("\n" + "=" * 78)
    print("=== Secondary: 累積前進系列 ΔNLL（市場に順次カテゴリを足す・参考のみ／採用判定でない）===")
    cum: list[str] = []
    for cat in active:
        cum += membership[cat]

        def fit_c(train, _cols=list(cum)):
            return fit_residual_head(train, _cols, l2=args.l2)

        def prob_b(_p, race):
            return market_probs(race["odds"])

        def prob_c(theta, race):
            if not theta or all(v == 0 for v in theta.values()):
                return market_probs(race["odds"])
            return residual_win_probs(race["odds"], race["feats"], theta)

        dnll, blocks = [], []
        for train, test, _y in rolling_origin_folds(records, min_train_years=args.min_train_years):
            theta = fit_c(train)
            for r in test:
                if r.get("winner") is None:
                    continue
                pb, pc = prob_b(None, r), prob_c(theta, r)
                if pb and pc:
                    dnll.append(race_nll(pc, r["winner"]) - race_nll(pb, r["winner"]))
                    blocks.append(str(r["race_id"])[:10])
        bb = block_bootstrap_ci(dnll, blocks, n_boot=max(2000, args.n_boot))
        added = " + ".join(active[:active.index(cat) + 1])
        print(f"  市場 + {added:<28} ΔNLL={bb['mean']:+.6f} "
              f"95%CI[{bb['lo']:+.6f},{bb['hi']:+.6f}] (n_feat={len(cum)})")

    print("\n" + "=" * 78)
    print("[境界] P2 の勝者から 2018-2026 上でチューニング済み合成モデルを作らない（新仮説＝新期間）。")
    print("        採用候補は frozen 2027 期間で別途確認する（features/L2/MES/ECE許容は再調整しない）。")
    print("[多重探索の禁止] この出力を見て特徴を足す/閾値を動かす/再検定するのは事前登録違反。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
