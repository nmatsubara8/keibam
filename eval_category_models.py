"""6分割モデルの効果検証: カテゴリ専用モデル vs 統合モデルを同一レース上で比較する。

「全国/地方 × 芝/ダート/障害」の 6 分割（実データは中央 芝/ダート/障害）で、
カテゴリ専用 Place ヘッド（<version>__<category>.pickle）が統合 Place ヘッドより
そのカテゴリのレースで予測が良いかを AUC / logloss で定量化する。分割の価値の判定:

  - カテゴリ専用が統合を AUC で上回る（ΔAUC>0）→ 専用化が識別力を上げる＝分割の価値あり。
  - ほぼ同等/下回る → 統合モデルで十分（データ分割で1カテゴリあたりの学習量が減る不利が勝る）。

注意（in-sample バイアス）: 現行モデルは全データ学習のため評価スライスも in-sample で、
絶対 AUC は楽観的。ただし「同一レース上で 統合 vs 専用」の**相対比較**は両者同条件なので
分割の価値判定には有効。厳密には保留年で両者を再学習して比較する（--years で近年に絞れる）。

実行:
  python eval_category_models.py                 # 全中央カテゴリで AUC/logloss 比較
  python eval_category_models.py --years 2025    # 近年スライスに限定
  python eval_category_models.py --roi           # ROI（回収率）も比較（確定オッズ・較正込み）
"""

from __future__ import annotations

import argparse
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _auc_logloss(y: np.ndarray, p: np.ndarray) -> tuple[float, float]:
    from sklearn.metrics import log_loss, roc_auc_score

    p = np.clip(p, 1e-7, 1 - 1e-7)
    return float(roc_auc_score(y, p)), float(log_loss(y, p))


def _probs(ai, X: pd.DataFrame) -> pd.Series:
    """モデルの top3 予測確率を race_id インデックス付き Series で返す。"""
    from src.policies._score_policy import PROB, ExpectedValueScorePolicy

    table = ai.calc_score(X, ExpectedValueScorePolicy)
    return pd.Series(np.asarray(table[PROB]), index=table.index)


def _binary_top3(slice_df: pd.DataFrame) -> pd.Series:
    """Place target（top3 二値）を得る。featured の 'rank' が二値ならそのまま、生着順なら<=3。"""
    raw = slice_df["rank"]
    uniq = set(pd.unique(raw.dropna().to_numpy()))
    if uniq <= {0, 1}:
        return raw.astype(float)
    return (raw <= 3).astype(float)


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="6分割モデルの効果検証（統合 vs カテゴリ専用）")
    ap.add_argument("--version", default=None, help="評価するモデルのバージョン名（省略時は最新）")
    ap.add_argument("--years", type=int, nargs="+", default=None, help="この年のレースに限定（例 2025）")
    ap.add_argument("--roi", action="store_true", help="ROI（回収率）も比較（確定オッズ・較正込み）")
    args = ap.parse_args()

    from app._data_loader import (
        available_categories_for,
        category_model_path_for,
        find_model_paths,
        load_model_by_version,
        load_model_from_path,
    )
    from app._model_eval import load_featured_data
    from src.constants._model_category import CATEGORY_LABELS
    from src.training._category_split import split_featured_by_category

    featured = load_featured_data()
    if featured is None or featured.empty:
        logger.error("featured_data が読み込めません")
        return
    if args.years:
        yset = {str(y) for y in args.years}
        featured = featured[featured.index.astype(str).str[:4].isin(yset)]
        logger.info("[eval-cat] 年 %s に限定: %d 行", sorted(yset), len(featured))
    if featured.empty:
        logger.error("[eval-cat] 対象レースがありません")
        return

    if args.version:
        combined = load_model_by_version(args.version)
        # version→path 解決（category パス導出に実ファイルパスが要る）
        place_path = next(p for p in find_model_paths("models") if args.version in p)
    else:
        paths = find_model_paths("models")
        if not paths:
            logger.error("学習済みモデルがありません")
            return
        place_path = paths[0]
        combined = load_model_from_path(place_path)
    logger.info("[eval-cat] 統合モデル: %s", place_path)

    cats = available_categories_for(place_path)
    if not cats:
        logger.error("[eval-cat] カテゴリ別モデルがありません（retrain の 6 分割で生成してください）")
        return
    logger.info("[eval-cat] 実在するカテゴリ別モデル: %s", cats)

    groups = split_featured_by_category(featured)

    rows = []
    for cat in cats:
        sub = groups.get(cat)
        if sub is None or sub.empty:
            rows.append((cat, 0, None))
            continue
        y = _binary_top3(sub)
        mask = y.notna().to_numpy()
        y_arr = y.to_numpy()[mask]
        if len(np.unique(y_arr)) < 2:
            rows.append((cat, int(sub.index.nunique()), None))
            continue
        p_comb = _probs(combined, sub).to_numpy()[mask]
        cat_ai = load_model_from_path(category_model_path_for(place_path, cat))
        p_cat = _probs(cat_ai, sub).to_numpy()[mask]
        auc_c, ll_c = _auc_logloss(y_arr, p_comb)
        auc_k, ll_k = _auc_logloss(y_arr, p_cat)
        roi = None
        if args.roi:
            roi = _roi_compare(combined, cat_ai, sub)
        rows.append((cat, int(sub.index.nunique()), (auc_c, auc_k, ll_c, ll_k, roi)))

    print("=" * 78)
    print("6分割モデルの効果検証（同一カテゴリ上で 統合 vs 専用）")
    print("=" * 78)
    hdr = f"{'カテゴリ':<16}{'レース':>7}{'AUC統合':>9}{'AUC専用':>9}{'ΔAUC':>8}{'LL統合':>8}{'LL専用':>8}{'ΔLL':>8}"
    print(hdr)
    for cat, nraces, m in rows:
        label = CATEGORY_LABELS.get(cat, cat)
        if m is None:
            print(f"{label:<16}{nraces:>7}   （データ/正例不足でスキップ）")
            continue
        auc_c, auc_k, ll_c, ll_k, roi = m
        dauc = auc_k - auc_c
        dll = ll_k - ll_c
        mark = " ✓専用有利" if dauc > 0.001 else (" ~同等" if abs(dauc) <= 0.001 else " ✗統合有利")
        print(f"{label:<16}{nraces:>7}{auc_c:>9.4f}{auc_k:>9.4f}{dauc:>+8.4f}"
              f"{ll_c:>8.4f}{ll_k:>8.4f}{dll:>+8.4f}{mark}")
        if roi is not None:
            print(f"{'  └ ROI':<16}{'':>7}{roi[0]:>9.3f}{roi[1]:>9.3f}{roi[1]-roi[0]:>+8.3f}"
                  "  （統合 / 専用 / Δ回収率）")

    print("\n" + "=" * 78)
    print("読み方: ΔAUC>0＝カテゴリ専用が識別力で上回る＝6分割の価値あり。")
    print("~同等/負なら統合モデルで十分（分割で1カテゴリの学習量が減る不利が勝る）。")
    print("注意: 全データ学習モデルのため in-sample。相対比較は有効だが絶対値は楽観的。")
    print("=" * 78)


def _roi_compare(combined, cat_ai, sub: pd.DataFrame):
    """統合 vs 専用モデルで sub を backtest し (roi_combined, roi_category) を返す。"""
    from src.constants._local_paths import LocalPaths
    from src.pipeline._cli_common import _return_processor_db_first
    from src.preparing._odds_snapshot import build_final_odds_lookup
    from src.preparing.odds_scheduler import load_snapshots
    from src.simulation._backtest import default_thresholds, run_backtest

    rp, _ = _return_processor_db_first()
    snaps = load_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)
    lookup = build_final_odds_lookup(snaps) if snaps else None
    th = default_thresholds()

    def _roi(model) -> float:
        res = run_backtest(model.effective_model, sub, rp, final_odds_lookup=lookup, thresholds=th)
        overall = res.get("overall")
        stake = getattr(overall, "stake", 0) if overall else 0
        return float(getattr(overall, "returned", 0) / stake) if stake else float("nan")

    return _roi(combined), _roi(cat_ai)


if __name__ == "__main__":
    main()
