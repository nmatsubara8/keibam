"""較正あり/なしの実バックテスト回収率を比較する（実データ A/B）。

models/takeout_calibration.json の較正済み控除率を使った EV 選定と、公称控除率 0.2 の
EV 選定を、同じモデル・同じ featured_data・同じ払戻テーブルでバックテストし、券種別の
回収率・的中率・買い目数を比較する。

控除率は連系推定オッズ（HistoricalOddsProvider）にのみ効くため、同じ EV 閾値でも
選定される買い目が変わる。較正で順序系（馬単/三連単）の推定オッズが下がり EV が絞られた
結果、実払戻での回収率がどう変わるかを定量化する。

実行:
  python compare_calibration_backtest.py                 # 直近20%のレースで比較
  python compare_calibration_backtest.py --test-frac 0.3 # 検証区間を変える
  python compare_calibration_backtest.py --version v1    # モデルを指定（既定は最新）
"""

from __future__ import annotations

import argparse
import logging

logger = logging.getLogger(__name__)

_LABEL = {
    "tansho": "単勝", "fukusho": "複勝", "umaren": "馬連", "umatan": "馬単",
    "wide": "ワイド", "sanrenpuku": "三連複", "sanrentan": "三連単",
}


def _fmt(x, pct=False):
    if x is None:
        return "—"
    return f"{x * 100:.1f}%" if pct else f"{x:.3f}"


def _diagnose_tansho_ev(ai, featured_slice) -> None:
    """単勝の EV(=較正勝率×単勝オッズ) 分布を出して、買い目0の原因を切り分ける。

    - EV が NaN だらけ → 単勝オッズ列や勝率がおかしい（バグ）。
    - EV が 0.8 付近に密集 → 効率的市場（モデルが市場とほぼ一致）。閾値はそれ未満に。
    """
    import numpy as np

    from src.policies._score_policy import CURRENT_ODDS
    from src.policies._score_policy import ExpectedValueScorePolicy
    from src.policies._score_policy import PROB

    try:
        table = ai.calc_score(featured_slice, ExpectedValueScorePolicy)
        ev = (table[PROB].astype(float) * table[CURRENT_ODDS].astype(float)).to_numpy()
        finite = ev[np.isfinite(ev)]
        n_nan = int(len(ev) - len(finite))
        print("\n[診断] 単勝 EV = 較正勝率 × 単勝オッズ の分布")
        print(f"  対象 {len(ev)} 行 / NaN {n_nan} 行 / 有効 {len(finite)} 行")
        if len(finite):
            qs = np.percentile(finite, [50, 90, 95, 99, 100])
            print(f"  EV 中央値={qs[0]:.3f}  p90={qs[1]:.3f}  p95={qs[2]:.3f}"
                  f"  p99={qs[3]:.3f}  max={qs[4]:.3f}")
            for th in (0.7, 0.8, 0.9, 1.0, 1.2):
                print(f"   EV>{th}: {int((finite > th).sum())} 行")
    except Exception as e:  # noqa: BLE001
        print(f"[診断] EV 分布の計算に失敗: {e}")


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="較正あり/なしの実バックテスト回収率比較")
    ap.add_argument("--test-frac", type=float, default=0.2, help="検証に使う直近レースの割合")
    ap.add_argument("--version", default=None, help="モデルのバージョン名（既定は最新）")
    ap.add_argument("--min-bets", type=int, default=10, help="この買い目数未満の券種は警告のみ")
    ap.add_argument(
        "--ev-threshold", type=float, default=None,
        help="全券種の EV 閾値を上書き（既定の BetThresholds は高く較正モデルでは買い目0になりやすい。"
             "較正効果を見るには 1.0 等の低めを指定）",
    )
    args = ap.parse_args()

    from app._bet_type_optimizer import compare_calibration_backtest
    from app._data_loader import find_model_paths
    from app._data_loader import load_model_by_version
    from app._data_loader import load_model_from_path
    from app._model_compare import recent_race_slice
    from app._model_eval import _load_return_processor
    from app._model_eval import load_featured_data
    from src.policies._takeout_calibration import latest_takeout_map
    from src.policies._takeout_calibration import takeout_calibration_path

    featured = load_featured_data()
    if featured is None or featured.empty:
        logger.error("featured_data.pkl がありません。ingest / rebuild-featured を実行してください")
        return
    rp = _load_return_processor()
    if rp is None:
        logger.error("払戻テーブル（return_tables）が読み込めません")
        return

    calib = latest_takeout_map(takeout_calibration_path("models"))
    if not calib:
        logger.error(
            "models/takeout_calibration.json がありません。先に calibrate-takeout を実行してください"
        )
        return

    if args.version:
        ai = load_model_by_version(args.version)
        model_label = args.version
    else:
        paths = find_model_paths("models")
        if not paths:
            logger.error("models/ に学習済みモデル（*_keibam.pickle）がありません")
            return
        ai = load_model_from_path(paths[0])
        model_label = paths[0]

    featured_slice = recent_race_slice(featured, args.test_frac)
    n_races = featured_slice.index.nunique()
    logger.info(
        "[compare] モデル=%s / 検証 %d レース（直近 %.0f%%）/ EV閾値=%s / 較正控除率=%s",
        model_label, n_races, args.test_frac * 100,
        args.ev_threshold if args.ev_threshold is not None else "既定(BetThresholds)",
        {k: round(v, 4) for k, v in calib.items()},
    )

    _diagnose_tansho_ev(ai, featured_slice)

    df = compare_calibration_backtest(
        ai, featured_slice, rp, calib, ev_threshold=args.ev_threshold
    )

    print("\n" + "=" * 86)
    print("較正あり/なし バックテスト比較（回収率 = 払戻 / 投資。1.0 超で黒字）")
    print("=" * 86)
    header = (f"{'券種':<8}{'買い目(公称)':>12}{'回収率(公称)':>14}"
              f"{'買い目(較正)':>12}{'回収率(較正)':>14}{'Δ回収率':>12}")
    print(header)
    print("-" * 86)
    for _, r in df.iterrows():
        bt = r["bet_type"]
        delta = r["delta_return"]
        mark = ""
        if delta is not None:
            mark = "  ▲" if delta > 0.005 else ("  ▼" if delta < -0.005 else "")
        print(
            f"{_LABEL.get(bt, bt):<8}"
            f"{int(r['n_nominal']):>12}{_fmt(r['return_nominal']):>14}"
            f"{int(r['n_calibrated']):>12}{_fmt(r['return_calibrated']):>14}"
            f"{_fmt(delta):>12}{mark}"
        )
    print("=" * 86)
    total_bets = int(df["n_nominal"].sum() + df["n_calibrated"].sum())
    if total_bets == 0:
        print("⚠ 全券種で買い目0。較正済みモデルでは EV が既定閾値"
              "（単勝1.78〜三連単10.0）を超えないためです（効率的市場の現実）。")
        print("  較正効果を観察するには低い閾値で再実行: "
              "python compare_calibration_backtest.py --ev-threshold 1.0")
    else:
        print("Δ回収率 = 較正 − 公称。▲ は較正で改善、▼ は悪化。単勝は控除率の影響を受けない。")
        print("買い目数が少ない券種（< --min-bets）は統計的に不安定なので参考値。")
    print("=" * 86)


if __name__ == "__main__":
    main()
