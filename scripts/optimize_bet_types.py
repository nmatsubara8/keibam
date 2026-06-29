"""券種別パラメータ最適化のヘッドレス実行（UI モデルラボ Tab4「券種別最適化」の CLI 版）。

実 featured_data + 払戻テーブル(return_tables) で券種ごとに
(EV閾値 × 温度 × prob_scale) のグリッドをバックテストし、目的関数（既定 return_rate）を
最大化する組合せを選んで models/bet_type_params.json に保存する。予測（run_prediction）は
最新スナップショットを自動参照するため、保存すればそのまま実戦の券種別選定に反映される。

UI だとクリック操作が必要で再現性・cron 化ができないため、同じ最適化を 1 コマンドで回せる
ようにしたのが本スクリプト（暴走の温床だったアドホック処理の代替・§3-35 の枠組みを利用）。

実行:
    # ドライラン（保存せず結果だけ表示）
    python scripts/optimize_bet_types.py
    # 保存（予測に反映）
    python scripts/optimize_bet_types.py --save
    # 券種・目的関数・最小ベット数を指定
    python scripts/optimize_bet_types.py --save --objective sharpe_ratio --min-bets 20 \
        --bet-types tansho fukusho
"""

from __future__ import annotations

import argparse
import logging
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# 推論を numpy 配列で大量に回すため LightGBM/sklearn が毎回出す無害な警告を抑止する
# （モデルは列名つきで学習済み。バックテストの予測結果には影響しない）。
warnings.filterwarnings(
    "ignore", message="X does not have valid feature names", category=UserWarning,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("optimize_bet_types")


def _load_inputs(models_dir: str):
    """(ai, featured, return_processor) を読み込む。欠けていれば理由付きで None を返す。"""
    from app._data_loader import load_latest_model
    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths

    ai = load_latest_model(models_dir)
    if ai is None:
        return None, "モデルが見つかりません（先に retrain でモデルを生成してください）"
    featured = load_featured_data()
    if featured is None or len(featured) == 0:
        return None, "featured_data が見つかりません（ingest/retrain で featured を生成してください）"
    try:
        from src.preprocessing._return_processor import ReturnProcessor

        rp = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)
    except Exception as e:  # noqa: BLE001
        return None, f"払戻テーブル(return_tables)を読めません: {e}"
    return (ai, featured, rp), None


def main() -> None:
    ap = argparse.ArgumentParser(description="券種別パラメータ最適化（ヘッドレス）")
    ap.add_argument("--save", action="store_true", help="結果を bet_type_params.json に保存（既定はドライラン）")
    ap.add_argument("--objective", default="return_rate",
                    help="目的関数（return_rate / hit_rate / sharpe_ratio など。既定 return_rate）")
    ap.add_argument("--min-bets", type=int, default=10, help="採用に必要な最小ベット数（既定 10）")
    ap.add_argument("--takeout", type=float, default=0.2, help="控除率（既定 0.2）")
    ap.add_argument("--bet-types", nargs="+", default=None,
                    help="対象券種を限定（未指定は OPTIMIZABLE 全券種）")
    ap.add_argument("--models-dir", default="models", help="モデル/保存先ディレクトリ（既定 models）")
    args = ap.parse_args()

    loaded, err = _load_inputs(args.models_dir)
    if loaded is None:
        logger.error("実行できません: %s", err)
        sys.exit(2)
    ai, featured, rp = loaded
    logger.info("入力: featured %d 行 / 券種=%s / objective=%s / min_bets=%d",
                len(featured), args.bet_types or "全件", args.objective, args.min_bets)

    # optimize_all は最後まで無言なので、券種ごとにループして進捗を出す（中身は等価）。
    from app._bet_type_optimizer import default_grid
    from app._bet_type_optimizer import optimize_bet_type
    from src.policies._bet_type_params import OPTIMIZABLE_BET_TYPES
    from src.policies._bet_type_params import default_params

    g = default_grid()
    grid_size = len(g["ev_thresholds"]) * len(g["temperatures"]) * len(g.get("prob_scales", [1.0]))
    targets = list(args.bet_types) if args.bet_types else list(OPTIMIZABLE_BET_TYPES)
    params_map: dict = {}
    metrics_map: dict = {}
    for i, bt in enumerate(targets, 1):
        logger.info("[%d/%d] %s 最適化中…（%d通り × %d行）", i, len(targets), bt, grid_size, len(featured))
        res = optimize_bet_type(ai, featured, rp, bt, objective=args.objective,
                                min_bets=args.min_bets, takeout=args.takeout)
        params_map[bt] = res["best_params"] or default_params(bt)
        metrics_map[bt] = res["best_summary"]
        m = res["best_summary"] or {}
        rr = m.get("return_rate")
        logger.info("[%d/%d] %s 完了: n_bets=%s 回収率=%s", i, len(targets), bt,
                    m.get("n_bets", "—"),
                    f"{rr:.3f}" if isinstance(rr, (int, float)) else "—")

    # 結果サマリ（券種ごとのベスト param と主要指標）。
    print(f"\n■ 券種別最適化結果（objective={args.objective}, min_bets={args.min_bets}）")
    header = f"  {'券種':<10}{'EV閾値':>8}{'温度':>7}{'prob_sc':>9}{'n_bets':>8}{'回収率':>9}{'的中率':>8}"
    print(header)
    eligible = 0
    for bt, params in params_map.items():
        m = metrics_map.get(bt) or {}
        n_bets = m.get("n_bets", 0)
        rr = m.get("return_rate")
        hr = m.get("hit_rate")
        ok = bool(m)  # best_summary があれば min_bets を満たす組合せが見つかった
        eligible += 1 if ok else 0
        d = params.to_dict()
        rr_s = f"{rr:>9.3f}" if isinstance(rr, (int, float)) else f"{'—':>9}"
        hr_s = f"{hr:>8.3f}" if isinstance(hr, (int, float)) else f"{'—':>8}"
        mark = "" if ok else "（既定・該当組合せなし）"
        print(f"  {bt:<10}{d['ev_threshold']:>8.2f}{d['temperature']:>7.2f}{d['prob_scale']:>9.2f}"
              f"{n_bets:>8}{rr_s}{hr_s}  {mark}")
    print(f"  → {eligible}/{len(params_map)} 券種で min_bets を満たすベストを発見")

    if not args.save:
        print("\n（ドライラン）保存して予測に反映するには --save を付けてください。")
        return

    from src.policies._bet_type_params import bet_type_params_path
    from src.policies._bet_type_params import save_bet_type_params

    path = bet_type_params_path(args.models_dir)
    save_bet_type_params(params_map, path, objective=args.objective, metrics=metrics_map)
    print(f"\n✅ 保存しました: {path}")
    print("   予測（run_prediction）は最新スナップショットを自動参照します。")


if __name__ == "__main__":
    main()
