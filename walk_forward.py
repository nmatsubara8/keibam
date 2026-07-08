"""walk-forward OOS バックテスト: 各時間チャンクを、それ以前のデータだけで学習したモデルで検証。

漏洩も in-sample 楽観も無い honest な成績を、時系列に沿って測る土台。
レースを発走日順に --folds 個のチャンクへ分け、fold k（k>=1）を「fold 0..k-1 で学習した
モデル」で予測・単勝バックテストする（拡張窓 walk-forward）。fold 0 は初期学習のみで評価しない。
各 fold と通算（プール）の回収率・的中率・損益を出す。

これにより「直近20%だけ」でなく時系列全体で honest にエッジを評価でき、今後の施策
（特徴量追加・別券種・別モデル）も同じ枠組みで比較できる。

実行:
  python walk_forward.py                          # 5分割・単勝・EV1.1・単一LightGBM（速い）
  python walk_forward.py --folds 6 --ev-floor 1.2
  python walk_forward.py --stacking               # 本番同等のスタッキングで（やや遅い）
"""

from __future__ import annotations

import argparse
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
warnings.filterwarnings("ignore", message="X does not have valid feature names", category=UserWarning)


def _fmt(x):
    return f"{x:.3f}" if isinstance(x, (int, float)) else str(x)


def _score_predictors(edge):
    """edge_df（p_mkt/r_hat/p_blend/won）から各予測器の (勝logloss, Brier, ECE, AUC) を返す。"""
    import numpy as np

    from predict_quality import _ece
    from src.simulation._edge_diagnostic import _win_logloss

    out = {}
    won = edge["won"]
    for name, col in [("市場", "p_mkt"), ("モデル", "r_hat"), ("companion", "p_blend")]:
        p = edge[col]
        ll = _win_logloss(p, won)
        brier = float(np.nanmean((p.to_numpy() - won.to_numpy()) ** 2))
        ece = _ece(p, won)
        try:
            from sklearn.metrics import roc_auc_score
            auc = float(roc_auc_score(won, p))
        except Exception:  # noqa: BLE001
            auc = float("nan")
        out[name] = (ll, brier, ece, auc)
    return out


def _train_ai(ai, args):
    """args に応じて学習方法を選ぶ。--with-tuning で Optuna ハイパラ探索を有効化する。

    - stacking + tuning: 本番同等（4モデル×Optuna→スタッキング）
    - tuning のみ:        単一モデルの Optuna 探索
    - stacking のみ:      スタッキング（探索なし）
    - どちらも無し:        単一 LightGBM（探索なし・従来の既定）
    """
    tc = getattr(args, "tuning_config_obj", None)  # --tuning-config で読み込んだ TuningConfig（無ければ None）
    bm = getattr(args, "base_models_obj", None)     # --base-models の BaseModelsConfig（無ければ既定=LGBMのみ）
    if args.stacking:
        ai.train_with_stacking(with_tuning=args.with_tuning, tuning_config=tc, base_models_config=bm)
    elif args.with_tuning:
        ai.train_with_tuning(tuning_config=tc)
    else:
        ai.train_without_tuning()


def _quality_walk_forward(featured, chunks, factory, args):
    """各 fold: 過去のみ学習→直前 fold で合成(α,β)を fit→評価 fold で市場/モデル/companion を OOS 評価。"""
    import pandas as pd

    from predict_quality import _blend_series
    from src.policies._blend import fit_blend
    from src.policies._score_policy import ExpectedValueScorePolicy
    from src.simulation._edge_diagnostic import build_edge_frame

    def _won(sl):
        return (pd.to_numeric(sl["着順"], errors="coerce") == 1).astype(float)

    def _edge(ai, race_ids):
        sl = featured.loc[race_ids]
        st = ExpectedValueScorePolicy.calc(ai.effective_model, sl)
        return build_edge_frame(st, _won(sl).to_numpy())

    print("=" * 74)
    _learn = ("スタッキング" if args.stacking else "単一LightGBM") + ("+Optuna探索" if args.with_tuning else "")
    print(f"予測品質 walk-forward（市場 vs モデル vs companion / {args.folds}分割 / 学習={_learn}）")
    print(f"  {'評価fold期間':<22}{'予測器':<11}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}")
    print("-" * 74)
    pooled = []
    for k in range(2, args.folds):
        train_races = [r for c in chunks[:k - 1] for r in c]  # chunks[0..k-2]
        try:
            ai = factory.create(featured.loc[train_races], test_size=0.1, valid_size=0.2)
            _train_ai(ai, args)
            ebf = _edge(ai, chunks[k - 1])  # 直前 fold で合成 fit
            races = []
            for _rid, g in ebf.groupby(level=0):
                pf = {int(u): float(p) for u, p in zip(g["umaban"], g["r_hat"], strict=False) if p == p and p > 0}
                pp = {int(u): float(p) for u, p in zip(g["umaban"], g["p_mkt"], strict=False) if p == p and p > 0}
                w = g[g["won"] == 1]["umaban"]
                if len(w) == 1 and pf and pp:
                    races.append((pf, pp, int(w.iloc[0])))
            bw = fit_blend(races) if races else None
            a, b = (bw.alpha, bw.beta) if bw else (0.0, 1.0)
            ev = _edge(ai, chunks[k])  # 評価 fold（モデル・合成とも OOS）
            ev["p_blend"] = _blend_series(ev, a, b).to_numpy()
        except Exception as e:  # noqa: BLE001
            print(f"  fold{k} 失敗: {e}")
            continue
        d0 = pd.to_datetime(featured.loc[chunks[k]]["date"]).min().date()
        d1 = pd.to_datetime(featured.loc[chunks[k]]["date"]).max().date()
        label = f"{d0}〜{d1}(α{a:.2f}β{b:.2f})"
        for i, (name, (ll, br, ece, _auc)) in enumerate(_score_predictors(ev).items()):
            print(f"  {(label if i == 0 else ''):<22}{name:<11}{ll:>11.4f}{br:>10.5f}{ece:>9.4f}")
        print("  " + "-" * 70)
        pooled.append(ev)
    if not pooled:
        print("  評価できる fold がありません（--folds を増やすか件数を確認）")
        return
    allev = pd.concat(pooled)
    print(f"\n[通算プールOOS] レース={allev.index.nunique()} / 馬={len(allev)}")
    print(f"  {'予測器':<11}{'勝logloss':>11}{'Brier':>10}{'ECE':>9}{'AUC':>8}")
    sc = _score_predictors(allev)
    for name, (ll, br, ece, auc) in sc.items():
        print(f"  {name:<11}{ll:>11.4f}{br:>10.5f}{ece:>9.4f}{auc:>8.4f}")
    mkt, comp = sc["市場"], sc["companion"]
    print("-" * 74)
    if comp[0] <= mkt[0] and comp[2] <= mkt[2]:
        print(f"  → companion ≤ 市場（logloss {comp[0]:.4f}≤{mkt[0]:.4f}・ECE {comp[2]:.4f}≤{mkt[2]:.4f}）"
              "＝多時点で安定。market-companion 達成。")
    else:
        print(f"  → companion は市場を安定して上回らない（logloss {comp[0]:.4f} vs {mkt[0]:.4f}・"
              f"ECE {comp[2]:.4f} vs {mkt[2]:.4f}）。単年の微小な勝ちはノイズの可能性。")
    print("=" * 74)


def main():
    ap = argparse.ArgumentParser(description="walk-forward OOS バックテスト（時系列 honest 評価）")
    ap.add_argument("--folds", type=int, default=5, help="時間チャンク数（既定5＝4回評価）")
    ap.add_argument("--bet-type", default="tansho")
    ap.add_argument("--ev-floor", type=float, default=1.1, help="賭ける EV 下限（既定1.1）")
    ap.add_argument("--ev-max", type=float, default=100.0, help="賭ける EV 上限（既定100）")
    ap.add_argument("--temperature", type=float, default=1.0)
    ap.add_argument("--takeout", type=float, default=0.2)
    ap.add_argument("--stacking", action="store_true", help="本番同等スタッキングで学習（既定は単一LightGBM）")
    ap.add_argument("--with-tuning", action="store_true",
                    help="Optuna ハイパラ探索を有効化（--stacking と併用で4モデル×探索→スタッキング=本番最強構成）。"
                         "『チューニングでも市場を出し抜けるか』の OOS 実測用。学習は重くなる")
    ap.add_argument("--tuning-config", default=None,
                    help="探索設定 JSON（例 configs/tuning_config.example.json）。"
                         "省略時は LightGBMTuner の自動段階探索。--with-tuning と併用")
    ap.add_argument("--base-models", default=None,
                    help="スタッキングの base 学習器をカンマ区切りで指定"
                         "（例 'lightgbm,xgboost,catboost,nn,kernel'）。--stacking と併用。"
                         "省略時は LightGBM のみ（＝多モデルにするには明示指定が必要）")
    ap.add_argument("--tune-per-model", action="store_true",
                    help="xgboost/catboost/nn を各モデル個別に Optuna 探索する（--base-models と併用）")
    ap.add_argument("--by-odds", action="store_true",
                    help="全fold プールの OOS 回収率をオッズ帯別に出す（『中人気にエッジ』の honest 検証）")
    ap.add_argument("--quality", action="store_true",
                    help="予測品質モード: 各fold(過去のみ学習→直前foldで合成fit→評価foldでOOS)で"
                         "市場 vs モデル vs companion の勝logloss/Brier/ECEを集計（market-companion 安定性確認）")
    args = ap.parse_args()

    # --tuning-config を指定していれば TuningConfig を読み込み、_train_ai から使えるようにする。
    args.tuning_config_obj = None
    if args.with_tuning and args.tuning_config:
        from src.training._tuning_config import load_tuning_config
        args.tuning_config_obj = load_tuning_config(args.tuning_config)

    # --base-models を指定していれば BaseModelsConfig を作る（スタッキングの base 学習器の顔ぶれ）。
    args.base_models_obj = None
    if args.stacking and args.base_models:
        from src.training._base_models_config import SUPPORTED_MODELS, BaseModelsConfig
        models = tuple(m.strip() for m in args.base_models.split(",") if m.strip())
        bad = [m for m in models if m not in SUPPORTED_MODELS]
        if bad:
            ap.error(f"未対応の base モデル: {bad}（対応: {SUPPORTED_MODELS}）")
        args.base_models_obj = BaseModelsConfig(models=models, tune_per_model=args.tune_per_model)
        print(f"[base学習器] {models}  tune_per_model={args.tune_per_model}")

    import pandas as pd

    from app._bet_type_optimizer import backtest_bet_type
    from app._model_eval import load_featured_data
    from src.constants._local_paths import LocalPaths
    from src.policies._bet_type_params import BetTypeParams
    from src.preprocessing._return_processor import ReturnProcessor
    from src.training._keiba_ai_factory import KeibaAIFactory

    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return
    rp = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)

    # レース単位で発走日順に並べ、--folds 個のチャンクへ分割（レースを跨いで割らない）。
    race_date = pd.to_datetime(featured["date"]).groupby(level=0).first().sort_values()
    ordered = list(race_date.index)
    n = len(ordered)
    if n < args.folds * 2:
        print(f"レース数 {n} が少なすぎます（--folds {args.folds}）")
        return
    bounds = [round(i * n / args.folds) for i in range(args.folds + 1)]
    chunks = [ordered[bounds[i]:bounds[i + 1]] for i in range(args.folds)]

    if args.quality:
        _quality_walk_forward(featured, chunks, KeibaAIFactory, args)
        return

    params = BetTypeParams(ev_threshold=args.ev_floor, temperature=args.temperature,
                           prob_scale=1.0, ev_max=args.ev_max)

    # --by-odds: kelly_backtest の検証済み候補選択・決済を再利用して全 fold をオッズ帯別に集計。
    ODDS_BUCKETS = [(1.0, 3.0), (3.0, 7.0), (7.0, 15.0), (15.0, 50.0), (50.0, float("inf"))]
    band_acc = settle = None
    if args.by_odds:
        from kelly_backtest import _make_settle_fn
        from src.simulation._betting_tickets import BettingTickets
        band_acc = {b: {"n": 0, "hit": 0, "stake": 0.0, "ret": 0.0} for b in ODDS_BUCKETS}
        settle = _make_settle_fn(BettingTickets(rp))

    mode = "スタッキング" if args.stacking else "単一LightGBM"
    print("=" * 78)
    print(f"walk-forward OOS / 券種={args.bet_type} / EV[{args.ev_floor},{args.ev_max}] "
          f"/ 温度={args.temperature} / 学習={mode} / {args.folds}分割")
    print(f"  {'評価fold期間':<26}{'学習R':>8}{'評価R':>8}{'買い目':>8}"
          f"{'的中率':>8}{'回収率':>8}{'損益':>10}")
    print("-" * 78)

    tot_bet = tot_ret = 0.0
    fold_rrs = []
    for k in range(1, args.folds):
        train_races = [r for c in chunks[:k] for r in c]
        eval_races = chunks[k]
        train = featured.loc[train_races]
        fold = featured.loc[eval_races]
        d0 = pd.to_datetime(fold["date"]).min().date()
        d1 = pd.to_datetime(fold["date"]).max().date()
        label = f"{d0}〜{d1}"
        try:
            ai = KeibaAIFactory.create(train, test_size=0.1, valid_size=0.2)
            _train_ai(ai, args)
            summary, _ = backtest_bet_type(ai, fold, rp, args.bet_type, params, takeout=args.takeout)
        except Exception as e:  # noqa: BLE001
            print(f"  {label:<26}  学習/評価失敗: {e}")
            continue
        nb = summary.get("n_bets", 0)
        hr = summary.get("hit_rate", 0.0)
        rr = summary.get("return_rate", 0.0)
        tb = summary.get("total_bet_amount", 0.0)
        profit = summary.get("profit", 0.0)
        tot_bet += tb
        tot_ret += profit + tb
        if nb:
            fold_rrs.append(rr)
        print(f"  {label:<26}{len(set(train_races)):>9}{len(set(eval_races)):>9}{nb:>8}"
              f"{_fmt(hr):>8}{_fmt(rr):>8}{profit:>10,.0f}")

        # この fold の OOS 候補をオッズ帯別に集計（プールに加算）。
        if args.by_odds:
            from kelly_backtest import _candidates_by_race
            from src.policies._score_policy import ExpectedValueScorePolicy
            st = ai.calc_score(fold, ExpectedValueScorePolicy)
            for rid, cands in _candidates_by_race(st, args.ev_floor, args.ev_max, float("inf")):
                for c in cands:
                    ba, ra = settle(rid, c.combo[0], 100)
                    if ba <= 0:
                        continue
                    for lo, hi in ODDS_BUCKETS:
                        if lo <= c.odds < hi:
                            a = band_acc[(lo, hi)]
                            a["n"] += 1
                            a["stake"] += ba
                            a["ret"] += ra
                            if ra > 0:
                                a["hit"] += 1
                            break

    print("-" * 78)
    pooled = tot_ret / tot_bet if tot_bet else 0.0
    print(f"  通算（プールOOS）: 投資 {tot_bet:,.0f} / 払戻 {tot_ret:,.0f} / "
          f"回収率 {pooled:.3f} / 損益 {tot_ret - tot_bet:,.0f}")
    if fold_rrs and all(r > 1.0 for r in fold_rrs) and pooled > 1.0:
        print("  → 全 fold かつ通算で回収率>1 → 時系列に頑健なエッジの可能性（要件数・分散も確認）")
    else:
        n_neg = sum(1 for r in fold_rrs if r <= 1.0)
        print(f"  → {n_neg}/{len(fold_rrs)} fold で回収率≤1、通算 {pooled:.3f}。"
              "honest なエッジは確認されない")

    if args.by_odds:
        print("\n[オッズ帯別 OOS（全 fold プール・フラット100円）]")
        print(f"  {'オッズ帯':<12}{'買い目':>9}{'的中率':>9}{'回収率':>9}")
        any_edge = False
        for lo, hi in ODDS_BUCKETS:
            a = band_acc[(lo, hi)]
            if a["n"] == 0:
                continue
            hr_b = a["hit"] / a["n"]
            rr_b = a["ret"] / a["stake"] if a["stake"] > 0 else 0.0
            any_edge = any_edge or rr_b > 1.0
            hi_s = "∞" if hi == float("inf") else f"{hi:.0f}"
            mark = " ◎" if rr_b > 1.0 else ""
            print(f"  {f'{lo:.0f}–{hi_s}':<12}{a['n']:>9}{_fmt(hr_b):>9}{_fmt(rr_b):>9}{mark}")
        print("  ※ ◎(回収率>1)の帯があれば、そこに OOS エッジの候補。全て≤1なら exploitable な帯は無い。")
    print("=" * 78)


if __name__ == "__main__":
    main()
