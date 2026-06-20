"""未実施レース（明日・当日）の出馬表＋暫定オッズで予測・推奨を出す CLI。

既存の予測ページ（app/pages/2_prediction.py）は featured_data に既にあるレース＝
取込済み（=実施済み）レースしか予測できない。本スクリプトは「まだ実施していないレース」を
出馬表（shutuba）ページからスクレイプし、暫定（前売り）単勝オッズを使って予測する。

フロー（tests/integration/test_live_prediction.py の実証済み経路を踏襲）:
  出馬表スクレイプ → ShutubaTableProcessor → ShutubaDataMerger（過去成績/血統/レース情報を結合）
  → FeatureEngineering → model.effective_model → run_prediction（config.yaml の検証済み戦略）

推奨は config.yaml の戦略（単勝・EV>1.1・オッズ≤15・1/4ケリー）に従う。暫定オッズで
選定するため、これは docs/betting_strategy.md §6 で言う「締切前オッズでの実戦」そのもの。

実行:
  # 明日の全レースを予測（既定）
  python predict_upcoming.py

  # 日付指定（開催日 YYYYMMDD）
  python predict_upcoming.py --date 20260621

  # 特定レースだけ
  python predict_upcoming.py --date 20260621 --race-id 202605030211 202605030212

  # スクレイプ済みの出馬表 pickle を使う（ネット不要・予測部の単体確認用）
  python predict_upcoming.py --shutuba-pkl data/tmp/shutuba.pkl
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import tempfile

logger = logging.getLogger(__name__)


def _scrape_shutuba_day(date_yyyymmdd: str, race_ids: list[str] | None):
    """開催日の出馬表（暫定オッズ込み）をスクレイプし、結合 DataFrame を返す。

    race_ids 未指定なら当日の全レースを自動検出する。取得失敗レースはスキップ。
    """
    import pandas as pd

    from src.preparing._scrape_shutuba import scrape_race_id_race_time_list
    from src.preparing._scrape_shutuba import scrape_shutuba_table

    date_str = f"{date_yyyymmdd[:4]}/{date_yyyymmdd[4:6]}/{date_yyyymmdd[6:8]}"
    # 開催レースと発走時刻を取得（--race-id 指定時はその部分集合に絞る）。
    all_ids, all_times = scrape_race_id_race_time_list(date_yyyymmdd)
    if not all_ids:
        logger.error("開催レースが見つかりません: %s（非開催日 or 出馬表未公開）", date_yyyymmdd)
        return None
    post_of = {str(r): t for r, t in zip(all_ids, all_times, strict=False)}
    ids = [str(r) for r in race_ids] if race_ids else [str(r) for r in all_ids]

    def _post_dt(rid: str):
        t = post_of.get(rid)
        if not t:
            return None
        try:
            return dt.datetime.strptime(f"{date_yyyymmdd} {t}", "%Y%m%d %H:%M")
        except ValueError:
            return None

    # 発走順（早い順）に並べて「次のレース」を分かりやすくする。
    ids.sort(key=lambda r: _post_dt(r) or dt.datetime.max)
    print(f"出馬表・暫定オッズを取得: {date_yyyymmdd} / {len(ids)} レース（発走順）")

    frames = []
    for i, rid in enumerate(ids, 1):
        pdt = _post_dt(rid)
        post_s = post_of.get(rid, "??:??")
        if pdt is not None:
            mtp = int((pdt - dt.datetime.now()).total_seconds() // 60)
            when = f"発走{post_s}・あと{mtp}分" + ("（発走済み）" if mtp < 0 else "")
        else:
            when = f"発走{post_s}"
        print(f"  [{i:2d}/{len(ids)}] R{rid[-2:]} {rid}  {when} … 取得中", flush=True)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
                tmp = tf.name
            scrape_shutuba_table(rid, date_str, tmp)
            df = pd.read_pickle(tmp)
            frames.append(df)
            print(f"  [{i:2d}/{len(ids)}] R{rid[-2:]} {rid}  ✓ 取得済み（{len(df)}頭）", flush=True)
        except Exception as e:  # noqa: BLE001 — 1レースの失敗で全体を止めない
            print(f"  [{i:2d}/{len(ids)}] R{rid[-2:]} {rid}  ✗ 取得失敗: {e}", flush=True)
        finally:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
    if not frames:
        return None
    return pd.concat(frames)


def _build_featured(shutuba_df):
    """出馬表 DataFrame → 学習と同じ特徴量セットの featured_data を構築する。

    特徴量チェーンは tests/integration/test_live_prediction.py（出馬表データで動作実証済み）に
    準拠。学習時チェーン（src/pipeline/run_pipeline.py）と差異が出て列数が合わない場合は、
    そちらに合わせて調整する（予測の特徴量整合が崩れた兆候）。
    """
    from src.preprocessing._feature_engineering import FeatureEngineering
    from src.preprocessing._horse_info_processor import HorseInfoProcessor
    from src.preprocessing._horse_results_processor import HorseResultsProcessor
    from src.preprocessing._peds_processor import PedsProcessor
    from src.preprocessing._race_info_processor import RaceInfoProcessor
    from src.preprocessing._shutuba_data_merger import ShutubaDataMerger
    from src.preprocessing._shutuba_table_processor import ShutubaTableProcessor
    from src.constants._local_paths import LocalPaths

    paths = LocalPaths()
    # 発走前は馬体重が未発表（空欄）のことがある。'480(+2)' 形式を前提とする
    # ResultsProcessor の体重パースが落ちるため、空欄を '0(0)'（体重=体重変化=0 の
    # 中立値）に正規化してから渡す。
    from src.constants._results_cols import ResultsCols
    wcol = ResultsCols.WEIGHT_AND_DIFF
    if wcol in shutuba_df.columns:
        s = shutuba_df[wcol].astype(str).str.strip()
        blank = ~s.str.contains("(", regex=False) | s.isin(["", "nan", "None", "NaN"])
        if blank.any():
            shutuba_df = shutuba_df.copy()
            shutuba_df.loc[blank, wcol] = "0(0)"
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as tf:
        shutuba_pkl = tf.name
    shutuba_df.to_pickle(shutuba_pkl)
    try:
        stp = ShutubaTableProcessor(shutuba_pkl)
        merger = ShutubaDataMerger(
            stp,
            HorseResultsProcessor(paths.RAW_HORSE_RESULTS_PATH),
            HorseInfoProcessor(paths.RAW_HORSE_INFO_PATH),
            PedsProcessor(paths.RAW_PEDS_PATH),
            target_cols=["着順"],
            group_cols=["騎手"],
            race_info_processor=RaceInfoProcessor(paths.RAW_RACE_INFO_PATH),
        )
        merger.merge()
        return (
            FeatureEngineering(merger)
            .add_interval().add_agedays()
            .add_interaction_features().add_race_level_zscore()
            .dumminize_kaisai().dumminize_sex().dumminize_weather()
            .dumminize_race_type()
            .dumminize_ground_state1().dumminize_ground_state2()
            .dumminize_ground_state()
            .dumminize_around().dumminize_race_class()
            .encode_horse_id().encode_jockey_id().encode_trainer_id()
            .encode_owner_id().encode_breeder_id()
        ).featured_data
    finally:
        if os.path.exists(shutuba_pkl):
            os.unlink(shutuba_pkl)


def _print_recommendations(race_id, candidates, bankroll: float) -> float:
    """1レースの推奨馬券を表示し、そのレースの合計投資額を返す。"""
    from src.constants._bet_types import BetType

    tansho = [c for c in candidates if c.bet_type in ("tansho", BetType.TANSHO)]
    if not tansho:
        return 0.0
    print(f"\n■ race_id {race_id}")
    print(f"  {'馬番':>4}{'オッズ':>8}{'勝率':>8}{'EV':>7}{'推奨額':>10}")
    total = 0.0
    for c in sorted(tansho, key=lambda c: -c.expected_value):
        stake = int(round(c.stake))
        total += stake
        print(f"  {c.combo[0]:>4}{c.odds:>8.1f}{c.probability:>8.3f}"
              f"{c.expected_value:>7.2f}{stake:>10,d}")
    print(f"  → 投資合計 {int(total):,} 円（bankroll {int(bankroll):,} 円の "
          f"{total / bankroll * 100:.1f}%）")
    return total


def _align_features(featured, model):
    """featured の特徴量列をモデル学習時(feature_names_)の順序・セットに揃える。

    KeibaAI.calc_score と同じ整合（不足列は0埋め・余分列は除外・score_policy が参照する
    メタ列=馬番/枠番/単勝等は保持）を施す。run_prediction は effective_model を直接使い
    この整合を経ないため、ここで明示的に行わないと学習時と列数が合わず predict が落ちる。
    """
    import pandas as pd

    names = getattr(model, "feature_names_", None)
    if not names:
        logger.warning("モデルに feature_names_ が無く列整合をスキップ（旧モデル？）")
        return featured
    from src.policies._score_policy import META_COLS

    meta = [c for c in META_COLS if c in featured.columns]
    feat = [c for c in names if c not in meta]
    missing = [c for c in feat if c not in featured.columns]
    extra = [c for c in featured.columns if c not in names and c not in meta]
    if missing or extra:
        logger.info("列整合: 不足%d列を0埋め / 余分%d列を除外", len(missing), len(extra))
    x_feat = featured.reindex(columns=feat, fill_value=0)
    out = pd.concat([featured[meta], x_feat], axis=1)
    out.index = featured.index
    return out


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    ap = argparse.ArgumentParser(description="未実施レースを暫定オッズで予測（検証済み単勝戦略）")
    ap.add_argument("--date", default=None, help="開催日 YYYYMMDD（既定=明日）")
    ap.add_argument("--race-id", dest="race_ids", nargs="+", default=None,
                    help="対象 race_id を絞る（未指定なら当日全レース）")
    ap.add_argument("--shutuba-pkl", default=None,
                    help="スクレイプ済み出馬表 pickle を使う（ネット不要。予測部の確認用）")
    args = ap.parse_args()

    import pandas as pd

    from app._data_loader import load_latest_model
    from app._data_loader import load_operation_config
    from app._prediction_service import run_prediction

    op_config = load_operation_config("config.yaml")
    date_yyyymmdd = args.date or (dt.date.today() + dt.timedelta(days=1)).strftime("%Y%m%d")

    # 1. 出馬表（暫定オッズ込み）を用意
    if args.shutuba_pkl:
        shutuba_df = pd.read_pickle(args.shutuba_pkl)
    else:
        shutuba_df = _scrape_shutuba_day(date_yyyymmdd, args.race_ids)
    if shutuba_df is None or shutuba_df.empty:
        logger.error("出馬表が空のため中止")
        return

    # 2. 特徴量化
    featured = _build_featured(shutuba_df)
    if featured is None or featured.empty:
        logger.error("featured が空のため中止")
        return

    # 3. モデル + 検証済み戦略で予測
    model = load_latest_model()
    # run_prediction は effective_model（生LGBM）を使い KeibaAI.calc_score の列整合を
    # 経ないため、ここで学習時(feature_names_)の列順・セットに揃える（出馬表チェーンと
    # 学習チェーンの列差・ダミー列差を吸収。不足は0埋め・余分は除外・メタ列は保持）。
    featured = _align_features(featured, model)

    odds_cap = "なし" if op_config.max_odds == float("inf") else f"{op_config.max_odds:.0f}倍"
    print("=" * 70)
    print(f"明日予測（{date_yyyymmdd}） — 検証済み単勝戦略")
    print(f"  EV下限={op_config.tansho_ev_threshold or 'BetThresholds既定'} / "
          f"オッズ上限={odds_cap} / ケリー×{op_config.kelly_fraction_ratio} / "
          f"bankroll={int(op_config.bankroll):,}円")
    print("=" * 70)

    grand_total = 0.0
    n_reco = 0
    for race_id in sorted(featured.index.unique()):
        X = featured.loc[[race_id]]
        try:
            candidates = run_prediction(model.effective_model, X, op_config)
        except Exception as e:  # noqa: BLE001
            logger.warning("予測失敗 race_id=%s: %s", race_id, e)
            continue
        spent = _print_recommendations(race_id, candidates, op_config.bankroll)
        if spent > 0:
            grand_total += spent
            n_reco += 1

    print("\n" + "=" * 70)
    if n_reco == 0:
        print("推奨馬券なし（EV>閾値 かつ オッズ≤上限 を満たす馬がいない）")
    else:
        print(f"推奨 {n_reco} レース / 投資合計 {int(grand_total):,} 円")
    print("※ 暫定オッズでの推奨。締切直前にオッズが動くため発走前に再実行を推奨。")
    print("=" * 70)


if __name__ == "__main__":
    main()
