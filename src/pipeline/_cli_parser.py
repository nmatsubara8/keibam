"""run_pipeline の CLI 引数パーサ構築（サブコマンド定義の集約）。

`run_pipeline.py` の肥大化を避けるため、argparse のサブコマンド/オプション定義を本モジュールに
分離する。ハンドラ実装には依存しない純粋な引数定義のみ（実行時 import 不要・テスト容易）。
サブコマンドごとに ``_add_<name>(sub)`` へ分割し、``build_parser`` が順に呼んで組み立てる。
"""

from __future__ import annotations

import argparse


def _add_ingest(sub: argparse._SubParsersAction) -> None:
    """ingest サブコマンドを登録する。"""
    # ingest サブコマンド
    ingest_p = sub.add_parser("ingest", help="終了レースを日次取込")
    race_id_group = ingest_p.add_mutually_exclusive_group(required=True)
    race_id_group.add_argument("--race-id", dest="race_ids", nargs="+", type=int, help="対象 race_id（個別指定）")
    race_id_group.add_argument(
        "--post-date",
        dest="post_date",
        metavar="YYYYMMDD",
        help="開催日を指定して当日の全 race_id を自動取得（cron 用）",
    )
    # Phase 1: 誤情報修正時に既存 DB 行を削除してから再取込するためのフラグ
    ingest_p.add_argument(
        "--force",
        action="store_true",
        help="既存 DB 行を削除してから再取込（誤情報修正時に使用）",
    )
    ingest_p.add_argument(
        "--source",
        default=None,
        help="データ取得元（netkeiba / jravan）。省略時は UI 選択 or 既定 netkeiba",
    )


def _add_retrain(sub: argparse._SubParsersAction) -> None:
    """retrain サブコマンドを登録する。"""
    # retrain サブコマンド
    retrain_p = sub.add_parser("retrain", help="全データで週次再学習")
    retrain_p.add_argument("--version-name", default=None, help="バージョン名（省略時は日付自動生成）")
    retrain_p.add_argument(
        "--featured-path", default=None, metavar="PATH",
        help="学習に使う featured_data を明示指定（既定は FEATURED_DATA_PATH）。"
             "seed（別コーパス）で学習・比較する検証用: --featured-path data/raw/seed_featured_data.pkl",
    )
    retrain_p.add_argument("--no-stacking", action="store_true", help="スタッキングを使わない（LightGBM のみ）")
    retrain_p.add_argument(
        "--no-odds-features", action="store_true",
        help="オッズ由来の派生特徴(単勝_log・市場歪み overlay 等)を除外して学習（対市場エッジの A/B 検証用）",
    )
    retrain_p.add_argument(
        "--no-rating-features", action="store_true",
        help="Elo レーティング由来の特徴(elo_* と その _z)を除外して学習（レーティング効果の A/B 検証用）",
    )
    retrain_p.add_argument(
        "--no-win-head", action="store_true",
        help="Win ヘッド(1着予測, <version>__win.pickle)の併行学習を行わない（Place ヘッドのみ）",
    )
    retrain_p.add_argument("--with-tuning", action="store_true", help="Optuna ハイパラ探索を実行する")
    retrain_p.add_argument(
        "--resume-tuning", action="store_true",
        help="Optuna 探索を永続化して再開する（models/optuna_studies.db）。再実行で trial を追記し "
             "xgboost/catboost/nn（と手書き LightGBM 探索）の best が単調改善する。既定は毎回新規探索。",
    )
    retrain_p.add_argument(
        "--tune-models", default=None, metavar="M1,M2",
        help="探索対象モデルを絞る（カンマ区切り: lightgbm/xgboost/catboost/nn）。指定モデルだけ探索し "
             "他は stored/既定値で固定。指定すると自動で --with-tuning を有効化する。既定は全モデル探索。",
    )
    retrain_p.add_argument(
        "--nn-standalone", action="store_true",
        help="NN を GBDT スタックと分離して単体学習・保存する（分離NN + 遅延スタッキング）。"
             "GBDT は configs/base_models_gbdt.json で別途全データ学習し build-combined で meta 融合する。",
    )
    retrain_p.add_argument(
        "--nn-config", default=None,
        help="--nn-standalone 時の NN パラメータ JSON（nn_params キー）。未指定は既定。",
    )
    retrain_p.add_argument(
        "--gpu", action="store_true",
        help="GBDT(xgboost=device:cuda / catboost=task_type:GPU)を GPU で学習。CUDA 不可なら CPU に"
             "自動フォールバック。NN は torch 側で cuda を自動検出（本フラグ不要）。lightgbm は CPU 据え置き。",
    )
    retrain_p.add_argument(
        "--holdout-years", type=int, nargs="+", default=None, metavar="YYYY",
        help="指定年を学習から除外（out-of-sample 評価用）。例: --holdout-years 2025 → backtest --years 2025",
    )
    retrain_p.add_argument(
        "--since-year", type=int, default=None, metavar="YYYY",
        help="指定年以降の行だけで学習（メモリ/時間節約・A/B 用）。例: --since-year 2016 で直近のみ。"
             "全40年がメモリに載らない環境で Elo 有無等の A/B を回すための行数上限。",
    )
    retrain_p.add_argument(
        "--params-rank",
        type=int,
        default=None,
        help="保存済みチューニング履歴（成績順）の指定 rank のパラメータで学習する（--with-tuning と排他）",
    )
    retrain_p.add_argument(
        "--use-selected-params",
        action="store_true",
        help="UI（モデルラボ）で選択・保存したパラメータ（models/selected_params.json）で学習する",
    )
    # 手書き Optuna 探索（探索範囲・回数を制御）。いずれか指定で method="optuna" に切替。
    retrain_p.add_argument(
        "--n-trials",
        type=int,
        default=None,
        help="手書き Optuna 探索の試行回数（指定すると探索範囲を制御する optuna 方式に切替）",
    )
    retrain_p.add_argument(
        "--tuning-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="手書き Optuna 探索の打ち切り秒数（任意）",
    )
    retrain_p.add_argument(
        "--tuning-config",
        default=None,
        metavar="PATH",
        help="探索範囲・回数を定義した JSON 設定ファイル（src/training/_tuning_config.py 参照）",
    )
    retrain_p.add_argument(
        "--base-models",
        type=str,
        default=None,
        help="カンマ区切りの base 学習器リスト (例: lightgbm,xgboost,catboost)",
    )
    retrain_p.add_argument(
        "--base-models-config",
        type=str,
        default=None,
        help="BaseModelsConfig JSON ファイルパス",
    )


def _add_rebuild_featured(sub: argparse._SubParsersAction) -> None:
    """rebuild-featured サブコマンドを登録する。"""
    # rebuild-featured サブコマンド（raw から featured を再生成。取得なし）
    sub.add_parser(
        "rebuild-featured",
        help="raw pickle から featured_data を再生成する（スクレイプなし。DB 復元後等に使用）",
    )


def _add_backfill(sub: argparse._SubParsersAction) -> None:
    """backfill-*（6コマンド） サブコマンドを登録する。"""
    # backfill-notes サブコマンド（既存 race_id の当日ノートのみを取得。コア取得と独立）
    bn_p = sub.add_parser(
        "backfill-notes",
        help="既存 raw の race_id に当日ノート(調教/パドック/コメント)のみ取得。後で rebuild-featured",
    )
    bn_p.add_argument("--min-year", type=int, default=None, help="この開催年以降のみ対象（既定は年代ゲートに委譲）")
    bn_p.add_argument("--limit", type=int, default=None, help="先頭 N レースのみ（動作確認用）")
    bn_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bn_p.add_argument("--no-skip-existing", action="store_true", help="取得済み race_id も再取得する（既定はスキップ）")

    # backfill-yoso サブコマンド（既存 race_id の予想印のみを取得。コア取得と独立）
    by_p = sub.add_parser(
        "backfill-yoso",
        help="既存 raw の race_id に予想印(無料+premium)のみ取得。後で rebuild-featured",
    )
    by_p.add_argument("--min-year", type=int, default=None, help="この開催年以降のみ対象")
    by_p.add_argument("--limit", type=int, default=None, help="先頭 N レースのみ（動作確認用）")
    by_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    by_p.add_argument("--no-skip-existing", action="store_true", help="取得済み race_id も再取得する")

    # backfill-yoso-predictors サブコマンド（予想家スキル prior のみ取得）
    byp = sub.add_parser(
        "backfill-yoso-predictors",
        help="yoso_marks の predictor_yid に予想家スキル prior のみ取得。後で rebuild-featured",
    )
    byp.add_argument("--limit", type=int, default=None, help="先頭 N 人のみ（動作確認用）")
    byp.add_argument("--source", type=str, default=None, help="データソース名")
    byp.add_argument("--no-skip-existing", action="store_true", help="取得済み予想家も再取得する")

    # backfill-persons サブコマンド（人物の年度別成績のみ取得。コア取得と独立）
    bp2 = sub.add_parser(
        "backfill-persons",
        help="results の jockey/trainer_id に人物年度別成績のみ取得。後で rebuild-featured",
    )
    bp2.add_argument("--types", type=str, default=None, help="対象種別（カンマ区切り。既定 jockey,trainer）")
    bp2.add_argument("--limit", type=int, default=None, help="先頭 N 人のみ（動作確認用）")
    bp2.add_argument("--source", type=str, default=None, help="データソース名")
    bp2.add_argument("--no-skip-existing", action="store_true", help="取得済み entity も再取得する")

    # backfill-horses サブコマンド（既存 horse_id の馬ページを網羅取得。peds は別ジョブ）
    bh_p = sub.add_parser(
        "backfill-horses",
        help="results の全 horse_id に馬ページ(horse_results/horse_info)を取得。KEIBA_SKIP_PEDS=1 で血統を分離",
    )
    bh_p.add_argument("--limit", type=int, default=None, help="先頭 N 頭のみ（動作確認用）")
    bh_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bh_p.add_argument("--no-skip-existing", action="store_true", help="取得済み horse_id も再取得する")

    # backfill-peds サブコマンド（既存 horse_id の血統のみを取得。馬ページ取得と独立）
    bp_p = sub.add_parser(
        "backfill-peds",
        help="既存 raw の horse_id に血統(peds)のみ取得。KEIBA_SKIP_PEDS=1 で馬ページ先行取得した後に使用",
    )
    bp_p.add_argument("--limit", type=int, default=None, help="先頭 N 頭のみ（動作確認用）")
    bp_p.add_argument("--source", type=str, default=None, help="データソース名（既定: 選択保存 > netkeiba）")
    bp_p.add_argument("--no-skip-existing", action="store_true", help="取得済み horse_id も再取得する")


def _add_evaluate_odds_dynamics(sub: argparse._SubParsersAction) -> None:
    """evaluate-odds-dynamics サブコマンドを登録する。"""
    # evaluate-odds-dynamics サブコマンド
    eval_p = sub.add_parser("evaluate-odds-dynamics", help="オッズ力学モデルの比較評価（重力統計も更新）")
    eval_p.add_argument("--holdout-frac", type=float, default=0.2, help="検証に使う直近レースの割合")


def _add_fetch_final_odds(sub: argparse._SubParsersAction) -> None:
    """fetch-final-odds サブコマンドを登録する。"""
    # fetch-final-odds サブコマンド（過去レースの最終確定オッズを全券種で取得）
    fo_p = sub.add_parser(
        "fetch-final-odds",
        help="過去レースの最終確定オッズを全券種（単複/枠連/馬連/馬単/ワイド/三連複/三連単）で取得・永続化",
    )
    fo_group = fo_p.add_mutually_exclusive_group(required=True)
    fo_group.add_argument("--race-id", dest="race_ids", nargs="+", type=int, help="対象 race_id（個別指定）")
    fo_group.add_argument(
        "--post-date", dest="post_date", metavar="YYYYMMDD", help="開催日を指定して当日の全レースを対象（1 日分）"
    )
    fo_group.add_argument(
        "--from-results", dest="from_results", action="store_true",
        help="取込済み results.pkl の全 race_id を対象（過去全レースのバックフィル）",
    )
    fo_p.add_argument(
        "--bet-types", dest="bet_types", nargs="+", default=None,
        help="対象券種（省略時は全 8 券種）。例: tansho umaren sanrentan",
    )
    fo_p.add_argument(
        "--years", dest="years", nargs="+", type=int, default=None,
        metavar="YYYY", help="race_id の年で絞り込む（例: 2010 2011 … 大量バックフィルの分割用）",
    )
    fo_p.add_argument(
        "--limit", dest="limit", type=int, default=None,
        help="1 回で取得するレース数の上限（resume で分割実行するため）",
    )
    fo_p.add_argument(
        "--force", dest="force", action="store_true",
        help="取得済みレースもスキップせず再取得する",
    )


def _add_calibrate_takeout(sub: argparse._SubParsersAction) -> None:
    """calibrate-takeout サブコマンドを登録する。"""
    # calibrate-takeout サブコマンド（払戻実績から券種別実効控除率を逆算）
    ct_p = sub.add_parser(
        "calibrate-takeout",
        help="払戻実績×単勝勝率から券種別の実効控除率を逆算し models/takeout_calibration.json に保存",
    )
    ct_p.add_argument(
        "--min-samples", dest="min_samples", type=int, default=20,
        help="較正に必要な券種別の最小サンプル数（未満は JRA 公称控除率へフォールバック）",
    )
    ct_p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="逆算結果をログ表示するのみで保存しない",
    )


def _add_calibrate_ev(sub: argparse._SubParsersAction) -> None:
    """calibrate-ev サブコマンドを登録する。"""
    # calibrate-ev サブコマンド（OOS で γ,δ / r̂較正 / α,β を fit して保存）
    ce_p = sub.add_parser(
        "calibrate-ev",
        help="OOSデータで補正Harville(γ,δ)/r̂較正/市場合成(α,β)を fit し models/*.json に保存",
    )
    ce_p.add_argument("--version", default=None, help="基準にするモデルのバージョン名（省略時は最新）")
    ce_p.add_argument(
        "--years", type=int, nargs="+", default=None, metavar="YYYY",
        help="fit に使う年（学習年より後の OOS にすること。例: 2025）",
    )
    ce_p.add_argument(
        "--which", nargs="+", default=["exponents", "calibrator", "blend"],
        choices=["exponents", "calibrator", "blend"],
        help="fit するアーティファクト（既定: 全て）",
    )
    ce_p.add_argument(
        "--dry-run", dest="dry_run", action="store_true",
        help="fit 結果をログ表示するのみで保存しない",
    )
    ce_p.add_argument(
        "--no-odds-features", action="store_true",
        help="retrain --no-odds-features で学習したモデルを較正する際に指定（featured から同じ"
             "オッズ由来列を落として列を一致させる。backtest --no-odds-features と対）",
    )
    ce_p.add_argument(
        "--no-rating-features", action="store_true",
        help="retrain --no-rating-features で学習したモデルを較正する際に指定（Elo 由来列を除外）",
    )


def _add_backtest(sub: argparse._SubParsersAction) -> None:
    """backtest サブコマンドを登録する。"""
    # backtest サブコマンド
    bt_p = sub.add_parser(
        "backtest",
        help="ホールドアウト期間で2ヘッド予測→確定オッズEV選定→実払戻で券種別回収率を評価",
    )
    bt_p.add_argument("--version", default=None, help="評価するモデルのバージョン名（省略時は最新）")
    bt_p.add_argument(
        "--featured-path", default=None, metavar="PATH",
        help="評価に使う featured_data を明示指定（既定は FEATURED_DATA_PATH）。"
             "seed モデルの edge-diagnostic 用: --featured-path data/raw/seed_featured_data.pkl",
    )
    bt_p.add_argument(
        "--years", type=int, nargs="+", default=None, metavar="YYYY",
        help="評価対象を race_id の年で絞る（例: 2024 2025）。学習年と重ねないこと",
    )
    bt_p.add_argument(
        "--no-win-head", action="store_true",
        help="Win ヘッドを使わず Place 単独で評価（2ヘッド無効化）",
    )
    bt_p.add_argument(
        "--no-final-odds", action="store_true",
        help="確定オッズを使わず単勝からの Harville 推定オッズで評価",
    )
    bt_p.add_argument(
        "--bet-types", nargs="+", default=None,
        help="評価する券種（省略時は全券種）。例: fukusho wide sanrenpuku",
    )
    bt_p.add_argument("--json", action="store_true", help="結果を JSON で出力")
    bt_p.add_argument(
        "--edge-diagnostic", action="store_true",
        help="自分の勝率 r̂ vs 実現最終市場 p_mkt の較正・エコー・勝ち馬logloss を併せて出力",
    )
    bt_p.add_argument(
        "--no-odds-features", action="store_true",
        help="retrain --no-odds-features で学習したモデルを評価する際に指定（featured から同じ"
             "オッズ由来列を落として列を一致させる。学習=.values で位置一致が必要なため）",
    )
    bt_p.add_argument(
        "--no-rating-features", action="store_true",
        help="retrain --no-rating-features で学習したモデルを評価する際に指定（featured から同じ"
             "Elo 由来列を落として列を一致させる）",
    )
    bt_p.add_argument(
        "--corrected-harville", action="store_true",
        help="models/place_exponents.json の (γ,δ) を読み補正Harvilleで順序券種を評価",
    )
    bt_p.add_argument(
        "--calibrate", action="store_true",
        help="models/win_calibrator.json の r̂ 較正を適用して勝率を補正",
    )
    bt_p.add_argument(
        "--blend", action="store_true",
        help="models/blend_weights.json の (α,β) で市場合成した勝率を使う",
    )
    bt_p.add_argument(
        "--unratable-fallback", action="store_true",
        help="初出走馬(career_starts=0/NaN)を公衆 implied 勝率に置換し初出走のみのレースは除外（ベンター §3）",
    )


def _add_doctor(sub: argparse._SubParsersAction) -> None:
    """doctor サブコマンドを登録する。"""
    doctor_p = sub.add_parser("doctor", help="データ/モデル/DB/ディスクの健全性を点検")
    doctor_p.add_argument("--json", action="store_true", help="結果を JSON で出力")
    doctor_p.add_argument("--strict", action="store_true", help="WARN でも非0終了する")
    doctor_p.add_argument(
        "--prune-models",
        type=int,
        default=None,
        metavar="KEEP",
        help="モデルを新しい順に KEEP 世代残して古い世代を削除する",
    )


def build_parser() -> argparse.ArgumentParser:
    """全サブコマンドを登録した ArgumentParser を返す。"""
    parser = argparse.ArgumentParser(description="継続学習パイプライン")
    sub = parser.add_subparsers(dest="job", required=True)
    _add_ingest(sub)
    _add_retrain(sub)
    _add_rebuild_featured(sub)
    _add_backfill(sub)
    _add_evaluate_odds_dynamics(sub)
    _add_fetch_final_odds(sub)
    _add_calibrate_takeout(sub)
    _add_calibrate_ev(sub)
    _add_backtest(sub)
    _add_build_combined(sub)
    _add_doctor(sub)
    return parser


def _add_build_combined(sub: argparse._SubParsersAction) -> None:
    """build-combined サブコマンド（分離NN + 遅延スタッキングの融合）を登録する。"""
    p = sub.add_parser("build-combined", help="GBDT スタックと NN 単体を meta 融合して保存")
    p.add_argument("--gbdt-model", required=True, help="GBDT スタックの pickle パス（KeibaAIFactory.save 済み）")
    p.add_argument("--nn-model", required=True, help="NN 単体の pickle パス（retrain --nn-standalone で保存）")
    p.add_argument("--version-name", default=None, help="出力バージョン名（既定: 日付自動）")
    p.add_argument("--featured-path", default=None, help="featured_data.pkl（既定: 本番）")
    p.add_argument(
        "--meta-years", type=int, nargs="+", default=None, metavar="YYYY",
        help="meta 融合を学習する holdout 年（両 base が --holdout-years で除外した年を推奨＝リーク回避）",
    )
    p.add_argument("--test-size", type=float, default=0.3, help="holdout 内の test 比率（既定 0.3）")
    p.add_argument("--valid-size", type=float, default=0.3, help="holdout 内の valid 比率（既定 0.3）")
