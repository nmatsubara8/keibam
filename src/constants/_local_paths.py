import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class LocalPaths:
    # パス
    ## プロジェクトルートの絶対パス
    BASE_DIR: str = os.path.abspath("./")
    ## dataディレクトリまでの絶対パス
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    ### HTMLディレクトリのパス
    HTML_DIR: str = os.path.join(DATA_DIR, "html")
    HTML_RACE_DIR: str = os.path.join(HTML_DIR, "race")
    HTML_HORSE_DIR: str = os.path.join(HTML_DIR, "horse")
    HTML_PED_DIR: str = os.path.join(HTML_DIR, "ped")
    ### 段階オッズ取得の生 HTML 保存ディレクトリ（race_id/フェーズ単位）
    HTML_ODDS_DIR: str = os.path.join(HTML_DIR, "odds")
    ### netkeiba の馬ページ解析テーブルの正本（UrlPaths.horse_results_table/horse_info_table の
    ### 出力先＝attr[4]/attr[5]）。backfill-horses はここに書く。RAW_* とは別物なので
    ### 取得済み判定・残頭数カウントはこちらを見る（raw を見ると常に空→無限ストール）。
    HTML_HORSE_RESULTS_PATH: str = os.path.join(HTML_DIR, "horse_results", "horse_results.pkl")
    HTML_HORSE_INFO_PATH: str = os.path.join(HTML_DIR, "horse_info", "horse_info.pkl")

    ### rawディレクトリのパス
    RAW_DIR: str = os.path.join(DATA_DIR, "raw")
    RAW_RESULTS_PATH: str = os.path.join(RAW_DIR, "results.pkl")
    RAW_RACE_INFO_PATH: str = os.path.join(RAW_DIR, "race_info.pkl")
    RAW_RETURN_TABLES_PATH: str = os.path.join(RAW_DIR, "return_tables.pkl")
    RAW_HORSE_RESULTS_PATH: str = os.path.join(RAW_DIR, "horse_results.pkl")
    RAW_HORSE_INFO_PATH: str = os.path.join(RAW_DIR, "horse_info.pkl")
    RAW_PEDS_PATH: str = os.path.join(RAW_DIR, "peds.pkl")
    ### レース当日ノート（無料・リーク無し）の集約 pickle
    RAW_TRAINING_PATH: str = os.path.join(RAW_DIR, "training.pkl")
    RAW_PADDOCK_PATH: str = os.path.join(RAW_DIR, "paddock.pkl")
    RAW_COMMENT_PATH: str = os.path.join(RAW_DIR, "comment.pkl")
    ### 予想印（無料＋プレミアム・リーク無し＝発走前）のロング集約 pickle
    RAW_YOSO_MARKS_PATH: str = os.path.join(RAW_DIR, "yoso_marks.pkl")
    ### 人物（騎手/調教師/馬主/生産者）の年度別成績（as-of で結合）
    RAW_PERSON_YEARLY_PATH: str = os.path.join(RAW_DIR, "person_yearly.pkl")
    ### 予想家プロフィール由来のスキル prior（直近ログ集計・予想家×1行）
    RAW_YOSO_PREDICTOR_PATH: str = os.path.join(RAW_DIR, "yoso_predictor.pkl")
    ### 段階オッズ スナップショットの集約 pickle（Layer2 学習データの蓄積先）
    RAW_ODDS_SNAPSHOT_PATH: str = os.path.join(RAW_DIR, "odds_snapshots.pkl")
    ### 馬の Elo レーティングの最新スナップショット（horse_id→{rating,n_races}）。
    ### featured 構築時に書き出し、ライブ予測（未来レース）の特徴量再現に使う。
    HORSE_RATINGS_PATH: str = os.path.join(RAW_DIR, "horse_ratings.json")
    ### ライブ推論用 履歴スナップショット（results 履歴由来の person_te / form を serve で再計算する）
    SERVE_HISTORY_PATH: str = os.path.join(RAW_DIR, "serve_history.pkl")
    ### オッズ力学モデルの予測テーブル（チェックポイント別の次時点/確定予測）
    RAW_ODDS_PREDICTIONS_PATH: str = os.path.join(RAW_DIR, "odds_predictions.pkl")
    ### 前処理済み特徴量データ（FeatureEngineering 出力、再学習の入力）
    FEATURED_DATA_PATH: str = os.path.join(RAW_DIR, "featured_data.pkl")
    ### Phase 2: dtype を完全保持する Parquet バックアップ（pyarrow が必要）
    FEATURED_DATA_PARQUET_PATH: str = os.path.join(RAW_DIR, "featured_data.parquet")

    ### Phase 1: raw データの永続化先 SQLite ファイル
    ### pickle が消えても DB から再生成できるように、scrape 結果をここに upsert する。
    DB_PATH: str = os.path.join(DATA_DIR, "keibam.db")

    ### masterディレクトリのパス
    MASTER_DIR: str = os.path.join(DATA_DIR, "master")
    MASTER_RAW_HORSE_RESULTS_PATH: str = os.path.join(MASTER_DIR, "horse_results_updated_at.csv")

    ### 卍式（①.5 補正）成果物のパス
    # ①（featured）は不変のまま、卍補正の共有成果物をここに置く（全シナリオ共有）。
    MANJI_DIR: str = os.path.join(DATA_DIR, "manji")
    # ①.5a ファクター事前計算表: key=(race_id, 馬番) の因子バケット＋近走/通算派生（1回だけ生成）
    MANJI_FACTOR_TABLE_PATH: str = os.path.join(MANJI_DIR, "factor_table.pkl")
    # ①.5b ベイズ事後分布ストア: (factor,bucket)→事後(n,neff,post_mean,post_var,point)。
    # 「as-of 日付」までの証拠のみで構成（前進安全）。全シナリオ共有・要素の線形結合で補正列を作る。
    MANJI_POSTERIOR_STORE_PATH: str = os.path.join(MANJI_DIR, "posterior_store.pkl")
