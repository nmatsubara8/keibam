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

    ### rawディレクトリのパス
    RAW_DIR: str = os.path.join(DATA_DIR, "raw")
    RAW_RESULTS_PATH: str = os.path.join(RAW_DIR, "results.pkl")
    RAW_RACE_INFO_PATH: str = os.path.join(RAW_DIR, "race_info.pkl")
    RAW_RETURN_TABLES_PATH: str = os.path.join(RAW_DIR, "return_tables.pkl")
    RAW_HORSE_RESULTS_PATH: str = os.path.join(RAW_DIR, "horse_results.pkl")
    RAW_HORSE_INFO_PATH: str = os.path.join(RAW_DIR, "horse_info.pkl")
    RAW_PEDS_PATH: str = os.path.join(RAW_DIR, "peds.pkl")
    ### 段階オッズ スナップショットの集約 pickle（Layer2 学習データの蓄積先）
    RAW_ODDS_SNAPSHOT_PATH: str = os.path.join(RAW_DIR, "odds_snapshots.pkl")
    ### オッズ力学モデルの予測テーブル（チェックポイント別の次時点/確定予測）
    RAW_ODDS_PREDICTIONS_PATH: str = os.path.join(RAW_DIR, "odds_predictions.pkl")
    ### 前処理済み特徴量データ（FeatureEngineering 出力、再学習の入力）
    FEATURED_DATA_PATH: str = os.path.join(RAW_DIR, "featured_data.pkl")
    ### Phase 2: dtype を完全保持する Parquet バックアップ（pyarrow が必要）
    FEATURED_DATA_PARQUET_PATH: str = os.path.join(RAW_DIR, "featured_data.parquet")

    ### Phase 1: raw データの永続化先 SQLite ファイル
    ### pickle が消えても DB から再生成できるように、scrape 結果をここに upsert する。
    DB_PATH: str = os.path.join(DATA_DIR, "keibam.db")

    ### modelsディレクトリのパス（学習済みモデル・各種スナップショットの保存先）
    MODELS_DIR: str = os.path.join(BASE_DIR, "models")
    ### Phase 1: ペアワイズ Elo レーティングの最新スナップショット（ライブ予測で参照）
    HORSE_RATINGS_PATH: str = os.path.join(MODELS_DIR, "horse_ratings.json")
    ### Phase 2: TrueSkill（μ/σ）の最新スナップショット（ライブ予測で参照）
    HORSE_TRUESKILL_PATH: str = os.path.join(MODELS_DIR, "horse_trueskill.json")
    ### Phase 3: 条件別 TrueSkill（horse×次元×バケット）の最新スナップショット
    HORSE_COND_TRUESKILL_PATH: str = os.path.join(MODELS_DIR, "horse_cond_trueskill.json")

    ### masterディレクトリのパス
    MASTER_DIR: str = os.path.join(DATA_DIR, "master")
    MASTER_RAW_HORSE_RESULTS_PATH: str = os.path.join(MASTER_DIR, "horse_results_updated_at.csv")
