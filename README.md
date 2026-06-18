# KeibaAM — 競馬予測・馬券最適化システム

netkeiba のレースデータを収集・前処理し、機械学習（LightGBM × ニューラルネットの
スタッキング）で勝率を予測、期待値（EV）ベースで馬券を選定し、ケリー基準で資金配分・
発注・清算までを一気通貫で扱うシステム。バックテスト（シミュレーション）、
Streamlit ダッシュボード、VPS 上での継続学習・時系列オッズ自動取得（cron 運用）に対応する。

設計の詳細は [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) を参照。

## 主要機能

| 機能 | 概要 |
|---|---|
| **データ収集** | netkeiba からレース結果・馬情報・血統・払戻を取得。**1973 年〜現在まで全年代のページ構造に対応**。新規レース・新規馬は ingest が自動差分取得 |
| **二重永続化** | raw データは pickle（キャッシュ）+ SQLite（`data/keibam.db`、冪等 upsert）。pickle が消えても DB から自動復元 |
| **学習** | LightGBM + NN スタッキング + Isotonic 較正。Optuna 探索の**全 trial を成績順に保存**し、任意の rank のパラメータで再学習可能 |
| **オッズ力学モデル** | 単勝市場を**投票シェアベクトル（Σp=1）の確率過程**として扱い、Dirichlet 回帰 / Kalman Filter / Particle Filter / アンサンブルで締切オッズを予測。人気順位別の経験的遷移（市場の重力）を事前分布として共有 |
| **時系列オッズ自動取得** | 発走 30/10/5/1 分前のチェックポイントでタイマー取得 → 取得のたびに予測を自動再計算。データソースは抽象化済み（netkeiba 既定 / **JRA-VAN Data Lab 受信契約**定義済み） |
| **EV 馬券選定** | 較正勝率 × オッズ → EV 閾値選定 → フラクショナル・ケリー配分。`use_predicted_odds` で予測確定オッズによる EV 計算に切替可能 |
| **発注 UI** | カート管理 → 資金上限チェック → IPAT 入力支援テキスト/CSV 出力 → 発注記録 → 実払戻による自動清算（回収率算出） |
| **UI（Streamlit）** | ダッシュボード / 予測・推奨 / オッズ推移・予測照会 / 成績・設定 / モデルラボ（ハイパラ選択・モデル比較バックテスト・オッズ力学評価）/ 発注 |
| **cron 運用** | 日次取込・週次再学習・オッズ自動取得・失敗通知（Slack/メール） |

---

## 1. 必要環境

- **Python 3.11**
- OS: Linux / macOS（VPS 運用は Linux 想定）
- スクレイピングを行う場合のみ: **Playwright (Chromium ヘッドレス)**

---

## 2. セットアップ手順

```bash
# 1) リポジトリ取得
git clone https://github.com/nmatsubara8/keibam.git
cd keibam

# 2) 仮想環境（推奨）
python3.11 -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate

# 3) 依存インストール
pip install -r requirements.txt          # アプリ/学習/スクレイピング一式
pip install -r requirements-dev.txt      # テスト・静的解析ツール（pytest / ruff / mypy / import-linter）

# 4) 環境変数（src / app を import 可能にする）
export PYTHONPATH="."
```

> 学習（`retrain --with-tuning` / `--params-rank`）には `optuna-integration[lightgbm]`
> が必要（`requirements.txt` に含まれる）。

### スクレイピングを使う場合（任意）

データ収集（`src/preparing/`）は Playwright を使う。実データ取得や VPS 運用をする場合のみ
以下を追加で実行する（テスト・学習・アプリ表示だけなら不要）。

```bash
playwright install chromium        # Chromium 本体を取得（必須）
playwright install-deps chromium   # Linux で不足する共有ライブラリを補う（必要時）
```

#### TLS 検査プロキシ配下の環境（CI・クラウドサンドボックス等）

プロキシが TLS を中間検査する環境では Chromium が `ERR_CERT_AUTHORITY_INVALID` になる。
その場合のみ次の環境変数でオプトインする（通常の VPS では**不要**。証明書検証は既定で有効）:

```bash
export KEIBAM_IGNORE_HTTPS_ERRORS=1
```

#### Ubuntu 26.04 の場合（公式未対応 → 24.04 ビルドで代替）

Playwright 1.60.0 時点で Ubuntu 26.04 はまだ公式サポート外のため、以下で回避する:

```bash
# 1. OS依存ライブラリを apt で入れる
sudo apt install -y \
  libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 \
  libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
  libxrandr2 libgbm1 libasound2t64 libpango-1.0-0 libcairo2 \
  libatspi2.0-0 libx11-6 libxcb1 libxext6

# 2. Ubuntu 24.04 用ビルドを明示指定してブラウザ本体を取得
PLAYWRIGHT_HOST_PLATFORM_OVERRIDE=ubuntu24.04-x64 playwright install chromium
```

> `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE` は `install` 時だけ必要。一度成功すれば
> `~/.cache/ms-playwright/` にキャッシュされる。

VPS での詳細手順は [`docs/setup_vps.md`](docs/setup_vps.md) を参照。

### 初回データ準備

リポジトリには `data/raw/*.pkl`（2023〜2026 年の実データ）が含まれるため、
クローン直後から学習・バックテスト・UI が動く。SQLite（`data/keibam.db`）は
ingest / retrain の初回起動時に pickle から自動移行される（`auto_migrate_all`）。

モデル pickle（数百 MB）は git 管理外のため、初回に一度だけ再学習を実行する:

```bash
python -m src.pipeline.run_pipeline retrain     # 約 3 分（チューニングなし）
```

---

## 3. 検証手順（テスト・静的解析）

CI（[`.github/workflows/ci.yml`](.github/workflows/ci.yml)）と同じ 4 ゲート。
ローカルでも以下を順に実行すれば同等の検証ができる。

```bash
# ① ユニット + 統合 + アーキテクチャテスト
python -m pytest tests/ -q

# ② Lint（ruff: E/F/W/B ルール。CI では必須ゲート）
python -m ruff check src/ app/

# ③ 型チェック（mypy: pyproject.toml の files に列挙した境界モジュールを段階検査）
python -m mypy --config-file pyproject.toml

# ④ レイヤ依存契約（import-linter）※ ① に内包されるが単独実行も可
lint-imports
```

期待結果の目安: pytest は **750+ 件 pass**（torch 未導入で 4 件 / ネットワーク必須 1 件が skip）、
ruff / mypy は **clean**、import-linter は **4 contracts kept**。

主な検証カバレッジ:

- **年代別パーサ**（`tests/preparing/test_era_parsers.py`）— 1970s〜現在のページ構造差を合成フィクスチャで再現
- **オッズ力学モデル**（`tests/training/test_odds_dynamics*.py`）— 合成シンプレックスウォークで
  Kalman が恒等予測に勝つこと・Dirichlet のパラメータ回復・PF≈Kalman 等を検証
- **取得→再計算パイプライン**（`tests/pipeline/test_odds_watch.py`）— スタブソースで 1 サイクル E2E
- **発注サービス**（`tests/app/test_order_service.py`）— カート・IPAT 出力・清算
- **ライブ予想 E2E**（`tests/integration/`）— 実データ + 実モデルで特徴量整合とスコア計算
  （データ/モデルが無い CI 環境は自動 skip）

### 実データでの E2E 検証（任意・要ネットワーク）

```bash
# 実 netkeiba から指定日のレースを取込み、特徴量再生成まで通す
python -m src.pipeline.run_pipeline ingest --post-date 20260607

# 取込後にデータ整合（pickle ↔ SQLite の行数一致等）はテストで確認できる
python -m pytest tests/integration/ -q
```

---

## 4. 利用手順

### 4-1. データ取込（日次）

```bash
# 指定日の全レースを自動検出して取込（レース結果・払戻・新規馬の馬情報/血統も差分取得）
python -m src.pipeline.run_pipeline ingest --post-date 20260607

# race_id 個別指定
python -m src.pipeline.run_pipeline ingest --race-id 202605030211

# 誤情報修正時: DB 行を事前 DELETE して再投入
python -m src.pipeline.run_pipeline ingest --race-id 202605030211 --force
```

ingest は「新規レースの HTML 取得 → テーブル化 → 既存 pickle へキー付きマージ →
SQLite 冪等 upsert → 未知の馬の馬ページ/血統の差分取得 → 特徴量（featured_data）再生成」
まで自動で行う。1973 年までの旧年代レースも取込可能。

### 4-2. 学習（週次）

```bash
# 通常の再学習（スタッキング + 較正、約 3 分）
python -m src.pipeline.run_pipeline retrain

# Optuna ハイパラ探索つき（LightGBMTuner の自動段階探索。探索範囲・回数は固定）
python -m src.pipeline.run_pipeline retrain --with-tuning

# 探索範囲・試行回数を制御する手書き Optuna 探索（method="optuna"）
#   --n-trials を付けると optuna 方式に切替（--with-tuning は自動で有効化）
python -m src.pipeline.run_pipeline retrain --n-trials 100
#   打ち切り秒数を指定
python -m src.pipeline.run_pipeline retrain --n-trials 200 --tuning-timeout 1800
#   探索範囲を JSON 設定ファイルで指定（例: configs/tuning_config.example.json）
python -m src.pipeline.run_pipeline retrain --tuning-config configs/tuning_config.example.json

# 保存済み探索結果から任意の rank のパラメータで学習
python -m src.pipeline.run_pipeline retrain --params-rank 2

# UI（モデルラボ）で選択・保存したパラメータで学習
python -m src.pipeline.run_pipeline retrain --use-selected-params

# マルチ GBDT スタッキング（LightGBM + XGBoost + CatBoost + NN を base に）
python -m src.pipeline.run_pipeline retrain --use-stacking \
    --base-models-config configs/base_models_nn.example.json
```

**base 学習器・meta 学習器の構成（`--base-models-config <JSON>`）**

`configs/base_models_*.json` で base 学習器の種類と meta 学習器を切り替える:

- `models`: `lightgbm` / `xgboost` / `catboost` / `nn` の組み合わせ
- `meta_model`: スタッキング 2 段目の学習器
  - `"logistic"`（既定）— LogisticRegression。線形結合で堅牢
  - `"lightgbm"` — 浅い GBDT meta。base が多様なとき非線形な組み合わせを学習し改善し得る
    （例: `configs/base_models_nn_gbdt_meta.json`）。meta 特徴量は base 予測確率の
    数列のみと低次元のため、既定は `num_leaves=3` の極浅構成で過学習を抑える。
    `meta_params` で上書き可能

### 4-3. ダッシュボード（Streamlit UI）

```bash
streamlit run app/Home.py
```

| ページ | 内容 |
|---|---|
| 📊 ダッシュボード | データ・モデルの状態サマリ |
| 🎯 予測・推奨 | レース選択 → 較正勝率 × EV → ケリー推奨額。**「🛒 発注カートへ追加」**で発注ページへ連携 |
| 📈 オッズ推移 | スナップショットの推移グラフ + **オッズ力学モデル予測の照会**（各時点の実績 vs 次時点・確定予測のマトリクス） |
| 🏆 成績・設定 | 回収率推移 / AUC / 特徴量重要度 / 較正プロット / config.yaml 編集 |
| 🧪 モデルラボ | ①Optuna 探索結果を成績順に表示・**使用パラメータの選択** ②複数モデルの**同一条件バックテスト比較**（回収率・的中率・シャープレシオ・資金推移） ③**オッズ力学モデルの精度比較**（KL/勝ち馬 logloss/MAE/MAPE + アンサンブル重み） |
| 🛒 発注 | 下記 4-5 参照 |

### 4-4. 時系列オッズの自動取得・予測再計算

単勝市場を投票シェアベクトルの確率過程として扱うオッズ力学モデル
（Dirichlet / Kalman / Particle / アンサンブル）が、チェックポイント
（**発走 30/10/5/1 分前**）ごとのオッズ取得のたびに「次時点」「発走時（確定）」の
予測を自動再計算する。

```bash
# 1 サイクル実行（cron 用。チェックポイント到来レースが無ければ即終了）
python -m src.pipeline.odds_watch --once

# 常駐モード（2 分間隔）
python -m src.pipeline.odds_watch --loop --interval 120

# データソース切替（既定 netkeiba。JRA-VAN はファイル連携の受信契約のみ定義済み）
python -m src.pipeline.odds_watch --once --source jravan
```

予測は `data/raw/odds_predictions.pkl` + SQLite `raw_odds_predictions` に保存され、
「📈 オッズ推移」ページの照会マトリクスに表示される。

```bash
# スナップショット蓄積後（目安 4〜8 週）: モデル比較評価 + 重力統計・アンサンブル重みの更新
python -m src.pipeline.run_pipeline evaluate-odds-dynamics
```

評価は KL / シェア MAE / オッズ MAPE に加え、**勝ち馬 log-loss**（results の着順 1 着馬を
正解とした予測シェアの負対数尤度）も算出し、モデルラボの「オッズ力学モデル」タブに表示する
（results 未取得時のみ NaN）。結果は `models/odds_dynamics_eval.json` /
`models/odds_gravity.json`（いずれも実行時生成・gitignore）に保存される。

予測確定オッズを期待値計算に使うには `config.yaml` に `use_predicted_odds: true` を設定する
（予測が無いレース/馬は現在オッズへ自動フォールバック）。

> **JRA-VAN Data Lab 連携**: JV-Link は Windows 専用 COM のため、Windows 機の
> エージェント（別途実装）が速報オッズ JSON を `data/incoming/jravan/` に置く方式。
> JSON スキーマは `src/preparing/_odds_source.py` の docstring を参照。

### 4-5. 馬券発注フロー

1. **予測**: 「🎯 予測・推奨」でレースを選び、推奨馬券を確認 → 「🛒 発注カートへ追加」
2. **編集**: 「🛒 発注」ページで金額（100 円単位）を編集・発注対象を選択。
   カート合計は当日上限（`bankroll × max_daily_ratio`）でブロックされる
3. **発注**（`config.yaml` の `operation_mode` で動作が変わる）:
   - `advisory` — 推奨として履歴記録のみ（購入は人間）
   - `semi_auto` — **IPAT 入力支援テキスト**（場名/R/式別/買い目/金額）と発注票 CSV を出力 →
     IPAT で投票後「🎫 発注済みとして記録」
   - `full_auto` — 既定無効（規約・法的リスク）
4. **清算**: レース結果の取込（ingest）後に「🧾 結果で清算する」→ 実払戻テーブルから
   payout/的中を自動計算し、回収率・的中数を表示

### 4-6. cron 運用（VPS）

[`crontab.example`](crontab.example) をベースに設定する:

```cron
# 日次取込（毎朝 6:00 に前日分）
0 6 * * * $PROJECT_DIR/scripts/daily_ingest.sh >> $PROJECT_DIR/logs/cron.log 2>&1

# 週次再学習（毎週月曜 3:00。--selected-params で UI 選択ハイパラを使用）
0 3 * * 1 $PROJECT_DIR/scripts/weekly_retrain.sh >> $PROJECT_DIR/logs/cron.log 2>&1

# 時系列オッズ取得 + 予測自動再計算（開催日 9〜16 時に 2 分間隔）
*/2 9-16 * * 6,0 $PROJECT_DIR/scripts/odds_snapshot.sh >> $PROJECT_DIR/logs/cron.log 2>&1

# オッズ力学モデルの週次評価
0 22 * * 0 cd $PROJECT_DIR && python -m src.pipeline.run_pipeline evaluate-odds-dynamics >> $PROJECT_DIR/logs/cron.log 2>&1
```

失敗時は `scripts/on_failure_notify.sh` 経由で通知できる
（環境変数 `NOTIFY_SLACK_WEBHOOK` または `NOTIFY_EMAIL` を設定）。

### 4-7. ノートブック（探索・実験）

```bash
jupyter lab        # main.ipynb 等を開く
```

---

## 5. 設定

運用パラメータは [`config.yaml`](config.yaml) で管理（`src.operation.OperationConfig` が読込。
UI の「成績・設定」ページからも編集可能）。

| 項目 | 説明 |
|---|---|
| `operation_mode` | `advisory`（推奨表示のみ・既定） / `semi_auto`（IPAT 発注票出力） / `full_auto`（自動発注・既定無効） |
| `bankroll` | 総資金 |
| `kelly_fraction_ratio` | フラクショナル・ケリー係数（0 < r ≤ 1） |
| `per_bet_cap_ratio` | 1 馬券あたり上限（bankroll 比） |
| `max_daily_ratio` | 1 日総投資上限（bankroll 比。発注 UI のブロック基準） |
| `use_predicted_odds` | オッズ力学モデルの予測確定オッズで EV を計算（既定 false） |

ログは標準で stdout、必要に応じてファイル出力（`src.constants._logging_config.setup_logging`）。

### 生成物の置き場所

| パス | 内容 |
|---|---|
| `data/raw/*.pkl` | raw データ（git 管理。スクレイプ結果のキャッシュ） |
| `data/keibam.db` | SQLite ミラー（git 管理外。pickle 揮発時の保険） |
| `data/betting_history.jsonl` | 投票履歴（発注 UI が記録・清算） |
| `data/order_basket.json` | 発注カート |
| `models/<date>/*.pickle` | 学習済みモデル（git 管理外。retrain で再生成） |
| `models/version_history.json` | モデルバージョン履歴（AUC 等） |
| `models/tuning_history.json` | Optuna 探索結果（成績順） |
| `models/odds_gravity.json` | 市場の重力（人気順別遷移統計） |
| `models/odds_dynamics_eval.json` | オッズ力学モデルの評価結果 |

---

## 6. プロジェクト構成

```
src/                ドメイン本体（レイヤ単方向依存）
├─ constants/       定数・列名・閾値・馬券種・オッズフェーズ/力学定数（最下層・他レイヤ非依存）
├─ storage/         raw データ SQLite 永続化（pickle 揮発時の保険・自動移行）
├─ preprocessing/   前処理（結果/払戻/血統/特徴量エンジニアリング）
├─ preparing/       データ収集アダプタ（Playwright スクレイパ・オッズソース抽象化・スケジューラ）
├─ policies/        馬券/スコア戦略・Harville 確率・オッズ供給（実/予測）
├─ training/        学習器（KeibaAI / スタッキング / チューニング履歴 / オッズ力学モデル）
├─ portfolio/       資金配分（ケリー）
├─ simulation/      バックテスト・指標・可視化
├─ operation/       運用モード・発注 executor・設定
└─ pipeline/        継続学習 CLI（ingest / retrain / evaluate-odds-dynamics）・odds_watch
app/                Streamlit UI（Home + pages/ + サービスロジック _*.py）
scripts/            cron 用シェル（daily_ingest / weekly_retrain / odds_snapshot / 失敗通知）
tests/              ユニット + 統合 + アーキテクチャテスト
docs/               ARCHITECTURE.md / setup_vps.md
```

レイヤ依存の方向（下位 → 上位、上位は下位のみ import 可）:

```
constants → storage → preprocessing / preparing → policies → training → portfolio → simulation → operation → pipeline → ui
```

この制約は `tests/architecture/` と `import-linter`（`.importlinter`）で機械的に検証される。

---

## 7. 設計ハイライト

### 7-1. 全年代対応のデータ取得（1973〜現在）

netkeiba のページ構造は年代で異なる（空テーブル・払戻の表構成・旧条件クラス名
「500万下」等・英字混じり馬 ID・リンク欠落行・JS 描画化）。パーサは `summary` 属性
ベースのテーブル特定・行単位の ID 抽出・旧クラス名の正規化等で全年代を吸収する。
レート制限による空レスポンスは自動リトライする。

### 7-1b. netkeiba への自主規制（ポライトネス制御）

netkeiba はスクレイピング起因の通信制限を公式に案内しており、2024 年 11 月の
クローラー対策強化以降は User-Agent 未設定だと HTTP 400 が返る。本プロジェクトは
`src/preparing/_rate_limiter.py` に自主規制を一元化している:

- **User-Agent**: 実ブラウザ相当の UA を全リクエストに設定（`_scraper.py`）
- **リクエスト間隔**: 最低 1 秒 + ランダム揺らぎ（既定で合計 1〜3 秒程度）
- **時間あたり上限**: 全リクエストが通る `PlaywrightScraper.fetch` で
  1 時間あたりのリクエスト数をスライディングウィンドウで自主制限（既定 1,000 件）

環境変数で調整できる（大量取得時も既定値の緩和は非推奨）:

| 変数 | 既定 | 意味 |
|---|---|---|
| `KEIBA_SCRAPE_DELAY` | 1.0 | 基準待機秒（1.0 未満の正値は 1.0 に切上げ、0 以下で無効） |
| `KEIBA_SCRAPE_JITTER_MAX` | 2.0 | 揺らぎ上限秒（uniform(0, max) を加算） |
| `KEIBA_MAX_REQUESTS_PER_HOUR` | 1000 | 1 時間あたり上限（0 以下で無効） |

### 7-2. pickle + SQLite の二重永続化

scrape 結果は pickle（既存 Processor の読込互換）と SQLite（`INSERT OR IGNORE` の
冪等 upsert）へ二重書き。pickle が消えても DB から自動復元され、再スクレイプ不要。
ingest/retrain 起動時に `auto_migrate_all` が空テーブルへ自動移行する。

### 7-3. オッズ力学モデル（投票シェアの確率過程）

単勝オッズはレース全体で総投票額を奪い合うゼロサム構造のため、馬ごとの独立回帰では
なくシェアベクトル p（Σp=1）を CLR 変換した空間で状態推定する。
人気順位バケット別の遷移統計（市場の重力: 1 番人気は収束しやすく、大穴は粗く動く）を
縮小推定して全モデルの事前分布に使う。アンサンブル重みは検証 KL の逆数比。
データ不足時は恒等予測（効率的市場ベースライン）へ安全に退化する。

### 7-4. 発注の安全設計

運用モードは段階制（advisory → semi_auto → full_auto）で、自動発注は既定無効。
資金は二重ガード（1 馬券上限 + 当日上限）。発注記録は JSON Lines に追記し、
結果確定後にバックテストと同一の払戻計算（`BettingTickets`）で清算する。

---

## 8. 関連ドキュメント

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — レイヤ設計・抽象境界・DI・ADR
- [`docs/setup_vps.md`](docs/setup_vps.md) — VPS（Playwright）セットアップ・cron 運用
- [`.claude-context.md`](.claude-context.md) — 開発履歴・修正の詳細ログ
- [`crontab.example`](crontab.example) — cron 設定例
