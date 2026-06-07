# KeibaAM — 競馬予測・馬券最適化システム

netkeiba のレースデータを収集・前処理し、機械学習（LightGBM × ニューラルネットの
スタッキング）で勝率を予測、期待値（EV）ベースで馬券を選定し、ケリー基準で資金配分する
までを一気通貫で扱うシステム。バックテスト（シミュレーション）と Streamlit ダッシュボード、
VPS 上での継続学習・段階オッズ取得（cron 運用）に対応する。

設計の詳細は [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) を参照。

---

## 1. 必要環境

- **Python 3.11**
- OS: Linux / macOS（VPS 運用は Linux 想定）
- スクレイピングを行う場合のみ: **Playwright (Chromium ヘッドレス)**

---

## 2. セットアップ

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
```

### スクレイピングを使う場合（任意）

データ収集（`src/preparing/`）は Playwright を使う。実データ取得や VPS 運用をする場合のみ
以下を追加で実行する（テスト・学習・アプリ表示だけなら不要）。

```bash
playwright install chromium        # Chromium 本体を取得（必須）
playwright install-deps chromium   # Linux で不足する共有ライブラリを補う（必要時）
```

#### Ubuntu 26.04 の場合（公式未対応 → 24.04 ビルドで代替）

Playwright 1.60.0 時点で Ubuntu 26.04 はまだ公式サポート外のため、上記コマンドはそのまま
失敗する。以下の手順で回避する:

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

> **注意点:**
> - `PLAYWRIGHT_HOST_PLATFORM_OVERRIDE` は `install`（ダウンロード）時だけ必要。
>   `launch()` でブラウザを使う際には不要（コード変更不要）。
> - インストールは一度成功すれば `~/.cache/ms-playwright/` にキャッシュされるので
>   再実行は不要。バージョン更新時のみ再度 override 付きで実行する。
> - `playwright install-deps` は 26.04 で失敗するため、代わりに上記 `apt install` で代替する。

VPS での詳細手順は [`docs/setup_vps.md`](docs/setup_vps.md) を参照。

### 環境変数

`PYTHONPATH` をリポジトリルートに通す（`src` / `app` を import 可能にするため）。

```bash
export PYTHONPATH="."
```

---

## 3. 検証（テスト・静的解析）

CI（[`.github/workflows/ci.yml`](.github/workflows/ci.yml)）と同じ 4 ゲート。
ローカルでも以下を順に実行すれば同等の検証ができる。

```bash
# ① ユニット + アーキテクチャテスト（478 件）
#    architecture テストが import-linter の 4 契約も subprocess で検証する
python -m pytest tests/ -q

# ② Lint（ruff: E/F/W/B ルール）
python -m ruff check src/ app/

# ③ 型チェック（mypy: pyproject.toml の files に列挙した境界モジュールを段階検査）
python -m mypy --config-file pyproject.toml

# ④ レイヤ依存契約（import-linter）※ ① に内包されるが単独実行も可
lint-imports
```

期待結果の目安: pytest は **pass（一部 selenium/Playwright 必須テストは skip）**、
mypy は対象モジュールで **clean**、import-linter は **4 contracts kept**。

> 補足: `tests/preparing/` のスクレイピング実依存テストは `@pytest.mark.skip` で
> 個別除外済みのため、ブラウザ未導入でもテストは通る。

---

## 4. 実行手順

### 4-1. Streamlit ダッシュボード

予測・オッズ監視・成績可視化・設定編集を行う UI。

```bash
streamlit run app/Home.py
```

マルチページ構成（`app/pages/` から自動ナビゲーション生成）:
ダッシュボード / 予測 / オッズ監視 / 成績・設定。

### 4-2. 継続学習パイプライン（CLI）

VPS の cron から日次取込・週次再学習を回す想定。

```bash
# 日次: 終了したレースの結果・払戻を raw pickle へ冪等取込
python -m src.pipeline.run_pipeline --job ingest \
    --race-id 202401010101 202401010102

# 週次: 全データで再学習（--with-tuning で Optuna 探索、--no-stacking で LightGBM のみ）
python -m src.pipeline.run_pipeline --job retrain
```

### 4-3. 段階オッズ取得スケジューラ（CLI）

過去の連オッズは遡及取得できないため、`OddsSnapshot` を「今から」段階的に収集・蓄積する。
前日 / 数時間前 / 30分前 / 直前 の各タイミングで cron 起動する。

```bash
python -m src.preparing.odds_scheduler --phase just_before \
    --race-id 202401010101 --post-time 2024-01-01T15:40 --bet-type tansho
```

`--post-time`（発走時刻）はフェーズ判定の基準。複数レースは `--race-id` を繰り返す。

### 4-4. ノートブック（探索・実験）

```bash
jupyter lab        # main.ipynb 等を開く
```

---

## 5. 設定

運用パラメータは [`config.yaml`](config.yaml) で管理（`src.operation.OperationConfig` が読込）。

| 項目 | 説明 |
|---|---|
| `operation_mode` | `advisory`（推奨表示のみ・既定） / `semi_auto`（購入リスト出力） / `full_auto`（自動発注・既定無効） |
| `bankroll` | 総資金 |
| `kelly_fraction_ratio` | フラクショナル・ケリー係数（0 < r ≤ 1） |
| `per_bet_cap_ratio` | 1 馬券あたり上限（bankroll 比） |
| `max_daily_ratio` | 1 日総投資上限（bankroll 比） |

ログは標準で stdout、必要に応じてファイル出力（`src.constants._logging_config.setup_logging`）。

---

## 6. プロジェクト構成

```
src/                ドメイン本体（レイヤ単方向依存）
├─ constants/       定数・列名・閾値・馬券種・ログ設定（最下層・他レイヤ非依存）
├─ preprocessing/   前処理（結果/払戻/血統/特徴量エンジニアリング）
├─ preparing/       データ収集アダプタ（Playwright スクレイパ・オッズ取得）
├─ policies/        馬券/スコア戦略・Harville 確率・オッズ供給
├─ training/        学習器（KeibaAI / スタッキング / sample weights）
├─ portfolio/       資金配分（ケリー）
├─ simulation/      バックテスト・指標・可視化
├─ operation/       運用モード・発注アダプタ・設定
└─ pipeline/        継続学習 CLI（ingest / retrain）
app/                Streamlit UI（Home + pages/）
tests/              ユニット + アーキテクチャテスト
docs/               ARCHITECTURE.md / setup_vps.md
```

レイヤ依存の方向（下位 → 上位、上位は下位のみ import 可）:

```
constants → preprocessing / preparing → policies → training → portfolio → simulation → operation → pipeline → ui
```

この制約は `tests/architecture/` と `import-linter`（`.importlinter`）で機械的に検証される。

---

## 7. 関連ドキュメント

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — レイヤ設計・抽象境界・DI・ADR
- [`docs/setup_vps.md`](docs/setup_vps.md) — VPS（Playwright）セットアップ・cron 運用
