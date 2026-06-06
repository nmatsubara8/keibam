# VPS セットアップ手順（Playwright スクレイピング）

§4 の全面移行により、スクレイピングは **Playwright（Chromium ヘッドレス）** に統一した。
selenium / webdriver-manager は依存から外し、`playwright` に一本化している。

## 1. 依存インストール

```bash
pip install -r requirements.txt
# Chromium ブラウザ本体を取得（Playwright 必須手順）
playwright install chromium
# Linux で不足しがちな共有ライブラリも入れる場合
playwright install-deps chromium
```

`playwright install chromium` を忘れると、実行時に「Executable doesn't exist」エラーになる。

## 2. ヘッドレス動作確認

```bash
python -c "
import asyncio
from src.preparing._scraper import PlaywrightScraper

async def main():
    async with PlaywrightScraper(headless=True) as s:
        html = await s.fetch('https://example.com')
        print('OK len=', len(html))

asyncio.run(main())
"
```

## 3. スクレイパーの使い方

### 同期パイプライン（既存コード）からの呼び出し
`AbstractScraper.fetch_sync(url)` / `fetch_many_sync(urls)` が `asyncio.run` 境界を
内包する。既存の同期処理（`modules.py` の各 scrape 関数）はこの境界経由で動作する。

### 非同期（並列取得）
段階オッズ取得など並列が効く処理は async を直接使う:

```python
import asyncio
from src.preparing._scraper import PlaywrightScraper

async def main():
    async with PlaywrightScraper() as s:
        htmls = await asyncio.gather(*(s.fetch(u) for u in urls))
```

### ページネーション（騎手・調教師リーディング等）
```python
pages = await scraper.scrape_paginated(base_url, page_param="page", max_pages=10)
```
空ページを検出した時点で自動的に打ち切る（過剰リクエスト防止）。

## 4. 段階オッズスケジューラ（cron）

```bash
# 直前フェーズの単勝オッズを取得
python -m src.preparing.odds_scheduler --phase just_before \
    --race-id 202401010101 --post-time 2024-01-01T15:40 --bet-type tansho
```

`--waiting-time` は Playwright 移行で不要になった（`wait_for_selector` で描画完了を
判定）。既存 cron コマンド互換のため引数自体は受け付けるが内部では無視される。

### crontab 例（`%` のエスケープに注意）
```cron
# 毎日 6:00 に日次取込（date の % は \% にエスケープする）
0 6 * * * cd /path/to/keibam && /usr/bin/python -m src.pipeline.run_pipeline ingest --race-id ... >> logs/ingest.log 2>&1
```

## 5. バックグラウンド実行（SSH 切断耐性）

```bash
nohup python -m src.pipeline.run_pipeline ingest --race-id ... >> logs/ingest.log 2>&1 &
```

## 補足: なぜ Playwright か（移行理由）
- 非同期ネイティブで `asyncio.gather` による並列取得が容易（段階オッズ × 複数券種に有効）
- Linux VPS でのヘッドレス安定性が高い
- `domcontentloaded` 待機 + `wait_for_selector` で JS 描画完了を確実に判定
- selenium/webdriver-manager 混在を解消し、ライブラリ依存を Playwright のみに集約
