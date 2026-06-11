#!/usr/bin/env bash
# daily_ingest.sh — 前日の終了レースを DB に取込む（cron 日次ジョブ）
#
# 使用例:
#   ./scripts/daily_ingest.sh              # 昨日分（既定）
#   ./scripts/daily_ingest.sh 20240101     # 日付を明示指定
#
# crontab 例（毎朝 6:00 に前日分を取込む）:
#   0 6 * * * /path/to/keibam/scripts/daily_ingest.sh >> /path/to/keibam/logs/cron.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 失敗通知ヘルパーを読み込む
# shellcheck source=scripts/on_failure_notify.sh
source "$SCRIPT_DIR/on_failure_notify.sh"

# 引数で日付を受け取る。省略時は前日（date -d は GNU coreutils、macOS は date -v-1d）
if [[ $# -ge 1 ]]; then
    POST_DATE="$1"
else
    POST_DATE="$(date -d 'yesterday' '+%Y%m%d' 2>/dev/null || date -v-1d '+%Y%m%d')"
fi

LOG_FILE="$LOG_DIR/ingest_${POST_DATE}.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# 失敗時に通知を送る trap
trap 'notify_failure "daily_ingest" "$POST_DATE" "$LOG_FILE"' ERR

echo "[$TIMESTAMP] === daily_ingest START post_date=$POST_DATE ===" | tee -a "$LOG_FILE"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

python -m src.pipeline.run_pipeline ingest \
    --post-date "$POST_DATE" \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] === daily_ingest DONE (exit=0) ===" | tee -a "$LOG_FILE"
else
    echo "[$TIMESTAMP] === daily_ingest FAILED (exit=$EXIT_CODE) ===" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
