#!/usr/bin/env bash
# odds_snapshot.sh — 当日の開催レースのオッズを段階取得する（cron 定期ジョブ）
#
# 使用例:
#   ./scripts/odds_snapshot.sh         # 発走 60 分以内のレースを取得（既定）
#   ./scripts/odds_snapshot.sh 45      # window を 45 分に変更
#   ./scripts/odds_snapshot.sh 600     # 朝イチに当日全レースの hours_before を取得
#
# crontab 例（開催日の 9〜16 時に 10 分間隔 + 朝 8 時に全レース分）:
#   */10 9-16 * * 6,0 /path/to/keibam/scripts/odds_snapshot.sh 45 >> /path/to/keibam/logs/cron.log 2>&1
#   0 8 * * 6,0 /path/to/keibam/scripts/odds_snapshot.sh 600 >> /path/to/keibam/logs/cron.log 2>&1
#
# フェーズ（prev_day / hours_before / thirty_min / just_before）は取得時点の
# minutes_to_post から自動分類されるため、cron 側は起動間隔だけ決めればよい。
# 開催が無い日は取得対象 0 件として即終了する（エラーにならない）。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 失敗通知ヘルパーを読み込む
# shellcheck source=scripts/on_failure_notify.sh
source "$SCRIPT_DIR/on_failure_notify.sh"

WINDOW_MINUTES="${1:-60}"
TODAY="$(date '+%Y%m%d')"
LOG_FILE="$LOG_DIR/odds_snapshot_${TODAY}.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# 失敗時に通知を送る trap
trap 'notify_failure "odds_snapshot" "$TODAY" "$LOG_FILE"' ERR

echo "[$TIMESTAMP] === odds_snapshot START window=${WINDOW_MINUTES}min ===" | tee -a "$LOG_FILE"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

python -m src.preparing.odds_scheduler --auto \
    --window-minutes "$WINDOW_MINUTES" \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] === odds_snapshot DONE (exit=0) ===" | tee -a "$LOG_FILE"
else
    echo "[$TIMESTAMP] === odds_snapshot FAILED (exit=$EXIT_CODE) ===" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
