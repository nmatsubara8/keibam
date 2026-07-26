#!/usr/bin/env bash
# ingest_range.sh — 日付範囲を1日ずつ取込む（レジューム対応・catch-up 用）
#
# daily_ingest.sh は「1日分」を取込むが、数日分をまとめて追いつきたい
# （例: 20260721 以降が未取込）場合はこちらを使う。
#
# 使用例:
#   ./scripts/ingest_range.sh --from 20260721                 # 20260721〜今日
#   ./scripts/ingest_range.sh --from 20260721 --to 20260726   # 範囲を明示（両端含む）
#   ./scripts/ingest_range.sh                                  # resume の続きから今日まで
#   ./scripts/ingest_range.sh --from 20260721 --list-only      # 対象日の確認のみ
#
# 取込完了日は logs/ingest_resume.txt に記録され、再実行時はスキップされる。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 失敗通知ヘルパーを読み込む
# shellcheck source=scripts/on_failure_notify.sh
source "$SCRIPT_DIR/on_failure_notify.sh"

TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
LOG_FILE="$LOG_DIR/ingest_range_$(date '+%Y%m%d_%H%M%S').log"

trap 'notify_failure "ingest_range" "$*" "$LOG_FILE"' ERR

echo "[$TIMESTAMP] === ingest_range START args=$* ===" | tee -a "$LOG_FILE"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

python scripts/ingest_range.py "$@" 2>&1 | tee -a "$LOG_FILE"
EXIT_CODE=${PIPESTATUS[0]}

TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] === ingest_range DONE (exit=0) ===" | tee -a "$LOG_FILE"
else
    echo "[$TIMESTAMP] === ingest_range FAILED (exit=$EXIT_CODE) ===" | tee -a "$LOG_FILE"
fi

exit "$EXIT_CODE"
