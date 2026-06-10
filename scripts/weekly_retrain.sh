#!/usr/bin/env bash
# weekly_retrain.sh — 全データで週次再学習（cron 週次ジョブ）
#
# 使用例:
#   ./scripts/weekly_retrain.sh            # スタッキング有効（既定）
#   ./scripts/weekly_retrain.sh --tuning   # Optuna ハイパラ探索あり（長時間）
#   ./scripts/weekly_retrain.sh --no-stack # LightGBM 単体（高速確認用）
#
# crontab 例（毎週月曜 3:00 に再学習）:
#   0 3 * * 1 /path/to/keibam/scripts/weekly_retrain.sh >> /path/to/keibam/logs/cron.log 2>&1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 失敗通知ヘルパーを読み込む
# shellcheck source=scripts/on_failure_notify.sh
source "$SCRIPT_DIR/on_failure_notify.sh"

TIMESTAMP_TAG="$(date '+%Y%m%d_%H%M%S')"
LOG_FILE="$LOG_DIR/retrain_${TIMESTAMP_TAG}.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# 失敗時に通知を送る trap
trap 'notify_failure "weekly_retrain" "$TIMESTAMP_TAG" "$LOG_FILE"' ERR

# オプション解析
WITH_TUNING=""
NO_STACKING=""
for arg in "$@"; do
    case "$arg" in
        --tuning)   WITH_TUNING="--with-tuning" ;;
        --no-stack) NO_STACKING="--no-stacking" ;;
    esac
done

echo "[$TIMESTAMP] === weekly_retrain START tuning=${WITH_TUNING:-off} stacking=${NO_STACKING:-on} ===" | tee -a "$LOG_FILE"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

# shellcheck disable=SC2086
python -m src.pipeline.run_pipeline retrain \
    ${WITH_TUNING} \
    ${NO_STACKING} \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] === weekly_retrain DONE (exit=0) ===" | tee -a "$LOG_FILE"
else
    echo "[$TIMESTAMP] === weekly_retrain FAILED (exit=$EXIT_CODE) ===" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
