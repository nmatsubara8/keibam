#!/usr/bin/env bash
# odds_snapshot.sh — 時系列オッズの自動取得・予測自動再計算（cron 定期ジョブ）
#
# odds_watch が発走30分前〜実締切（+10分猶予/確定検知）に入ったレースだけ
# オッズを取得し、取得のたびにオッズ力学モデルで「次時点」「発走時（確定）」の
# 予測を再計算して保存する。チェックポイント外の起動は対象 0 件で即終了（安価）。
#
# crontab 例（開催日の 9〜16 時に 3 分間隔＝発走30分前から3分おき）:
#   */3 9-16 * * 6,0 /path/to/keibam/scripts/odds_snapshot.sh >> /path/to/keibam/logs/cron.log 2>&1
#
# 開催が無い日は取得対象 0 件として即終了する（エラーにならない）。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# 失敗通知ヘルパーを読み込む
# shellcheck source=scripts/on_failure_notify.sh
source "$SCRIPT_DIR/on_failure_notify.sh"

TODAY="$(date '+%Y%m%d')"
LOG_FILE="$LOG_DIR/odds_snapshot_${TODAY}.log"
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

# 失敗時に通知を送る trap
trap 'notify_failure "odds_snapshot" "$TODAY" "$LOG_FILE"' ERR

echo "[$TIMESTAMP] === odds_snapshot START (odds_watch checkpoints) ===" | tee -a "$LOG_FILE"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

# odds_watch: チェックポイント（発走 30/10/5/1 分前）到来レースのオッズ取得 →
# オッズ力学モデル（Dirichlet/Kalman/Particle/Ensemble）で予測を自動再計算 → 保存
python -m src.pipeline.odds_watch --once \
    2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=${PIPESTATUS[0]}
TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "[$TIMESTAMP] === odds_snapshot DONE (exit=0) ===" | tee -a "$LOG_FILE"
else
    echo "[$TIMESTAMP] === odds_snapshot FAILED (exit=$EXIT_CODE) ===" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
