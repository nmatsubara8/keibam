#!/usr/bin/env bash
# odds_watch_loop.sh — odds_watch を常駐ループで回し続ける（tmux 常駐 / 自動再起動）。
#
# 目的: 2026-07 の「手動 --once の単発起動 → 各レースを単一時刻でしか取得できず
# evaluate-odds-dynamics が全 NaN」の再発防止。同一 race_id を発走30分前〜締切まで
# 繰り返し取得し、captured_at ごとに時系列を DB へ累積させる（軌跡の生成を保証する）。
#
# WSL2/デスクトップは cron が不安定なため、cron に依存せず tmux 常駐で毎ティック取得する。
#
# 使い方（開催日の朝に一度起動しておくだけ）:
#   tmux new -d -s odds 'scripts/odds_watch_loop.sh'
#   tmux attach -t odds        # 様子を見る
#   python -m src.pipeline.odds_watch --status   # 別窓で取得状況を点検（単一ティック警告つき）
#
# 起動日時・時刻を制御する（アプリ内スケジュール。cron 不要）:
#   # 早めに tmux 常駐させ、9:30 に取得開始・16:30 に自動終了する
#   ODDS_WATCH_ARGS='--start-at 09:30 --stop-at 16:30' tmux new -d -s odds 'scripts/odds_watch_loop.sh'
#   # 特定日を指定（前夜から起動しておく）
#   ODDS_WATCH_ARGS='--start-at 2026-07-20T09:30 --stop-at 2026-07-20T16:30' scripts/odds_watch_loop.sh
#   # 連休など複数開催日を事前予約（JSON に各日 start/stop を書いて一度起動するだけ）
#   ODDS_WATCH_ARGS='--schedule configs/odds_watch_schedule.example.json' \
#     tmux new -d -s odds 'scripts/odds_watch_loop.sh'
#
# 環境変数:
#   ODDS_WATCH_INTERVAL  取得間隔秒（既定 120）。発走30分前から2分おきに取得する。
#   ODDS_WATCH_ARGS      odds_watch へ渡す追加フラグ（--start-at / --stop-at / --date 等）。

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

INTERVAL="${ODDS_WATCH_INTERVAL:-120}"
EXTRA_ARGS="${ODDS_WATCH_ARGS:-}"
LOG_FILE="$LOG_DIR/odds_watch_loop_$(date '+%Y%m%d').log"

cd "$PROJECT_DIR"

# 仮想環境があれば有効化
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

echo "[odds_watch_loop] $(date '+%F %T') START interval=${INTERVAL}s（Ctrl-C か tmux kill-session で停止）" | tee -a "$LOG_FILE"

# odds_watch --loop はそれ自体が内部で interval 待ちのループを回す。異常終了(code≠0)は
# ここで捕捉して数秒後に再起動し、開催中は途切れず取得を継続する（累積保証の運用面）。
# --stop-at 到達などの正常終了(code=0)では再起動せず終える（busy-restart を避ける）。
while true; do
    # shellcheck disable=SC2086 — EXTRA_ARGS は意図的に単語分割してフラグ列にする
    python -m src.pipeline.odds_watch --loop --interval "${INTERVAL}" ${EXTRA_ARGS} 2>&1 | tee -a "$LOG_FILE"
    CODE=${PIPESTATUS[0]}
    if [[ $CODE -eq 0 ]]; then
        echo "[odds_watch_loop] $(date '+%F %T') 正常終了（--stop-at 到達等）→ ループ終了" | tee -a "$LOG_FILE"
        break
    fi
    echo "[odds_watch_loop] $(date '+%F %T') 異常終了 code=${CODE} → 5秒後に再起動" | tee -a "$LOG_FILE"
    sleep 5
done
