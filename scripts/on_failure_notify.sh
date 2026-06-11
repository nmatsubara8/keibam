#!/usr/bin/env bash
# on_failure_notify.sh — cron ジョブ失敗時の通知ヘルパー
#
# 環境変数:
#   NOTIFY_SLACK_WEBHOOK  Slack Incoming Webhook URL（設定時は Slack に POST）
#   NOTIFY_EMAIL          送信先メールアドレス（設定時は mail コマンドで送信）
#
# 使用例（スクリプト内の trap から呼ぶ）:
#   source "$(dirname "$0")/on_failure_notify.sh"
#   trap 'notify_failure "$JOB_NAME" "$POST_DATE" "$LOG_FILE"' ERR
#
# または直接呼び出し:
#   on_failure_notify.sh "daily_ingest" "20240101" "/path/to/log"

notify_failure() {
    local job="${1:-unknown_job}"
    local context="${2:-}"
    local log_file="${3:-}"
    local host
    host="$(hostname -s 2>/dev/null || echo unknown)"
    local ts
    ts="$(date '+%Y-%m-%d %H:%M:%S')"

    local subject="[keibam] FAILED: ${job} @ ${host} ${ts}"
    local body="Job   : ${job}"
    [[ -n "$context" ]] && body+=$'\n'"Context: ${context}"
    body+=$'\n'"Host  : ${host}"
    body+=$'\n'"Time  : ${ts}"
    [[ -n "$log_file" ]] && body+=$'\n'"Log   : ${log_file}"
    if [[ -n "$log_file" && -f "$log_file" ]]; then
        body+=$'\n\n'"--- last 30 lines ---"$'\n'
        body+="$(tail -30 "$log_file" 2>/dev/null || true)"
    fi

    # Slack 通知
    if [[ -n "${NOTIFY_SLACK_WEBHOOK:-}" ]]; then
        local payload
        payload="$(printf '{"text":"%s\\n%s"}' \
            ":x: *${subject}*" \
            "$(echo "$body" | sed 's/"/\\"/g' | tr '\n' '|' | sed 's/|/\\n/g')")"
        curl -s -X POST \
            -H 'Content-type: application/json' \
            --data "$payload" \
            "$NOTIFY_SLACK_WEBHOOK" > /dev/null 2>&1 || true
    fi

    # メール通知
    if [[ -n "${NOTIFY_EMAIL:-}" ]] && command -v mail > /dev/null 2>&1; then
        echo "$body" | mail -s "$subject" "$NOTIFY_EMAIL" 2>/dev/null || true
    fi

    # 常に stderr にも出力
    echo "[$ts] FAILURE NOTIFICATION: ${subject}" >&2
    echo "$body" >&2
}

# 直接実行された場合（source ではなく ./on_failure_notify.sh として呼ばれた場合）
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    notify_failure "${1:-}" "${2:-}" "${3:-}"
fi
