#!/usr/bin/env bash
# backfill_peds_resilient.sh — 血統(backfill-peds)を「落ちない・ハングしても復帰する」ように回す。
#
# horse 版(backfill_horses_resilient.sh)と同型だが、peds には固有の失敗モードがある:
#   ・プロセスは生きたまま Playwright ブラウザが死に、await が返らず「ハング」する
#     （ファイル数が増えないのにプロセスは Sl のまま。実際に発生した）。
# よって「ped ファイル数の増加」を監視するウォッチドッグで停滞を検知し、kill→再開する。
#
# 使い方:
#   ./scripts/backfill_peds_resilient.sh
#   KEIBA_MAX_REQUESTS_PER_HOUR=500 ./scripts/backfill_peds_resilient.sh   # 速度を上げる
#
# 主要つまみ（環境変数で上書き。既定は「無人で数週間流す」前提の安全側）:
#   KEIBA_SCRAPE_DELAY           リクエスト間隔秒（既定 5.0）
#   KEIBA_MAX_REQUESTS_PER_HOUR  1時間あたり上限（既定 250）
#   WATCH_INTERVAL               進捗チェック間隔秒（既定 120）
#   STALL_SECONDS                ped 数が増えないままこの秒数でハング判定（既定 1800=30分）。
#                                ただし直近ログがレート制限の長時間待機ならハング判定を保留する。
#   COOLDOWN_BASE_SEC            kill/停滞後の再起動前クールダウン秒（既定 120）
#   COOLDOWN_MAX_SEC             クールダウン上限秒（既定 1800）
#   MAX_ITERS                    ラッパー最大反復（既定 500。無人長時間の保険）
#   STALL_LIMIT                  「1反復まるごと新規0」が連続で許容される回数（既定 5）。超えたら中止。
#
# 設計: 1回の backfill-peds は残り全件を1プロセスで取り切る（チャンクしない）。正常なら
# 「全件取得済み」ログで終わる。ハングしたらウォッチドッグが kill し、次反復で取得済みを
# スキップして続きから再開する（冪等）。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
PED_DIR="$PROJECT_DIR/data/html/ped"
mkdir -p "$LOG_DIR"

# backfill-peds へ素通しする追加引数（`-- --source netkeiba` のように渡す）
EXTRA_ARGS=()
if [[ $# -gt 0 ]]; then
    if [[ "$1" == "--" ]]; then shift; fi
    EXTRA_ARGS=("$@")
fi

export KEIBA_SCRAPE_DELAY="${KEIBA_SCRAPE_DELAY:-5.0}"
export KEIBA_MAX_REQUESTS_PER_HOUR="${KEIBA_MAX_REQUESTS_PER_HOUR:-250}"

WATCH_INTERVAL="${WATCH_INTERVAL:-120}"
STALL_SECONDS="${STALL_SECONDS:-1800}"
COOLDOWN_BASE_SEC="${COOLDOWN_BASE_SEC:-120}"
COOLDOWN_MAX_SEC="${COOLDOWN_MAX_SEC:-1800}"
MAX_ITERS="${MAX_ITERS:-500}"
STALL_LIMIT="${STALL_LIMIT:-5}"

TS() { date '+%Y-%m-%d %H:%M:%S'; }
RUN_LOG="$LOG_DIR/backfill_peds_$(date '+%Y%m%d_%H%M%S').log"
say() { echo "[$(TS)] $*" | tee -a "$RUN_LOG"; }

ped_count() { ls "$PED_DIR" 2>/dev/null | wc -l | tr -d ' '; }

# 直近ログがレート制限の待機中か（大量取得時の窓境界で数十分ファイルが増えないのは正常）。
in_ratelimit_wait() { tail -n 3 "$RUN_LOG" 2>/dev/null | grep -q "秒待機します"; }

# 全件取得済みで自然終了したか。
looks_done() { tail -n 15 "$RUN_LOG" 2>/dev/null | grep -q "全件取得済み"; }

# --- 多重起動防止（ラッパー同士）---
WRAPPER_LOCK="${KEIBA_PEDS_WRAPPER_LOCK:-/tmp/keibam_peds_wrapper.lock}"
if command -v flock >/dev/null 2>&1; then
    exec 9>"$WRAPPER_LOCK"
    if ! flock -n 9; then
        echo "[$(TS)] 別の peds ラッパーが稼働中です（lock: $WRAPPER_LOCK）。終了します。" | tee -a "$RUN_LOG"
        exit 1
    fi
fi

cd "$PROJECT_DIR"
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

JOB_PID=""
kill_job() {
    [[ -n "$JOB_PID" ]] || return
    kill "$JOB_PID" 2>/dev/null
    sleep 5
    kill -9 "$JOB_PID" 2>/dev/null
    pkill -f "chromium" 2>/dev/null   # ハング時の残留ブラウザを掃除
    JOB_PID=""
}
trap 'say "中断シグナル受信。ジョブを停止して終了します。"; kill_job; exit 130' INT TERM

# 起動時に迷子の peds プロセスがいれば掃除（ロック衝突・二重取得の予防）。
if pgrep -f "run_pipeline backfill-peds" >/dev/null 2>&1; then
    say "既存の backfill-peds プロセスを検出。掃除してから開始します。"
    pkill -f "run_pipeline backfill-peds" 2>/dev/null
    pkill -f "chromium" 2>/dev/null
    sleep 5
fi

say "=== backfill-peds resilient START ==="
say "politeness: DELAY=$KEIBA_SCRAPE_DELAY MAX/h=$KEIBA_MAX_REQUESTS_PER_HOUR"
say "watchdog: WATCH=${WATCH_INTERVAL}s STALL=${STALL_SECONDS}s COOLDOWN=${COOLDOWN_BASE_SEC}..${COOLDOWN_MAX_SEC}s"
say "wrapper: MAX_ITERS=$MAX_ITERS STALL_LIMIT=$STALL_LIMIT  PED_DIR=$PED_DIR"
[[ ${#EXTRA_ARGS[@]} -gt 0 ]] && say "extra args: ${EXTRA_ARGS[*]}"

noprogress_iters=0
cooldown="$COOLDOWN_BASE_SEC"

for ((iter = 1; iter <= MAX_ITERS; iter++)); do
    start_count="$(ped_count)"
    say "--- iter $iter/$MAX_ITERS  ped=${start_count} 起動 ---"

    # backfill-peds をバックグラウンド起動（同じ RUN_LOG に追記）
    python -m src.pipeline.run_pipeline backfill-peds "${EXTRA_ARGS[@]}" >>"$RUN_LOG" 2>&1 &
    JOB_PID=$!

    # ウォッチドッグ: ped 数の増加を監視。停滞=ハングとみなし kill。
    last_count="$start_count"
    stalled=0
    hung=0
    while kill -0 "$JOB_PID" 2>/dev/null; do
        sleep "$WATCH_INTERVAL" &
        wait $!    # trap(INT) が sleep を割り込めるように
        cur="$(ped_count)"
        if [[ "$cur" -gt "$last_count" ]]; then
            last_count="$cur"
            stalled=0
        else
            stalled=$((stalled + WATCH_INTERVAL))
            if [[ "$stalled" -ge "$STALL_SECONDS" ]]; then
                if in_ratelimit_wait; then
                    say "ped 無増加 ${stalled}s だがレート制限待機中。ハング判定を保留。"
                    stalled=0
                else
                    say "ハング検知: ped=${cur} が ${stalled}s 増えず。ジョブを kill して再開します。"
                    hung=1
                    kill_job
                    break
                fi
            fi
        fi
    done

    wait "$JOB_PID" 2>/dev/null || true
    JOB_PID=""
    end_count="$(ped_count)"
    say "iter $iter 終了: ped ${start_count} → ${end_count}$([[ $hung -eq 1 ]] && echo '（ハング kill）')"

    # 自然終了かつ「全件取得済み」ログ → 完了
    if [[ "$hung" -eq 0 ]] && looks_done; then
        say "全件取得済み。正常終了。"
        exit 0
    fi

    # 進捗判定（1反復で新規が増えたか）
    if [[ "$end_count" -gt "$start_count" ]]; then
        noprogress_iters=0
        cooldown="$COOLDOWN_BASE_SEC"
    else
        noprogress_iters=$((noprogress_iters + 1))
        say "この反復で新規取得なし（${noprogress_iters}/${STALL_LIMIT}）。"
        if [[ "$noprogress_iters" -ge "$STALL_LIMIT" ]]; then
            say "進捗停止が続くため中止。時間を置いて再実行してください（取得済みはスキップ）。"
            exit 2
        fi
    fi

    say "${cooldown}s クールダウンして次反復。"
    sleep "$cooldown" &
    wait $!
    if [[ "$hung" -eq 1 || "$end_count" -le "$start_count" ]]; then
        cooldown=$((cooldown * 2))
        [[ "$cooldown" -gt "$COOLDOWN_MAX_SEC" ]] && cooldown="$COOLDOWN_MAX_SEC"
    fi
done

say "MAX_ITERS=$MAX_ITERS 到達。未完なら再実行してください（取得済みはスキップ）。"
exit 3
