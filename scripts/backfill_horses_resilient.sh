#!/usr/bin/env bash
# backfill_horses_resilient.sh — 馬ページ網羅取得(backfill-horses)を「落ちない」ように回す。
#
# netkeiba のソフトブロック（短時間の累積リクエスト過多で IP に一時 BAN。空ページ len=39 が返る）
# は数分のクールダウンで解除される。本ラッパーは:
#   1. ブロック連鎖で run が中断(KEIBA_SCRAPE_ABORT_AFTER 到達)しても、
#   2. 数分クールダウンして自動再開する（backfill-horses は取得済みをスキップし冪等・再開可能）。
#   3. 残頭数が減らない（=ブロックが解けていない）場合はクールダウンを指数的に延ばす。
#   4. 全頭取得済みになったら正常終了。
#
# 使い方:
#   ./scripts/backfill_horses_resilient.sh
#   ./scripts/backfill_horses_resilient.sh -- --source netkeiba   # backfill-horses への追加引数は -- の後ろ
#
# 主要な調整つまみ（環境変数で上書き可能。既定は「一晩放置で取り切る」想定の安全側）:
#   KEIBA_SCRAPE_DELAY            リクエスト間隔秒（既定 6.0）
#   KEIBA_MAX_REQUESTS_PER_HOUR  時間あたり上限（既定 300）
#   KEIBA_SKIP_PEDS              血統を分離（既定 1。peds は後で backfill-peds）
#   KEIBA_SCRAPE_BACKOFF        run 内ブロック時の基準バックオフ秒（既定 30 → 30,60,120,...）
#   KEIBA_SCRAPE_MAX_RETRY      run 内の同一馬リトライ回数（既定 5）
#   KEIBA_SCRAPE_ABORT_AFTER    連続ブロックで run 中断する閾値（既定 12。従来 5 より高め）
#   COOLDOWN_BASE_SEC           run 中断後のクールダウン基準秒（既定 300=5分）
#   COOLDOWN_MAX_SEC            クールダウン上限秒（既定 1800=30分）
#   MAX_ITERS                   ラッパーの最大反復回数（既定 200。無限ループ保険）
#   STALL_LIMIT                 残頭数が減らない連続回数の許容上限（既定 6）。超えたら中止。

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_DIR/logs"
mkdir -p "$LOG_DIR"

# backfill-horses へ素通しする追加引数（`-- --source netkeiba` のように渡す）
EXTRA_ARGS=()
if [[ $# -gt 0 ]]; then
    if [[ "$1" == "--" ]]; then shift; fi
    EXTRA_ARGS=("$@")
fi

# --- politeness / 耐ブロック 既定値（呼び出し側 env が優先） ---
export KEIBA_SCRAPE_DELAY="${KEIBA_SCRAPE_DELAY:-6.0}"
export KEIBA_MAX_REQUESTS_PER_HOUR="${KEIBA_MAX_REQUESTS_PER_HOUR:-300}"
export KEIBA_SKIP_PEDS="${KEIBA_SKIP_PEDS:-1}"
export KEIBA_SCRAPE_BACKOFF="${KEIBA_SCRAPE_BACKOFF:-30}"
export KEIBA_SCRAPE_MAX_RETRY="${KEIBA_SCRAPE_MAX_RETRY:-5}"
export KEIBA_SCRAPE_ABORT_AFTER="${KEIBA_SCRAPE_ABORT_AFTER:-12}"

COOLDOWN_BASE_SEC="${COOLDOWN_BASE_SEC:-300}"
COOLDOWN_MAX_SEC="${COOLDOWN_MAX_SEC:-1800}"
MAX_ITERS="${MAX_ITERS:-200}"
STALL_LIMIT="${STALL_LIMIT:-6}"

TS() { date '+%Y-%m-%d %H:%M:%S'; }
RUN_LOG="$LOG_DIR/backfill_horses_$(date '+%Y%m%d_%H%M%S').log"
say() { echo "[$(TS)] $*" | tee -a "$RUN_LOG"; }

# Ctrl-C で即座に止める（クールダウン sleep 中でも抜ける）
INTERRUPTED=0
trap 'INTERRUPTED=1; say "中断シグナル受信。停止します。"; exit 130' INT TERM

cd "$PROJECT_DIR"
if [[ -f ".venv/bin/activate" ]]; then
    # shellcheck source=/dev/null
    source ".venv/bin/activate"
fi

# 残り取得対象の頭数を返す（raw_results の horse_id − horse_results.index）。
# 失敗時は -1 を返す（判定不能。停止条件には使わない）。
remaining_count() {
    python - <<'PY' 2>/dev/null || echo -1
from src.constants._local_paths import LocalPaths
from src.pipeline._ingestion import load_raw
res = load_raw(LocalPaths.RAW_RESULTS_PATH)
if res.empty or "horse_id" not in res.columns:
    print(0); raise SystemExit
ids = set(res["horse_id"].astype(str))
hr = load_raw(LocalPaths.RAW_HORSE_RESULTS_PATH)
done = {str(h) for h in hr.index} if not hr.empty else set()
print(len(ids - done))
PY
}

say "=== backfill-horses resilient START ==="
say "politeness: DELAY=$KEIBA_SCRAPE_DELAY MAX/h=$KEIBA_MAX_REQUESTS_PER_HOUR SKIP_PEDS=$KEIBA_SKIP_PEDS"
say "anti-block: BACKOFF=$KEIBA_SCRAPE_BACKOFF MAX_RETRY=$KEIBA_SCRAPE_MAX_RETRY ABORT_AFTER=$KEIBA_SCRAPE_ABORT_AFTER"
say "wrapper: COOLDOWN ${COOLDOWN_BASE_SEC}..${COOLDOWN_MAX_SEC}s MAX_ITERS=$MAX_ITERS STALL_LIMIT=$STALL_LIMIT"
[[ ${#EXTRA_ARGS[@]} -gt 0 ]] && say "extra args: ${EXTRA_ARGS[*]}"

prev_remaining=-1
stall_count=0
cooldown="$COOLDOWN_BASE_SEC"

for ((iter = 1; iter <= MAX_ITERS; iter++)); do
    rem="$(remaining_count)"
    if [[ "$rem" == "0" ]]; then
        say "残り 0 頭。全件取得済み。正常終了。"
        exit 0
    fi
    say "--- iter $iter/$MAX_ITERS  残り ${rem} 頭 ---"

    # 進捗(残頭数の減少)を判定し、ストール検知でクールダウンを延ばす
    if [[ "$rem" != "-1" && "$prev_remaining" != "-1" ]]; then
        if [[ "$rem" -lt "$prev_remaining" ]]; then
            stall_count=0
            cooldown="$COOLDOWN_BASE_SEC"   # 進捗あり → クールダウンをリセット
        else
            stall_count=$((stall_count + 1))
            say "進捗なし（前回 ${prev_remaining} → 今回 ${rem}）。ストール ${stall_count}/${STALL_LIMIT}"
            if [[ "$stall_count" -ge "$STALL_LIMIT" ]]; then
                say "ストール上限到達。ブロックが解けていない可能性が高いので中止します。"
                say "時間を置いて（数十分〜数時間）再実行してください。再開は取得済みをスキップします。"
                exit 2
            fi
        fi
    fi
    prev_remaining="$rem"

    set +e
    python -m src.pipeline.run_pipeline backfill-horses "${EXTRA_ARGS[@]}" 2>&1 | tee -a "$RUN_LOG"
    exit_code=${PIPESTATUS[0]}
    set -e 2>/dev/null || true

    if [[ "$INTERRUPTED" -eq 1 ]]; then exit 130; fi

    if [[ "$exit_code" -eq 0 ]]; then
        # exit 0 でも「全件取得済み」でない限り、まだ残っている可能性（バッチ途中正常終了等）。
        # 次ループ冒頭の remaining_count で 0 を確認して終了する。
        say "iter $iter 正常終了(exit=0)。残数を再確認します。"
        # 進捗していれば短い間隔で次へ、していなければ通常クールダウン
        next_rem="$(remaining_count)"
        if [[ "$next_rem" == "0" ]]; then
            say "残り 0 頭。全件取得済み。正常終了。"
            exit 0
        fi
        say "まだ ${next_rem} 頭。${COOLDOWN_BASE_SEC}s 待って継続。"
        sleep "$COOLDOWN_BASE_SEC" &
        wait $!
        continue
    fi

    # 非ゼロ終了（ブロック連鎖で ABORT_AFTER 到達など）→ クールダウンして再開
    say "iter $iter 異常終了(exit=$exit_code)。${cooldown}s クールダウンして再開します。"
    sleep "$cooldown" &
    wait $!   # wait なら trap(INT) が sleep を即座に割り込める
    # 次回のクールダウンを指数的に延ばす（上限まで）
    cooldown=$((cooldown * 2))
    [[ "$cooldown" -gt "$COOLDOWN_MAX_SEC" ]] && cooldown="$COOLDOWN_MAX_SEC"
done

say "MAX_ITERS=$MAX_ITERS に到達。未完なら再実行してください（取得済みはスキップ）。"
exit 3
