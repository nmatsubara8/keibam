#!/usr/bin/env bash
# nn_search_loop.sh — NN 単体構造探索(--nn-standalone --with-tuning)を N 回連続実行する。
#
# 各回は models/nn_standalone_leaderboard.json の上位を Optuna 初期値（ウォームスタート）に使い、
# --resume-tuning で永続 study を積み増す。回を重ねるほど探索が深まり、leaderboard(上位 top_k)が
# 育つ。1 回が落ちても続行し、各回のログを logs/ に分けて残し、最後に leaderboard を表示する。
#
# 使い方（リポジトリ直下で実行）:
#   scripts/nn_search_loop.sh                 # 既定 10 回
#   N=5 scripts/nn_search_loop.sh             # 5 回
#   VERSION=nn_full NN_CONFIG=configs/base_models_nn_tuned.json scripts/nn_search_loop.sh
# 推奨: tmux 内で実行（切断に強い）。 例:  tmux new -s nn 'scripts/nn_search_loop.sh'
#
# 環境変数:
#   N          連続実行回数（既定 10）
#   VERSION    --version-name（既定 nn_full）
#   NN_CONFIG  --nn-config（既定 configs/base_models_nn_tuned.json）
#   HOLDOUT    --holdout-years（既定 2024）
#   LOGDIR     ログ出力先（既定 logs）
#   EXTRA_ARGS retrain へ追加で渡す引数（例: "--since-year 2010"）

set -uo pipefail   # -e は付けない（1 回の失敗で全体を止めないため）

N="${N:-10}"
VERSION="${VERSION:-nn_full}"
NN_CONFIG="${NN_CONFIG:-configs/base_models_nn_tuned.json}"
HOLDOUT="${HOLDOUT:-2024}"
LOGDIR="${LOGDIR:-logs}"
EXTRA_ARGS="${EXTRA_ARGS:-}"
LEADERBOARD="models/nn_standalone_leaderboard.json"

mkdir -p "$LOGDIR"
echo "[nn_search_loop] N=$N version=$VERSION config=$NN_CONFIG holdout=$HOLDOUT logdir=$LOGDIR"

fails=0
for i in $(seq 1 "$N"); do
    ts="$(date '+%Y-%m-%d %H:%M:%S')"
    log="$LOGDIR/nn_search_$(printf '%02d' "$i").log"
    echo "==== [$i/$N] $ts 開始 → $log ===="
    # shellcheck disable=SC2086
    python -m src.pipeline.run_pipeline retrain --version-name "$VERSION" \
        --nn-standalone --with-tuning \
        --nn-config "$NN_CONFIG" \
        --holdout-years "$HOLDOUT" --float32-features --resume-tuning \
        $EXTRA_ARGS 2>&1 | tee "$log"
    rc="${PIPESTATUS[0]}"   # tee ではなく python の終了コード
    if [[ "$rc" -ne 0 ]]; then
        echo "==== [$i/$N] 異常終了 exit=$rc（続行）===="
        fails=$((fails + 1))
    else
        echo "==== [$i/$N] 正常終了 ===="
    fi
done

echo "[nn_search_loop] 完了。失敗 ${fails}/${N} 回。"
if [[ -f "$LEADERBOARD" ]]; then
    echo "=== leaderboard ($LEADERBOARD) ==="
    cat "$LEADERBOARD"
else
    echo "[nn_search_loop] 警告: $LEADERBOARD が見つかりません（全回失敗した可能性）。logs/ を確認してください。"
fi
