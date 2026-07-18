#!/usr/bin/env bash
# odds_watch_all.sh — 全馬券種を 3 分間隔で取得する（将来の検証用の生オッズ蓄積）。
#
# 目的: 現行モデル（単勝ベースの勝率動力学）用ではなく、**将来の検証**（連系 ΔR²・
# 券種横断のオッズ力学など）に備えて、全 8 券種の事前オッズ時系列を貯める。
#
# 取得券種:
#   単勝(tansho) / 複勝(fukusho)  … b1 ページで追加リクエスト無し
#   枠連(wakuren) / 馬連(umaren) / 馬単(umatan) / ワイド(wide) /
#   三連複(sanrenpuku) / 三連単(sanrentan)  … 各券種が別ページ＝1レース1tickあたり +6 fetch
#
# 注意（重要）:
#   - 行数が大きい。三連単は 18 頭で 4,896 組/レース/tick、三連複は 816 組。
#     全券種・全レース・全 tick で 1 日あたり数百万行・数百 MB 増を見込む。DB 容量に注意。
#   - 連系は券種ごとに別ページ取得のため、1 tick の所要時間が延びる。実効間隔は
#     「取得所要 + INTERVAL」になり、3 分ちょうどにはならない場合がある（アーカイブ用途なので許容）。
#   - raw_odds 系は TEXT 保存。集計時は CAST(... AS INTEGER/REAL) を付けること。
#
# 使い方（リポジトリ直下・tmux 推奨）:
#   scripts/odds_watch_all.sh                          # 当日 09:30-16:30 / 3 分間隔
#   START=09:00 STOP=17:00 scripts/odds_watch_all.sh   # 時間帯変更
#   INTERVAL=120 scripts/odds_watch_all.sh             # 間隔（秒）変更
#   DATE=20260720 scripts/odds_watch_all.sh            # 特定日
#   例: tmux new -s odds 'scripts/odds_watch_all.sh'

set -uo pipefail

START="${START:-09:30}"
STOP="${STOP:-16:30}"
INTERVAL="${INTERVAL:-180}"        # 3 分
SOURCE="${SOURCE:-netkeiba}"

# 単勝＋複勝は既定 ON（b1 同居）。連系 6 券種を明示的に有効化する。
export KEIBA_ODDS_CAPTURE_PLACE="${KEIBA_ODDS_CAPTURE_PLACE:-1}"
export KEIBA_ODDS_CAPTURE_EXOTIC="${KEIBA_ODDS_CAPTURE_EXOTIC:-wakuren,umaren,umatan,wide,sanrenpuku,sanrentan}"

echo "[odds_watch_all] 全券種取得 interval=${INTERVAL}s start=${START} stop=${STOP} date=${DATE:-today}"
echo "[odds_watch_all] PLACE=${KEIBA_ODDS_CAPTURE_PLACE} EXOTIC=${KEIBA_ODDS_CAPTURE_EXOTIC}"

exec python -m src.pipeline.odds_watch --loop --source "${SOURCE}" \
    --start-at "${START}" --stop-at "${STOP}" --interval "${INTERVAL}" \
    ${DATE:+--date "${DATE}"}
