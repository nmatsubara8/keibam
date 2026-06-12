"""ダッシュボードページ。

本日開催レース一覧・データ収集状況・次締切カウントダウン・システム稼働状態を表示する。
締切接近時は st_autorefresh で自動再描画する。
"""

import datetime as dt
import os
import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore[import]

    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

from app._data_loader import load_db_stats
from app._data_loader import load_system_status
from src.constants._local_paths import LocalPaths

st.set_page_config(page_title="ダッシュボード — KeibaAM", page_icon="📊", layout="wide")
st.title("📊 ダッシュボード")

# ------------------------------------------------------------------
# 自動リフレッシュ（30秒ごと）
# ------------------------------------------------------------------
if _HAS_AUTOREFRESH:
    st_autorefresh(interval=30_000, key="dashboard_refresh")

# ------------------------------------------------------------------
# システム状態メトリクス
# ------------------------------------------------------------------
status = load_system_status()

c1, c2, c3, c4 = st.columns(4)
c1.metric("モデルバージョン", status["model_version"] or "—")
c2.metric("AUC (test)", f"{status['model_auc']:.4f}" if status["model_auc"] else "—")
c3.metric("スナップショット", f"{status['n_snapshots']:,} 件")
c4.metric("最終取込", status["last_ingest"] or "未実行")

# Phase 2: 特徴量データの統計
if status.get("n_featured_rows"):
    c5, c6, c7, c8 = st.columns(4)
    c5.metric("特徴量 行数", f"{status['n_featured_rows']:,}")
    c6.metric("特徴量 列数", f"{status['n_featured_cols']:,}")
    c7.metric("最古 race_id", status.get("featured_min_race_id") or "—")
    c8.metric("最新 race_id", status.get("featured_max_race_id") or "—")

mode_color = {"advisory": "🟢", "semi_auto": "🟡", "full_auto": "🔴"}.get(status["operation_mode"], "⚪")
st.info(f"{mode_color} 運用モード: **{status['operation_mode']}**")

# ------------------------------------------------------------------
# 安全装置（損失ストップ）と実効 bankroll
# ------------------------------------------------------------------
from app._betting_history import load_history
from app._data_loader import load_operation_config
from src.operation._risk_guard import effective_bankroll
from src.operation._risk_guard import evaluate_kill_switch

_op = load_operation_config()
_history = load_history()
_guard = evaluate_kill_switch(_history, _op)
_eff_bankroll = effective_bankroll(_history, _op.initial_bankroll)

s1, s2, s3 = st.columns(3)
if _guard.blocked:
    s1.error("🛑 取引停止")
else:
    s1.success("🟢 取引可能")
s2.metric("当日実現損失", f"¥{_guard.daily_loss:,.0f}", help=f"上限 ¥{_guard.limit:,.0f}")
s3.metric(
    "実効 bankroll",
    f"¥{_eff_bankroll:,.0f}",
    delta=f"{_eff_bankroll - _op.initial_bankroll:+,.0f}",
    help="初期資金 + 確定済みの累積純損益",
)

st.divider()

# ------------------------------------------------------------------
# Raw データ存在チェック
# ------------------------------------------------------------------
st.subheader("📂 データ収集状況")
lp = LocalPaths()
raw_files = {
    "レース結果": lp.RAW_RESULTS_PATH,
    "レース情報": lp.RAW_RACE_INFO_PATH,
    "払戻テーブル": lp.RAW_RETURN_TABLES_PATH,
    "馬過去成績": lp.RAW_HORSE_RESULTS_PATH,
    "馬基本情報": lp.RAW_HORSE_INFO_PATH,
    "血統": lp.RAW_PEDS_PATH,
    "特徴量データ": lp.FEATURED_DATA_PATH,
    "オッズスナップ": lp.RAW_ODDS_SNAPSHOT_PATH,
}
status_rows = []
for label, path in raw_files.items():
    exists = os.path.exists(path)
    mtime = (
        dt.datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M")
        if exists
        else "—"
    )
    size_mb = f"{os.path.getsize(path) / 1e6:.1f} MB" if exists else "—"
    status_rows.append({"データ": label, "状態": "✅" if exists else "❌", "最終更新": mtime, "サイズ": size_mb})

st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

st.divider()

# ------------------------------------------------------------------
# DB 統計（Phase 2）
# ------------------------------------------------------------------
st.subheader("🗄️ データベース統計")
db_stats = load_db_stats()
if db_stats["table_counts"]:
    db_rows = [
        {"テーブル": alias, "行数": cnt if cnt >= 0 else "エラー"}
        for alias, cnt in db_stats["table_counts"].items()
    ]
    st.dataframe(pd.DataFrame(db_rows), use_container_width=True, hide_index=True)
    st.caption(f"DB ファイルサイズ: {db_stats['db_size_mb']:.1f} MB  ({LocalPaths.DB_PATH})")
else:
    st.info("DB が未初期化です。取込実行後に統計が表示されます。")

st.divider()

# ------------------------------------------------------------------
# 次締切カウントダウン（スケジュールデータがある場合）
# ------------------------------------------------------------------
st.subheader("⏱️ 次締切")
schedule_path = os.path.join(LocalPaths.HTML_DIR, "schedule", "schedule.pkl")
if os.path.exists(schedule_path):
    try:
        schedule_df = pd.read_pickle(schedule_path)
        # pickleがリスト/dict の場合はDataFrameに変換する
        if not isinstance(schedule_df, pd.DataFrame):
            schedule_df = pd.DataFrame(schedule_df)
        now = dt.datetime.now()
        st.dataframe(schedule_df.head(10), use_container_width=True)
    except Exception as e:
        st.warning(f"スケジュール読み込みエラー: {e}")
else:
    st.info("スケジュールデータが見つかりません。`process_selector.py` でスクレイピングしてください。")
