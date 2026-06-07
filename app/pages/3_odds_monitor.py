"""オッズ推移モニタページ。

段階スナップショットのオッズ変動を馬番別折れ線グラフで表示する。
締切接近時は st_autorefresh で自動更新する。
"""

import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh  # type: ignore[import]

    _HAS_AUTOREFRESH = True
except ImportError:
    _HAS_AUTOREFRESH = False

from app._data_loader import load_odds_snapshots
from app._data_loader import snapshots_to_dataframe
from app._formatters import snapshots_to_chart_df
from src.constants._bet_types import BetType
from src.constants._local_paths import LocalPaths
from src.constants._odds_phases import OddsPhase

st.set_page_config(page_title="オッズ推移 — KeibaAM", page_icon="📈", layout="wide")
st.title("📈 オッズ推移モニタ")

# ------------------------------------------------------------------
# 自動リフレッシュ（60秒ごと）
# ------------------------------------------------------------------
if _HAS_AUTOREFRESH:
    st_autorefresh(interval=60_000, key="odds_monitor_refresh")

# ------------------------------------------------------------------
# データ読込（キャッシュ）
# ------------------------------------------------------------------
@st.cache_data(ttl=60, show_spinner="スナップショット読み込み中…")
def _load_snapshots():
    return load_odds_snapshots(LocalPaths.RAW_ODDS_SNAPSHOT_PATH)


snapshots = _load_snapshots()

if not snapshots:
    st.info("オッズスナップショットがまだありません。`odds_scheduler.py` で収集してください。")
    st.stop()

df_all = snapshots_to_dataframe(snapshots)

# ------------------------------------------------------------------
# フィルタ
# ------------------------------------------------------------------
col_a, col_b = st.columns(2)
with col_a:
    race_ids = sorted(df_all["race_id"].unique(), reverse=True)
    selected_race = st.selectbox("race_id", race_ids)
with col_b:
    bet_types = df_all["bet_type"].unique().tolist()
    selected_bet = st.selectbox("馬券種", bet_types, index=0)

df_race = df_all[(df_all["race_id"] == selected_race) & (df_all["bet_type"] == selected_bet)]

if df_race.empty:
    st.warning("選択条件に一致するデータがありません。")
    st.stop()

# ------------------------------------------------------------------
# フェーズ別スナップ一覧
# ------------------------------------------------------------------
st.subheader(f"{selected_race} / {selected_bet} — フェーズ別スナップ")
st.dataframe(
    df_race.sort_values("minutes_to_post", ascending=False).reset_index(drop=True),
    use_container_width=True,
    hide_index=True,
)

# ------------------------------------------------------------------
# オッズ推移グラフ（馬番別）
# ------------------------------------------------------------------
st.subheader("オッズ推移（横軸: 締切まで残り分数）")

# 単勝スナップショットのみグラフ化（連系は combo 展開が複雑なため単勝に限定）
if selected_bet == BetType.TANSHO:

    snaps_for_race = [s for s in snapshots if s.race_id == selected_race and s.bet_type == selected_bet]
    chart_df = snapshots_to_chart_df(snaps_for_race)

    if not chart_df.empty:
        pivot = chart_df.pivot_table(index="minutes_to_post", columns="umaban", values="odds", aggfunc="last")
        # 縦軸を逆に（締切に近い順）
        pivot = pivot.sort_index(ascending=False)
        st.line_chart(pivot, use_container_width=True)
    else:
        st.info("グラフデータが不足しています。")
else:
    st.info("連系馬券の推移グラフは単勝取得後に実装予定です（Phase B）。")

# ------------------------------------------------------------------
# phase ごとの変動幅サマリ
# ------------------------------------------------------------------
if selected_bet == BetType.TANSHO:
    st.subheader("フェーズ間 オッズ変動サマリ")
    pivot_phase = df_race.pivot_table(index="combo", columns="phase", values="odds", aggfunc="last")
    phases = [OddsPhase.PREV_DAY, OddsPhase.HOURS_BEFORE, OddsPhase.THIRTY_MIN, OddsPhase.JUST_BEFORE]
    existing_phases = [p for p in phases if p in pivot_phase.columns]
    if len(existing_phases) >= 2:
        pivot_phase = pivot_phase[existing_phases]
        st.dataframe(pivot_phase.round(1), use_container_width=True)
    else:
        st.info("フェーズが 2 つ以上揃うと変動幅を表示します。")
