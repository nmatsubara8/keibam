"""オッズ推移モニタページ。

段階スナップショットのオッズ変動を馬番別折れ線グラフで表示する。
締切接近時は st_autorefresh で自動更新する。
"""

import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

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
    from src.constants._odds_phases import PHASE_TIMELINE
    phases = list(PHASE_TIMELINE)
    existing_phases = [p for p in phases if p in pivot_phase.columns]
    if len(existing_phases) >= 2:
        pivot_phase = pivot_phase[existing_phases]
        st.dataframe(pivot_phase.round(1), use_container_width=True)
    else:
        st.info("フェーズが 2 つ以上揃うと変動幅を表示します。")


# ------------------------------------------------------------------
# オッズ力学モデルの照会（各時点の実績 vs 次時点・確定の予測）
# ------------------------------------------------------------------
st.subheader("🔮 オッズ力学モデル予測の照会")

from app._odds_dynamics_compare import available_models
from app._odds_dynamics_compare import available_races
from app._odds_dynamics_compare import inquiry_matrix
from src.pipeline.odds_watch import load_predictions


@st.cache_data(ttl=60, show_spinner=False)
def _load_predictions():
    return load_predictions()


predictions = _load_predictions()
pred_races = available_races(predictions)
if not pred_races:
    st.info(
        "予測データがまだありません。開催日に `python -m src.pipeline.odds_watch --once` "
        "がタイマー実行されると、チェックポイント（発走 30/10/5/1 分前）ごとの実績と"
        "次時点・確定オッズの予測がここに表示されます。"
    )
else:
    col_a, col_b = st.columns(2)
    with col_a:
        pred_race = st.selectbox("照会するレース", pred_races, key="pred_race")
    with col_b:
        pred_model = st.selectbox("モデル", available_models(predictions), key="pred_model")
    matrix = inquiry_matrix(predictions, pred_race, pred_model)
    if matrix.empty:
        st.info("このレース・モデルの予測がありません。")
    else:
        st.dataframe(matrix.round(3), use_container_width=True)
        st.caption(
            "各時点（チェックポイント）の実績オッズと、その時点で計算した「次時点の予測シェア」"
            "「発走時の予測確定オッズ」。最新の予測確定オッズは期待値計算"
            "（config.yaml: use_predicted_odds: true）に自動で使われます。"
        )
