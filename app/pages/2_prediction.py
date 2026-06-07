"""予測・EV・推奨馬券ページ。

較正勝率 × 予測オッズ → EV → フラクショナル・ケリー推奨額 を表示し、
advisory モードでは「承認して記録」ボタンで投票履歴に保存する。
"""


import pandas as pd
import streamlit as st

from app._betting_history import DEFAULT_HISTORY_PATH
from app._betting_history import append_history
from app._data_loader import load_latest_model
from app._data_loader import load_operation_config
from app._formatters import candidates_to_display_df
from app._prediction_service import run_prediction
from src.constants._local_paths import LocalPaths
from src.operation._bet_executor import create_bet_executor
from src.operation._config import ADVISORY

st.set_page_config(page_title="予測・推奨 — KeibaAM", page_icon="🎯", layout="wide")
st.title("🎯 予測・EV・推奨馬券")

# ------------------------------------------------------------------
# 設定・モデル読込（キャッシュ）
# ------------------------------------------------------------------
@st.cache_resource(show_spinner="モデルを読み込み中…")
def _load_model():
    return load_latest_model()


@st.cache_data(show_spinner=False)
def _load_config():
    return load_operation_config()


op_config = _load_config()
model = _load_model()

if model is None:
    st.error("モデルが見つかりません。先に `run_pipeline.py --job retrain` を実行してください。")
    st.stop()

# ------------------------------------------------------------------
# レース選択
# ------------------------------------------------------------------
st.subheader("レース選択")

featured_path = LocalPaths.FEATURED_DATA_PATH
try:
    featured_df = pd.read_pickle(featured_path)
    available_races = sorted(featured_df.index.unique().tolist(), reverse=True)
except FileNotFoundError:
    st.error("特徴量データが見つかりません。先に ingestion を実行してください。")
    st.stop()

race_id = st.selectbox("race_id", available_races, index=0)

if not race_id:
    st.stop()

X = featured_df.loc[[race_id]]

# ------------------------------------------------------------------
# 予測実行
# ------------------------------------------------------------------
with st.spinner("予測中…"):
    try:
        candidates = run_prediction(model.effective_model, X, op_config)
    except Exception as e:
        st.error(f"予測エラー: {e}")
        st.stop()

# ------------------------------------------------------------------
# 結果表示
# ------------------------------------------------------------------
if not candidates:
    st.info("EV 閾値を超える推奨馬券がありません。")
    st.stop()

display_df = candidates_to_display_df(candidates)

col_left, col_right = st.columns([3, 1])
with col_left:
    st.subheader(f"推奨馬券（{len(candidates)} 件）")
    st.dataframe(display_df, use_container_width=True, hide_index=True)

with col_right:
    total_stake = sum(c.stake for c in candidates)
    st.metric("推奨総額", f"¥{int(total_stake):,}")
    st.metric("bankroll", f"¥{int(op_config.bankroll):,}")
    st.metric("使用率", f"{total_stake / op_config.bankroll * 100:.1f}%")

# ------------------------------------------------------------------
# Advisory モード: 承認ボタン
# ------------------------------------------------------------------
st.divider()
if op_config.operation_mode == ADVISORY:
    st.subheader("承認（Advisory モード）")
    st.caption("推奨を確認後、「承認して記録」ボタンを押すと投票履歴に保存されます。実際の馬券購入は人間が行ってください。")

    if st.button("✅ 承認して記録", type="primary"):

        def recorder(r):
            return append_history(r, DEFAULT_HISTORY_PATH)

        executor = create_bet_executor(ADVISORY, recorder)
        records = executor.execute(candidates)
        st.success(f"{len(records)} 件を投票履歴に記録しました。")
        st.balloons()

elif op_config.operation_mode == "semi_auto":
    st.subheader("購入リスト出力（Semi-Auto モード）")
    csv = display_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("📥 CSV ダウンロード", csv, file_name=f"bets_{race_id}.csv", mime="text/csv")

else:
    st.warning("full_auto モードは本番環境でのみ有効です。")
