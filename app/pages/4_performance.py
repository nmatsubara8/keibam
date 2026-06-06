"""成績・設定ページ。

回収率推移 / AUC 推移 / 特徴量重要度 / スタッキング寄与 / 較正プロット /
EV 閾値スイープ / 投票履歴 / モデルバージョン管理 / config.yaml 編集 を提供する。
"""

import datetime as dt

import numpy as np
import pandas as pd
import streamlit as st

from app._betting_history import DEFAULT_HISTORY_PATH
from app._betting_history import calc_summary_stats
from app._betting_history import history_to_dataframe
from app._betting_history import load_history
from app._data_loader import find_model_paths
from app._data_loader import list_model_versions
from app._data_loader import load_model_by_version
from app._data_loader import load_operation_config
from app._model_eval import compute_calib_curves
from app._model_eval import compute_ev_sweep
from app._model_eval import compute_stacking_auc
from app._model_eval import load_featured_data
from src.simulation._plot import best_ev_threshold
from src.simulation._plot import plot_calibration
from src.simulation._plot import plot_ev_threshold_sweep
from src.simulation._plot import plot_stacking_contribution

st.set_page_config(page_title="成績・設定 — KeibaAM", page_icon="🏆", layout="wide")
st.title("🏆 成績・モデル管理・設定")

tabs = st.tabs(["📊 成績サマリ", "🔬 モデル管理", "📋 投票履歴", "⚙️ 設定"])

# ──────────────────────────────────────────────────────────────────
# 共通: featured_data & モデル（遅延読込）
# ──────────────────────────────────────────────────────────────────

@st.cache_data(show_spinner=False)
def _load_featured():
    return load_featured_data()


@st.cache_resource(show_spinner=False)
def _load_model(version: str):
    try:
        return load_model_by_version(version)
    except Exception:
        return None


# ──────────────────────────────────────────────────────────────────
# Tab 1: 成績サマリ
# ──────────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("投票成績サマリ")
    history = load_history(DEFAULT_HISTORY_PATH)
    stats = calc_summary_stats(history)

    if stats["n_bets"] == 0:
        st.info("投票履歴がまだありません。")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("総投票数", stats["n_bets"])
        c2.metric("総投資額", f"¥{stats['total_stake']:,.0f}")
        c3.metric("総払戻額", f"¥{stats['total_payout']:,.0f}")
        rr = stats["return_rate"]
        c4.metric("回収率", f"{rr * 100:.1f}%" if rr is not None else "—")

    st.subheader("AUC 推移（モデルバージョン別）")
    versions = list_model_versions()
    if versions:
        version_df = pd.DataFrame(versions)
        if "trained_at" in version_df.columns and "auc_test" in version_df.columns:
            chart_data = version_df[["trained_at", "auc_test", "version"]].set_index("trained_at")
            st.line_chart(chart_data["auc_test"], use_container_width=True)
            st.dataframe(
                version_df[["version", "trained_at", "auc_test", "n_races", "use_stacking"]],
                hide_index=True,
            )
    else:
        st.info("バージョン履歴がありません。")

    # ── EV 閾値スイープ ────────────────────────────────────────────
    st.subheader("EV 閾値スイープ（単勝バックテスト）")
    st.caption("テストセットで閾値ごとの回収率・シャープ・賭け回数を計算します。")

    versions = list_model_versions()
    version_options = [v["version"] for v in versions] if versions else []

    if not version_options:
        st.info("バージョンが存在しません。")
    else:
        sel_ver_sweep = st.selectbox("バージョン（スイープ用）", version_options, key="sweep_version")
        featured = _load_featured()

        if featured is None:
            st.info(f"`{__import__('src.constants._local_paths', fromlist=['LocalPaths']).LocalPaths.FEATURED_DATA_PATH}` が見つかりません。")
        else:
            if st.button("スイープ実行", key="run_sweep"):
                with st.spinner("計算中…"):
                    mdl = _load_model(sel_ver_sweep)
                    if mdl is None:
                        st.error("モデルを読み込めませんでした。")
                    else:
                        eff = getattr(mdl, "effective_model", mdl)
                        sweep_df = compute_ev_sweep(eff, featured)
                        st.session_state["sweep_df"] = sweep_df

            if "sweep_df" in st.session_state:
                sdf = st.session_state["sweep_df"]
                valid = sdf.dropna(subset=["return_rate"])
                if valid.empty:
                    st.warning("有効な賭けが見つかりませんでした。EV 閾値を下げてください。")
                else:
                    opt_th = best_ev_threshold(valid, min_bets=5)
                    st.caption(f"推奨閾値（回収率最大 / n_bets≥5）: **{opt_th:.2f}**")
                    fig = plot_ev_threshold_sweep(sdf, optimal_threshold=opt_th)
                    st.pyplot(fig, use_container_width=True)
                    st.dataframe(sdf.round(4), hide_index=True, use_container_width=True)

# ──────────────────────────────────────────────────────────────────
# Tab 2: モデル管理
# ──────────────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("モデルバージョン一覧")
    versions = list_model_versions()
    model_paths = find_model_paths()

    if not versions:
        st.info("バージョン履歴がありません。")
    else:
        version_df = pd.DataFrame(versions)
        st.dataframe(version_df, use_container_width=True, hide_index=True)

    st.subheader("特徴量重要度")
    version_options = [v["version"] for v in versions] if versions else []
    if version_options:
        selected_ver = st.selectbox("バージョン", version_options, key="fi_version")
        try:
            model = load_model_by_version(selected_ver)
            fi = model.feature_importance(num_features=20)
            if fi is not None:
                st.bar_chart(fi.set_index("features")["importance"], use_container_width=True)
            else:
                st.info("特徴量重要度データがありません。")
        except Exception as e:
            st.error(f"モデル読込エラー: {e}")
    else:
        st.info("バージョンが存在しません。")

    # ── 較正プロット ──────────────────────────────────────────────
    st.subheader("較正プロット（Reliability Diagram）")
    st.caption("較正ホールドアウトでの確率較正前後を比較します。スタッキング+較正済みモデルが必要です。")

    if version_options:
        sel_ver_calib = st.selectbox("バージョン（較正用）", version_options, key="calib_version")
        featured = _load_featured()

        if featured is None:
            st.info("featured_data が見つかりません。")
        elif st.button("較正プロット表示", key="show_calib"):
            with st.spinner("計算中…"):
                mdl = _load_model(sel_ver_calib)
                if mdl is None:
                    st.error("モデルを読み込めませんでした。")
                else:
                    data = compute_calib_curves(mdl, featured)
                    if data is None:
                        st.info("較正済みモデルが見つかりません（スタッキング+Isotonic 較正が必要です）。")
                    else:
                        fig = plot_calibration(data["y_true"], data["prob_pre"], data["prob_post"])
                        st.pyplot(fig, use_container_width=True)

    # ── スタッキング寄与 ──────────────────────────────────────────
    st.subheader("スタッキング寄与（AUC 比較）")
    st.caption("テストセットで各 base モデルと meta スタッキングの AUC を比較します。")

    if version_options:
        sel_ver_stack = st.selectbox("バージョン（スタッキング用）", version_options, key="stack_version")
        featured = _load_featured()

        if featured is None:
            st.info("featured_data が見つかりません。")
        elif st.button("スタッキング寄与表示", key="show_stack"):
            with st.spinner("計算中…"):
                mdl = _load_model(sel_ver_stack)
                if mdl is None:
                    st.error("モデルを読み込めませんでした。")
                else:
                    data = compute_stacking_auc(mdl, featured)
                    if data is None:
                        st.info("スタッキングモデルが見つかりません。")
                    else:
                        fig = plot_stacking_contribution(
                            data["y_true"],
                            data["base_probs"],
                            data["meta_probs"],
                            base_names=data["base_names"],
                        )
                        st.pyplot(fig, use_container_width=True)
    else:
        st.info("バージョンが存在しません。")

# ──────────────────────────────────────────────────────────────────
# Tab 3: 投票履歴
# ──────────────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("投票履歴")
    history = load_history(DEFAULT_HISTORY_PATH)
    hist_df = history_to_dataframe(history)

    if hist_df.empty:
        st.info("投票履歴がありません。")
    else:
        bet_types_in_hist = hist_df["bet_type"].unique().tolist() if "bet_type" in hist_df.columns else []
        if bet_types_in_hist:
            selected_types = st.multiselect("馬券種フィルタ", bet_types_in_hist, default=bet_types_in_hist)
            hist_df = hist_df[hist_df["bet_type"].isin(selected_types)]

        st.dataframe(hist_df, use_container_width=True, hide_index=True)
        csv = hist_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("📥 CSV ダウンロード", csv, file_name="betting_history.csv", mime="text/csv")

# ──────────────────────────────────────────────────────────────────
# Tab 4: 設定
# ──────────────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("運用設定 (config.yaml)")
    op_config = load_operation_config()

    st.code(
        f"""\
operation_mode: {op_config.operation_mode}
bankroll: {op_config.bankroll}
kelly_fraction_ratio: {op_config.kelly_fraction_ratio}
per_bet_cap_ratio: {op_config.per_bet_cap_ratio}
max_daily_ratio: {op_config.max_daily_ratio}
""",
        language="yaml",
    )
    st.caption(
        "設定を変更するには `config.yaml` を直接編集してください。"
        " `operation_mode` を `full_auto` にする場合は規約・法的リスクを確認してください。"
    )

    st.divider()
    st.subheader("継続学習ジョブ実行")
    st.caption("以下のコマンドを VPS の cron に登録してください。")
    st.code(
        "# 日次取込（翌日 02:00）\n"
        "0 2 * * * cd /path/to/keibam && python -m src.pipeline.run_pipeline ingest --race-id ...\n\n"
        "# 週次再学習（月曜 03:00）\n"
        "0 3 * * 1 cd /path/to/keibam && python -m src.pipeline.run_pipeline retrain",
        language="bash",
    )
