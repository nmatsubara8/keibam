"""バックテスト・確信度スイープページ。

過去データで AI 推奨通りに馬券を購入した場合の通算成績シミュレーション、
および確信度（EV 閾値）をパラメータとした回収率・的中率・損益の感度分析を提供する。
"""

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd
import streamlit as st

from app._data_loader import list_model_versions
from app._data_loader import load_model_by_version
from app._model_eval import _build_return_table_df
from app._model_eval import compute_confidence_sweep
from app._model_eval import compute_full_backtest
from app._model_eval import load_featured_data
from src.constants._local_paths import LocalPaths
from src.simulation._plot import best_ev_threshold
from src.simulation._plot import plot_confidence_sweep

st.set_page_config(page_title="バックテスト — KeibaAM", page_icon="🔍", layout="wide")
st.title("🔍 バックテスト・確信度スイープ")

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


versions = list_model_versions()
version_options = [v["version"] for v in versions] if versions else []

if not version_options:
    st.warning("モデルが見つかりません。先に `retrain` を実行してください。")
    st.stop()

featured = _load_featured()
if featured is None:
    st.warning(f"`{LocalPaths.FEATURED_DATA_PATH}` が見つかりません。先に取込・特徴量生成を実行してください。")
    st.stop()

tabs = st.tabs(["🎯 フルシミュレーション", "📈 確信度スイープ"])

# ──────────────────────────────────────────────────────────────────
# Tab 1: フルシミュレーション
# ──────────────────────────────────────────────────────────────────
with tabs[0]:
    st.subheader("AI 推奨通りに馬券を購入した場合の通算成績")
    st.caption(
        "テストセット（直近 20%）で EV 閾値を超えた馬に単勝 100 円ずつ賭けた場合の成績です。"
        " EV = P(勝利) × 単勝オッズ。"
    )

    col_ver, col_th = st.columns([2, 2])
    with col_ver:
        sel_ver = st.selectbox("モデルバージョン", version_options, key="bt_version")
    with col_th:
        ev_th = st.slider("EV 閾値（確信度）", min_value=1.0, max_value=3.0, value=1.5, step=0.05, key="bt_threshold")

    if st.button("シミュレーション実行", key="run_backtest"):
        with st.spinner("計算中…"):
            mdl = _load_model(sel_ver)
            if mdl is None:
                st.error("モデルを読み込めませんでした。")
            else:
                result = compute_full_backtest(mdl, featured, ev_threshold=ev_th)
                st.session_state["bt_result"] = result

    if "bt_result" in st.session_state:
        result = st.session_state["bt_result"]
        summary = result.get("summary", {})
        per_race = result.get("per_race", pd.DataFrame())

        if not summary:
            st.warning("選択した閾値では賭けが一件も成立しませんでした。閾値を下げてください。")
        else:
            # ── サマリメトリクス ───────────────────────────────────
            c1, c2, c3, c4 = st.columns(4)
            rr = summary.get("return_rate", 0.0)
            c1.metric("回収率", f"{rr * 100:.1f}%", delta=f"{(rr - 1) * 100:.1f}%")
            c2.metric("的中率", f"{summary.get('hit_rate', 0) * 100:.1f}%")
            c3.metric("総損益", f"¥{summary.get('profit', 0):,.0f}")
            c4.metric("賭け回数", f"{summary.get('n_bets', 0):,} 件")

            c5, c6, c7 = st.columns(3)
            c5.metric("対象レース数", f"{summary.get('n_races', 0):,}")
            c6.metric("シャープレシオ", f"{summary.get('sharpe_ratio', 0):.3f}")
            c7.metric("最大ドローダウン", f"¥{summary.get('max_drawdown', 0):,.0f}")

            # ── 累積損益グラフ ─────────────────────────────────────
            if not per_race.empty and "cumulative_profit" in per_race.columns:
                st.subheader("累積損益推移")
                # race_id をインデックスにすることで、グラフのツールチップと
                # テーブルの race_id が対応して照会しやすくなる
                chart_df = (
                    per_race.set_index("race_id")[["cumulative_profit"]]
                    .rename(columns={"cumulative_profit": "累積損益 (円)"})
                )
                st.line_chart(chart_df, use_container_width=True)

            # ── レース別テーブル ───────────────────────────────────
            st.subheader("レース別成績")
            _all_cols = [
                "race_id", "n_bets", "bet_amount", "return_amount",
                "hit_or_not", "profit", "cumulative_profit",
            ]
            display_cols = [c for c in _all_cols if c in per_race.columns]
            st.dataframe(
                per_race[display_cols].rename(columns={
                    "race_id": "race_id", "n_bets": "賭け数", "bet_amount": "投資額",
                    "return_amount": "払戻額", "hit_or_not": "的中", "profit": "損益",
                    "cumulative_profit": "累積損益",
                }),
                use_container_width=True,
                hide_index=True,
            )
            csv = per_race[display_cols].to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 CSV ダウンロード", csv, file_name="backtest_per_race.csv", mime="text/csv")

            # ── 賭け明細（掛け目・実着順） ─────────────────────────
            per_bet = result.get("per_bet", pd.DataFrame())
            rp = result.get("return_processor")
            if not per_bet.empty:
                st.subheader("賭け明細（掛け目・実着順）")
                st.caption(
                    "EV 閾値を超えて実際に賭けた 1 頭ごとの明細です。"
                    " race_id で絞り込むと、そのレースの掛け目（馬番）と実着順を照会できます。"
                )
                race_filter = st.selectbox(
                    "race_id で絞り込み",
                    ["（全件）"] + per_bet["race_id"].astype(str).unique().tolist(),
                    key="bet_detail_race",
                )
                detail = per_bet
                if race_filter != "（全件）":
                    detail = per_bet[per_bet["race_id"].astype(str) == race_filter]
                # 表示用コピー: 予測勝率を % 表示にするため 100 倍する（per_bet 自体は
                # CSV 用に 0〜1 のまま保持）。
                detail_view = detail.copy()
                if "予測勝率" in detail_view.columns:
                    detail_view["予測勝率"] = detail_view["予測勝率"] * 100
                # Pandas Styler はセル数上限（26万）があり全件（数十万行）で例外に
                # なるため、セル数制限のない column_config で数値フォーマットする。
                col_cfg: dict = {
                    "予測勝率": st.column_config.NumberColumn(format="%.1f%%"),
                    "単勝オッズ": st.column_config.NumberColumn(format="%.1f"),
                    "EV": st.column_config.NumberColumn(format="%.2f"),
                    "払戻": st.column_config.NumberColumn(format="%.1f"),
                    "損益": st.column_config.NumberColumn(format="%+.1f"),
                    "複勝払戻": st.column_config.NumberColumn(format="%.1f"),
                }
                st.dataframe(
                    detail_view,
                    use_container_width=True,
                    hide_index=True,
                    column_config=col_cfg,
                )
                bet_csv = per_bet.to_csv(index=False).encode("utf-8-sig")
                st.download_button(
                    "📥 賭け明細 CSV ダウンロード", bet_csv,
                    file_name="backtest_per_bet.csv", mime="text/csv",
                )

                # ── 払戻テーブル（全馬券種） ────────────────────────
                if rp is not None and race_filter != "（全件）":
                    with st.expander("🎰 このレースの払戻テーブル（全馬券種）"):
                        rt_df = _build_return_table_df(rp, race_filter)
                        if rt_df.empty:
                            st.info("払戻データが見つかりませんでした。")
                        else:
                            st.dataframe(
                                rt_df,
                                use_container_width=True,
                                hide_index=True,
                                column_config={
                                    "払戻(円)": st.column_config.NumberColumn(format="%,d"),
                                },
                            )
                elif rp is None:
                    st.caption("💡 払戻テーブル（複勝・馬連等）を表示するには return_tables データが必要です。"
                               " race_id を絞り込むと全馬券種払戻テーブルを表示できます。")

# ──────────────────────────────────────────────────────────────────
# Tab 2: 確信度スイープ
# ──────────────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("確信度（EV 閾値）スイープ")
    st.caption(
        "EV 閾値を 1.0〜2.5 の範囲でスイープし、回収率・的中率・シャープ・賭け回数がどう変化するかを示します。"
    )

    sel_ver_sw = st.selectbox("モデルバージョン", version_options, key="sw_version")

    if st.button("スイープ実行", key="run_sweep_full"):
        with st.spinner("計算中（閾値 16 点）…"):
            mdl = _load_model(sel_ver_sw)
            if mdl is None:
                st.error("モデルを読み込めませんでした。")
            else:
                sweep_df = compute_confidence_sweep(mdl, featured)
                st.session_state["sw_result"] = sweep_df

    if "sw_result" in st.session_state:
        sdf = st.session_state["sw_result"]
        valid = sdf.dropna(subset=["return_rate"])

        if valid.empty:
            st.warning("有効な賭けが見つかりませんでした。")
        else:
            opt_th = best_ev_threshold(valid, min_bets=5)
            st.caption(f"推奨閾値（回収率最大 / n_bets≥5）: **{opt_th:.2f}**")

            fig = plot_confidence_sweep(sdf, optimal_threshold=opt_th)
            st.pyplot(fig, use_container_width=True)

            # 数値テーブル
            st.subheader("スイープ詳細")
            display_sdf = sdf.copy()
            for pct_col in ["return_rate", "hit_rate"]:
                if pct_col in display_sdf.columns:
                    display_sdf[pct_col] = display_sdf[pct_col].map(lambda x: f"{x:.1%}" if pd.notna(x) else "—")
            for num_col in ["profit", "max_drawdown", "sharpe_ratio"]:
                if num_col in display_sdf.columns:
                    display_sdf[num_col] = display_sdf[num_col].map(lambda x: f"{x:.3f}" if pd.notna(x) else "—")
            st.dataframe(display_sdf, use_container_width=True, hide_index=True)

            csv = sdf.to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 CSV ダウンロード", csv, file_name="confidence_sweep.csv", mime="text/csv")
