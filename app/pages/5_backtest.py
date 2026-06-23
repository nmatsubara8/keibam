"""バックテスト・確信度スイープページ。

過去データで AI 推奨通りに馬券を購入した場合の通算成績シミュレーション、
および確信度（EV 閾値）をパラメータとした回収率・的中率・損益の感度分析を提供する。
"""

import os
import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd
import streamlit as st

from app._data_loader import find_model_paths
from app._data_loader import list_model_versions
from app._data_loader import load_model_by_version
from app._data_loader import load_model_from_path
from app._data_loader import load_odds_snapshots
from app._data_loader import load_win_head_for
from app._model_compare import BET_POLICY_CHOICES
from app._model_compare import cumulative_profit
from app._model_compare import recent_race_slice
from app._model_compare import simulate_model
from app._model_eval import _build_return_table_df
from app._model_eval import _load_return_processor
from app._model_eval import compute_confidence_sweep
from app._model_eval import compute_full_backtest
from app._model_eval import load_featured_data
from app._two_head_backtest import BET_TYPE_LABELS
from app._two_head_backtest import available_years
from app._two_head_backtest import run_two_head_backtest
from app._two_head_backtest import selectable_bet_types
from src.constants._local_paths import LocalPaths
from src.preparing._odds_snapshot import build_final_odds_lookup
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


@st.cache_resource(show_spinner=False)
def _load_two_head(version: str):
    """version に対応する Place モデルと Win ヘッド（<version>__win.pickle）を読み込む。

    Win ヘッドが無い旧モデルでは win=None（連系は単勝 Harville 推定で評価）。
    """
    for path in find_model_paths("models"):
        if version in os.path.basename(path):
            return load_model_from_path(path), load_win_head_for(path)
    return None, None


@st.cache_resource(show_spinner=False)
def _load_return_processor_cached():
    return _load_return_processor()


@st.cache_data(show_spinner=False)
def _load_final_odds_lookup():
    """確定オッズ lookup（スナップショット由来）を構築する（なければ None）。"""
    snaps = load_odds_snapshots()
    return build_final_odds_lookup(snaps) if snaps else None


versions = list_model_versions()
version_options = [v["version"] for v in versions] if versions else []

if not version_options:
    st.warning("モデルが見つかりません。先に `retrain` を実行してください。")
    st.stop()

featured = _load_featured()
if featured is None:
    st.warning(f"`{LocalPaths.FEATURED_DATA_PATH}` が見つかりません。先に取込・特徴量生成を実行してください。")
    st.stop()

tabs = st.tabs([
    "🎯 フルシミュレーション",
    "📈 確信度スイープ",
    "🎰 券種別バックテスト",
    "🎯🎯 2ヘッド+確定オッズ",
])

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

# ──────────────────────────────────────────────────────────────────
# Tab 3: 券種別バックテスト（単勝以外も照会）
# ──────────────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("単勝以外も含む券種別の通算成績")
    st.caption(
        "テストセット（直近 20%）で、レース内標準化スコア（偏差値的な相対評価）が"
        " 閾値を超えた馬に各券種で 1 点ずつ賭けた場合の成績です。"
        " BOX 馬券は閾値超えの馬の全組合せを購入します。実払戻テーブルで清算します"
        "（回収率・的中率は賭け金額に依存しません。損益は 1 点=1 円換算）。"
    )

    col_v, col_b, col_t = st.columns([2, 2, 2])
    with col_v:
        sel_ver_bt = st.selectbox("モデルバージョン", version_options, key="bbt_version")
    with col_b:
        bet_label = st.selectbox("馬券種", list(BET_POLICY_CHOICES.keys()), key="bbt_bet")
    with col_t:
        bt_th = st.slider(
            "スコア閾値（レース内標準化）", min_value=-0.5, max_value=3.0,
            value=1.0, step=0.1, key="bbt_threshold",
            help="レース内で標準化したスコアの閾値。低いほど対象馬が増え、BOX の組合せ数も増えます。",
        )

    if st.button("券種別シミュレーション実行", key="run_bet_backtest"):
        with st.spinner("計算中…"):
            mdl = _load_model(sel_ver_bt)
            if mdl is None:
                st.error("モデルを読み込めませんでした。")
            else:
                featured_slice = recent_race_slice(featured, test_frac=0.2)
                summary, per_race, diag = simulate_model(mdl, featured_slice, bet_label, bt_th)
                st.session_state["bbt_result"] = {
                    "summary": summary, "per_race": per_race, "diag": diag, "bet_label": bet_label,
                }

    if "bbt_result" in st.session_state:
        res = st.session_state["bbt_result"]
        summary = res["summary"]
        per_race = res["per_race"]
        diag = res["diag"]

        # 診断: 賭け成立レース数と払戻テーブルでカバーされたレース数
        st.caption(
            f"閾値超えで賭けたレース: {diag.get('n_matched_races', 0)} / "
            f"うち払戻テーブルあり: {diag.get('n_covered_races', 0)}"
        )

        if not summary:
            if diag.get("n_matched_races", 0) == 0:
                st.warning("この閾値では賭けが一件も成立しませんでした。閾値を下げてください。")
            else:
                st.warning(
                    f"`{res['bet_label']}` の払戻テーブル（return_tables）が未取得の可能性があります。"
                    " ingest で払戻データを取得するか、別の券種・期間をお試しください。"
                )
        else:
            c1, c2, c3, c4 = st.columns(4)
            rr = summary.get("return_rate", 0.0)
            c1.metric("回収率", f"{rr * 100:.1f}%", delta=f"{(rr - 1) * 100:.1f}%")
            c2.metric("的中率", f"{summary.get('hit_rate', 0) * 100:.1f}%")
            c3.metric("総損益", f"¥{summary.get('profit', 0):,.0f}")
            c4.metric("賭け枚数", f"{summary.get('n_bets', 0):,} 枚")

            c5, c6, c7 = st.columns(3)
            c5.metric("対象レース数", f"{summary.get('n_races', 0):,}")
            c6.metric("シャープレシオ", f"{summary.get('sharpe_ratio', 0):.3f}")
            c7.metric("最大ドローダウン", f"¥{summary.get('max_drawdown', 0):,.0f}")

            curve = cumulative_profit(per_race)
            if not curve.empty:
                st.subheader("累積損益推移")
                st.line_chart(
                    curve.rename("累積損益 (円)").to_frame(), use_container_width=True
                )

            if not per_race.empty:
                st.subheader("レース別成績")
                disp = per_race.copy()
                disp.index.name = "race_id"
                st.dataframe(
                    disp.rename(columns={
                        "n_bets": "賭け枚数", "bet_amount": "投資額",
                        "return_amount": "払戻額", "hit_or_not": "的中",
                    }),
                    use_container_width=True,
                )
                csv = per_race.to_csv().encode("utf-8-sig")
                st.download_button(
                    "📥 CSV ダウンロード", csv,
                    file_name=f"backtest_{BET_POLICY_CHOICES[res['bet_label']][1]}.csv",
                    mime="text/csv",
                )

# ──────────────────────────────────────────────────────────────────
# Tab 4: 2ヘッド予測 + 確定オッズの券種別 EV バックテスト（CLI backtest と同経路）
# ──────────────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("2ヘッド予測 × 確定オッズ × 実払戻の券種別 EV バックテスト")
    st.caption(
        "CLI `run_pipeline backtest` と同じ評価です。"
        " Place（複勝/top3）と Win（単勝/1着）の 2 ヘッドで EV 選定し、確定オッズ"
        "（odds_snapshots、無い組合せは単勝 Harville 推定にフォールバック）で期待値を計算、"
        " 実払戻テーブルで決済します（フラット 1 点=1 単位）。"
    )
    st.caption(
        "⚠️ リーク回避: 評価年はモデルの**学習年と重ねない**でください"
        "（例: 学習 ≤2024 / 評価 2025）。"
    )

    col_v2, col_y2 = st.columns([2, 3])
    with col_v2:
        sel_ver_2h = st.selectbox("モデルバージョン（Place）", version_options, key="th_version")
    with col_y2:
        year_opts = available_years(featured)
        sel_years = st.multiselect(
            "評価対象の年（race_id 先頭 4 桁・未選択で全期間）",
            year_opts, default=[], key="th_years",
            help="学習年と重ならない年だけを選ぶとリークのない ROI が得られます。",
        )

    col_o2, col_w2 = st.columns([2, 2])
    with col_o2:
        use_final_odds = st.checkbox(
            "確定オッズを使う（odds_snapshots）", value=True, key="th_final_odds",
            help="オフにすると全券種を単勝からの Harville 推定オッズで評価します。",
        )
    with col_w2:
        use_win_head = st.checkbox(
            "Win ヘッドを使う（連系の Harville に 1着勝率を供給）",
            value=True, key="th_win_head",
            help="オフにすると Place（複勝確率）のみで連系を評価します。",
        )

    bt_targets = selectable_bet_types()
    sel_bet_types = st.multiselect(
        "評価する券種（未選択で全券種）",
        bt_targets,
        default=[],
        format_func=lambda bt: BET_TYPE_LABELS.get(bt, bt),
        key="th_bet_types",
        help="枠連は Harville（馬番）未対応のため対象外です。",
    )

    if st.button("2ヘッドバックテスト実行", key="run_two_head_bt"):
        with st.spinner("計算中…"):
            place_ai, win_ai = _load_two_head(sel_ver_2h)
            rp = _load_return_processor_cached()
            if place_ai is None:
                st.error("Place モデルを読み込めませんでした。")
            elif rp is None:
                st.error("払戻テーブル（return_tables）を読み込めませんでした。ingest で取得してください。")
            else:
                final_lookup = _load_final_odds_lookup() if use_final_odds else None
                result = run_two_head_backtest(
                    place_ai,
                    featured,
                    rp,
                    win_ai=win_ai if use_win_head else None,
                    final_odds_lookup=final_lookup,
                    bet_types=sel_bet_types or None,
                    years=sel_years or None,
                )
                st.session_state["th_result"] = {
                    "result": result,
                    "win_used": use_win_head and win_ai is not None,
                    "n_final_odds": len(final_lookup or {}),
                }

    if "th_result" in st.session_state:
        stored = st.session_state["th_result"]
        result = stored["result"]
        frame = result.get("frame", pd.DataFrame())

        st.caption(
            f"対象レース: {result.get('n_races', 0):,} / 買い目総数:"
            f" {result.get('n_candidates', 0):,} / Win ヘッド:"
            f" {'使用' if stored['win_used'] else '未使用'} / 確定オッズ:"
            f" {stored['n_final_odds']:,} 件"
        )

        if frame.empty:
            st.warning(
                "賭けが一件も成立しませんでした。閾値（券種別 EV 既定）に対し候補が無いか、"
                " 払戻テーブルに該当レースがありません。評価年・券種を見直してください。"
            )
        else:
            overall = result.get("overall")
            if overall is not None:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("回収率（全体）", f"{overall.roi * 100:.1f}%",
                          delta=f"{(overall.roi - 1) * 100:.1f}%")
                c2.metric("的中率（全体）", f"{overall.hit_rate * 100:.1f}%")
                c3.metric("総損益（単位券）", f"{overall.profit:+,.0f}")
                c4.metric("買い目点数", f"{overall.n_bets:,} 点")

            st.subheader("券種別成績")
            # 的中率・回収率は 0〜1 スケールなので、表示用コピーで % に直す
            # （NumberColumn の format は格納値にそのまま適用されるため）。
            frame_view = frame.copy()
            for pct_col in ["的中率", "回収率"]:
                frame_view[pct_col] = frame_view[pct_col] * 100
            st.dataframe(
                frame_view,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "的中率": st.column_config.NumberColumn(format="%.1f%%"),
                    "回収率": st.column_config.NumberColumn(format="%.1f%%"),
                    "投票": st.column_config.NumberColumn(format="%.0f"),
                    "払戻": st.column_config.NumberColumn(format="%.1f"),
                    "損益": st.column_config.NumberColumn(format="%+.1f"),
                },
            )
            # CSV は割合（0〜1）のまま出力する。
            csv = frame.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📥 CSV ダウンロード", csv,
                file_name="backtest_two_head.csv", mime="text/csv",
            )
