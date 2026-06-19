"""モデルラボ — ハイパラ探索結果の選択とモデル比較シミュレーション。

- Optuna 探索結果（tuning_history.json）を成績順に一覧表示し、
  どのパラメータで再学習するかを選択・保存できる。
- 複数のモデルバージョンを同一条件でバックテストし、
  「どのモデルを採用するとどうなるか」を比較できる。
"""

import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import datetime as dt
import json
import os

import pandas as pd
import streamlit as st

from app._bet_type_optimizer import BET_TYPE_LABELS
from app._bet_type_optimizer import default_grid
from app._bet_type_optimizer import optimize_all
from app._bet_type_optimizer import results_to_frame
from app._data_loader import find_model_paths
from app._data_loader import list_model_versions
from app._data_loader import load_model_by_version
from app._model_compare import BET_POLICY_CHOICES
from app._model_compare import align_profit_curves
from app._model_compare import comparison_table
from app._model_compare import cumulative_profit
from app._model_compare import recent_race_slice
from app._model_compare import simulate_model
from app._model_eval import _load_return_processor
from app._model_eval import load_featured_data
from src.policies._bet_type_params import OPTIMIZABLE_BET_TYPES
from src.policies._bet_type_params import bet_type_params_path
from src.policies._bet_type_params import latest_bet_type_params
from src.policies._bet_type_params import save_bet_type_params
from app._tuning_job import refresh_job_status
from app._tuning_job import start_tuning_job
from app._tuning_job import stop_tuning_job
from app._tuning_job import tail_log
from src.training._tuning_history import load_tuning_history
from src.training._tuning_history import tuning_history_path

st.set_page_config(page_title="モデルラボ — KeibaAM", page_icon="🧪", layout="wide")
st.title("🧪 モデルラボ")

SELECTED_PARAMS_PATH = os.path.join("models", "selected_params.json")

tabs = st.tabs([
    "🔧 ハイパラ探索結果", "🆚 モデル比較シミュレーション",
    "📈 オッズ力学モデル", "🎛️ 券種別最適化",
])


@st.cache_data(show_spinner=False)
def _load_featured():
    return load_featured_data()


@st.cache_resource(show_spinner="モデル読込中...")
def _load_model(version: str):
    return load_model_by_version(version)


# ──────────────────────────────────────────────────────────────────
# Tab 1: ハイパラ探索結果（成績順）と選択
# ──────────────────────────────────────────────────────────────────
with tabs[0]:
    # ──────────────────────────────────────────────────────────────
    # Optuna 探索の起動 + 状態監視（subprocess デタッチ起動）
    # ──────────────────────────────────────────────────────────────
    st.subheader("⚙️ Optuna 探索を起動")
    job = refresh_job_status("models")
    running = bool(job and job.get("status") == "running")

    if running:
        st.warning(
            f"🟢 探索ジョブ実行中（pid={job.get('pid')} / 開始 {job.get('started_at', '')[:16]}）。"
            "完了まで時間がかかります（フル探索で約 2 時間）。"
        )
        cols = st.columns([1, 1, 4])
        with cols[0]:
            if st.button("🔄 状態を更新"):
                st.rerun()
        with cols[1]:
            if st.button("🛑 中止", help="探索プロセスに SIGTERM を送って停止します"):
                stop_tuning_job("models")
                st.rerun()
    else:
        if job:
            _icon = {"completed": "✅", "failed": "❌", "cancelled": "⏹️", "unknown": "⚠️"}.get(
                job.get("status"), "ℹ️"
            )
            st.caption(
                f"{_icon} 前回ジョブ: {job.get('status')} "
                f"(終了コード {job.get('exit_code')} / {job.get('finished_at', '') or '—'})"
            )
        st.caption(
            "下のボタンで `retrain --with-tuning` をバックグラウンド起動します。"
            "完了後、探索結果がこのタブに成績順で表示されます。"
        )
        ack = st.checkbox("約 2 時間かかること・完了後は再学習が走ることを理解しました")
        if st.button("🚀 Optuna 探索を起動", type="primary", disabled=not ack):
            try:
                started = start_tuning_job("models")
                st.success(f"探索ジョブを起動しました（pid={started['pid']}）。")
                st.rerun()
            except RuntimeError as e:
                st.error(str(e))

    # 実行ログ（末尾）。実行中・直後の確認用。
    _log = tail_log("models", n=30)
    if _log:
        with st.expander("実行ログ（末尾 30 行）", expanded=running):
            st.code(_log, language="text")

    st.divider()

    history = load_tuning_history(tuning_history_path("models"))
    if not history:
        st.info(
            "チューニング履歴がありません。上の「🚀 Optuna 探索を起動」ボタン、または "
            "`python -m src.pipeline.run_pipeline retrain --with-tuning` を実行すると、"
            "Optuna の全探索結果が成績順でここに表示されます。"
        )
    else:
        versions = sorted({r["version"] for r in history}, reverse=True)
        sel_version = st.selectbox("探索バージョン", versions)
        records = [r for r in history if r["version"] == sel_version]
        records.sort(key=lambda r: r["rank"])

        # 成績順テーブル（主要パラメータを展開して表示）
        table = pd.DataFrame(
            [
                {
                    "rank": r["rank"],
                    f"value ({r.get('metric', 'logloss')}↓)": r["value"],
                    "num_leaves": r["params"].get("num_leaves"),
                    "feature_fraction": r["params"].get("feature_fraction"),
                    "bagging_fraction": r["params"].get("bagging_fraction"),
                    "lambda_l1": r["params"].get("lambda_l1"),
                    "lambda_l2": r["params"].get("lambda_l2"),
                    "min_child_samples": r["params"].get("min_child_samples"),
                    "trial": r["trial_number"],
                }
                for r in records
            ]
        ).set_index("rank")
        st.dataframe(table, use_container_width=True)

        sel_rank = st.selectbox("使用するパラメータ（rank）", [r["rank"] for r in records])
        sel_record = next(r for r in records if r["rank"] == sel_rank)
        with st.expander("選択中パラメータの全項目", expanded=False):
            st.json(sel_record["params"])

        col1, col2 = st.columns([1, 2])
        with col1:
            if st.button("✅ このパラメータを選択として保存", type="primary"):
                payload = {
                    "version": sel_version,
                    "rank": sel_rank,
                    "value": sel_record["value"],
                    "params": sel_record["params"],
                    "selected_at": dt.datetime.now().isoformat(),
                }
                os.makedirs(os.path.dirname(SELECTED_PARAMS_PATH), exist_ok=True)
                with open(SELECTED_PARAMS_PATH, "w") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                st.success(f"rank={sel_rank} を {SELECTED_PARAMS_PATH} に保存しました")
        with col2:
            st.code(
                f"python -m src.pipeline.run_pipeline retrain --params-rank {sel_rank}",
                language="bash",
            )

        if os.path.exists(SELECTED_PARAMS_PATH):
            with open(SELECTED_PARAMS_PATH) as f:
                current = json.load(f)
            st.caption(
                f"現在の選択: version={current.get('version')} rank={current.get('rank')} "
                f"(value={current.get('value'):.5f}, {current.get('selected_at', '')[:16]})"
            )

# ──────────────────────────────────────────────────────────────────
# Tab 2: モデル比較シミュレーション
# ──────────────────────────────────────────────────────────────────
with tabs[1]:
    model_paths = find_model_paths("models")
    if not model_paths:
        st.info("models/ にモデルがありません。`retrain` を実行してください。")
        st.stop()

    version_names = [os.path.basename(p).replace(".pickle", "") for p in model_paths]
    # version_history からメタ情報（AUC 等）を引いて表示名を作る
    meta_by_version = {m["version"]: m for m in list_model_versions("models")}

    def _label(v: str) -> str:
        m = meta_by_version.get(v)
        return f"{v} (AUC={m['auc_test']:.3f})" if m and m.get("auc_test") else v

    col1, col2, col3 = st.columns(3)
    with col1:
        sel_versions = st.multiselect(
            "比較するモデル", version_names, default=version_names[:1], format_func=_label
        )
    with col2:
        bet_label = st.selectbox("馬券種", list(BET_POLICY_CHOICES.keys()))
    with col3:
        threshold = st.slider("スコア閾値", 0.0, 5.0, 1.5, 0.1)

    test_frac = st.slider(
        "検証期間（直近レースの割合）", 0.05, 0.5, 0.2, 0.05,
        help="featured_data の日付末尾からこの割合のレースでバックテストする（DataSplitter の test 分割に相当）",
    )

    if st.button("🚀 シミュレーション実行", type="primary") and sel_versions:
        featured = _load_featured()
        if featured is None or featured.empty:
            st.error("featured_data.pkl がありません。ingest を実行してください。")
            st.stop()
        featured_slice = recent_race_slice(featured, test_frac)
        st.caption(f"対象: {featured_slice.index.nunique()} レース / {len(featured_slice)} 出走")

        summaries: dict[str, dict] = {}
        profits: dict[str, pd.Series] = {}
        diags: dict[str, dict] = {}
        progress = st.progress(0.0)
        for i, version in enumerate(sel_versions):
            try:
                ai = _load_model(version)
                summary, per_race, diag = simulate_model(ai, featured_slice, bet_label, threshold)
                diags[version] = diag
                if summary:
                    summaries[version] = summary
                    profits[version] = cumulative_profit(per_race)
            except Exception as e:  # noqa: BLE001 — 1 モデルの失敗で比較全体を止めない
                st.warning(f"{version}: シミュレーション失敗 — {e}")
            progress.progress((i + 1) / len(sel_versions))
        progress.empty()

        if not summaries:
            # 結果が空の理由を診断情報から区別して案内する。
            matched = max((d.get("n_matched_races", 0) for d in diags.values()), default=0)
            covered = max((d.get("n_covered_races", 0) for d in diags.values()), default=0)
            if matched == 0:
                st.warning("条件に合致する馬券がありませんでした（スコア閾値を下げてみてください）")
            elif covered == 0:
                st.warning(
                    f"閾値を超えて賭けた {matched} レースすべてに払戻データがありません。"
                    f"`{bet_label}` の払戻テーブル（return_tables）が未取得の可能性があります。"
                    " ingest で払戻データを取得するか、別の馬券種・期間をお試しください。"
                )
            else:
                st.warning("有効な集計結果が得られませんでした（払戻データを確認してください）。")
        else:
            st.subheader("📊 比較結果")
            comp = comparison_table(summaries)
            display = comp.rename(
                columns={
                    "return_rate": "回収率",
                    "hit_rate": "的中率",
                    "sharpe_ratio": "シャープレシオ",
                    "max_drawdown": "最大DD",
                    "profit": "損益",
                    "n_bets": "購入点数",
                    "n_races": "対象レース",
                    "n_hits": "的中数",
                    "total_bet_amount": "総購入額",
                }
            )
            st.dataframe(
                display.style.format(
                    {"回収率": "{:.1%}", "的中率": "{:.1%}", "シャープレシオ": "{:.2f}",
                     "最大DD": "{:,.0f}", "損益": "{:,.0f}", "総購入額": "{:,.0f}", "std": "{:.3f}"}
                ),
                use_container_width=True,
            )

            st.subheader("💰 資金推移（累積損益）")
            chart_df = align_profit_curves(profits)
            chart_df.index = pd.RangeIndex(1, len(chart_df) + 1, name="レース通番")
            st.line_chart(chart_df)


# ──────────────────────────────────────────────────────────────────
# Tab 3: オッズ力学モデル（投票シェア予測）の比較
# ──────────────────────────────────────────────────────────────────
with tabs[2]:
    from app._odds_dynamics_compare import eval_comparison_table
    from src.training._odds_dynamics_eval import dynamics_eval_path
    from src.training._odds_dynamics_eval import load_dynamics_eval

    records = load_dynamics_eval(dynamics_eval_path("models"))
    if not records:
        st.info(
            "オッズ力学モデルの評価結果がまだありません。スナップショットが蓄積されたら "
            "`python -m src.pipeline.run_pipeline evaluate-odds-dynamics` を実行すると、"
            "Dirichlet 回帰 / Kalman Filter / Particle Filter / アンサンブルの比較が"
            "ここに表示されます。"
        )
    else:
        st.markdown(
            "投票シェアベクトル（Σ=1）の確率過程として締切オッズを予測する各モデルの"
            " held-out 精度。**KL（実現シェア‖予測シェア）が小さいほど良い**。"
        )
        comp = eval_comparison_table(records)
        display = comp.rename(
            columns={
                "kl_mean": "KL↓",
                "winner_logloss": "勝ち馬logloss↓",
                "share_mae": "シェアMAE↓",
                "odds_mape": "オッズMAPE↓",
                "ensemble_weight": "アンサンブル重み",
                "n_test_races": "検証レース",
                "n_train_races": "学習レース",
            }
        )
        st.dataframe(
            display.style.format({
                "KL↓": "{:.4f}", "勝ち馬logloss↓": "{:.3f}", "シェアMAE↓": "{:.4f}",
                "オッズMAPE↓": "{:.1%}", "アンサンブル重み": "{:.2f}",
            }),
            use_container_width=True,
        )
        st.bar_chart(comp["kl_mean"])
        st.caption(
            "アンサンブル重みは検証 KL の逆数比。最良の総合判断モデル（ensemble）の"
            "予測確定オッズが odds_watch 経由で EV 計算に供給されます。"
        )

# ──────────────────────────────────────────────────────────────────
# Tab 4: 券種別最適化（最適化レイヤの管理）
# ──────────────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("券種別の EV 選定パラメータを最適化・管理")
    st.caption(
        "単勝勝率モデル + Harville を土台に、券種ごとの **EV 閾値 / 温度 β（人気側の尖り）/ "
        "確率較正** を実払戻バックテストで最適化します。連系の推定オッズは単勝オッズから "
        "Harville で導出（過去の連オッズは遡及取得不可のため）。保存した最適パラメータは "
        "予測ページの EV 選定に反映されます。"
    )

    _params_path = bet_type_params_path("models")
    _saved = latest_bet_type_params(_params_path)
    if _saved:
        st.markdown("**現在保存中の券種別パラメータ（最新）**")
        st.dataframe(
            pd.DataFrame(
                {bt: p.to_dict() for bt, p in _saved.items()}
            ).T.rename_axis("券種"),
            use_container_width=True,
        )
    else:
        st.info("まだ保存された券種別パラメータはありません（既定値で動作中）。")

    model_paths_bt = find_model_paths("models")
    if not model_paths_bt:
        st.info("models/ にモデルがありません。`retrain` を実行してください。")
    else:
        version_names_bt = [os.path.basename(p).replace(".pickle", "") for p in model_paths_bt]
        colA, colB, colC = st.columns(3)
        with colA:
            sel_ver_bt = st.selectbox("モデルバージョン", version_names_bt, key="bto_version")
        with colB:
            objective = st.selectbox(
                "最適化指標", ["return_rate", "sharpe_ratio"],
                format_func=lambda x: {"return_rate": "回収率", "sharpe_ratio": "シャープレシオ"}[x],
                key="bto_objective",
            )
        with colC:
            min_bets = st.number_input("最小賭け枚数（過学習防止）", 1, 1000, 30, key="bto_min_bets")

        sel_bts = st.multiselect(
            "対象券種", list(OPTIMIZABLE_BET_TYPES),
            default=list(OPTIMIZABLE_BET_TYPES),
            format_func=lambda b: BET_TYPE_LABELS.get(b, b), key="bto_bets",
        )
        test_frac = st.slider("検証期間（直近レースの割合）", 0.05, 0.5, 0.2, 0.05, key="bto_test_frac")

        if st.button("🚀 券種別最適化を実行", type="primary", key="run_bto") and sel_bts:
            featured = _load_featured()
            rp = _load_return_processor()
            if featured is None or featured.empty:
                st.error("featured_data.pkl がありません。ingest を実行してください。")
            elif rp is None:
                st.error("払戻テーブル（return_tables）がありません。ingest を実行してください。")
            else:
                with st.spinner("バックテスト探索中…"):
                    ai = _load_model(sel_ver_bt)
                    featured_slice = recent_race_slice(featured, test_frac)
                    # 払戻実績から較正済みの券種別控除率があれば連系推定オッズに反映する
                    from src.policies._takeout_calibration import latest_takeout_map
                    from src.policies._takeout_calibration import takeout_calibration_path
                    calib_takeout = latest_takeout_map(takeout_calibration_path())
                    if calib_takeout:
                        st.caption(
                            "較正済み控除率を適用: "
                            + ", ".join(f"{BET_TYPE_LABELS.get(bt, bt)}={t:.3f}"
                                        for bt, t in calib_takeout.items())
                        )
                    params_map, metrics_map, all_results = optimize_all(
                        ai, featured_slice, rp, bet_types=sel_bts,
                        grid=default_grid(), objective=objective, min_bets=int(min_bets),
                        takeout=calib_takeout or 0.2,
                    )
                    st.session_state["bto_result"] = {
                        "params_map": params_map, "metrics_map": metrics_map,
                        "all_results": all_results, "objective": objective,
                    }

        if "bto_result" in st.session_state:
            r = st.session_state["bto_result"]
            st.markdown("**最適化結果（券種別ベスト）**")
            summary_rows = []
            for bt, params in r["params_map"].items():
                m = r["metrics_map"].get(bt, {})
                summary_rows.append({
                    "券種": BET_TYPE_LABELS.get(bt, bt),
                    "EV閾値": params.ev_threshold, "温度": params.temperature,
                    "確率較正": params.prob_scale,
                    "回収率": m.get("return_rate"), "的中率": m.get("hit_rate"),
                    "シャープ": m.get("sharpe_ratio"), "賭け枚数": m.get("n_bets"),
                })
            st.dataframe(
                pd.DataFrame(summary_rows).style.format({
                    "回収率": "{:.1%}", "的中率": "{:.1%}", "シャープ": "{:.2f}",
                }, na_rep="—"),
                use_container_width=True, hide_index=True,
            )

            with st.expander("券種ごとのグリッド探索詳細"):
                for bt, res in r["all_results"].items():
                    st.markdown(f"**{BET_TYPE_LABELS.get(bt, bt)}**")
                    df = results_to_frame(res)
                    if df.empty:
                        st.caption("賭けが成立しませんでした（払戻データ・閾値を確認）。")
                    else:
                        st.dataframe(df.head(10), use_container_width=True, hide_index=True)

            if st.button("💾 この最適パラメータを保存", key="save_bto"):
                save_bet_type_params(
                    r["params_map"], _params_path,
                    objective=r["objective"],
                    metrics=r["metrics_map"],
                )
                st.success(f"{_params_path} に {len(r['params_map'])} 券種を保存しました。"
                           " 予測ページの EV 選定に反映されます。")
