"""レーティング・ラボ（Phase 1: ペアワイズ Elo）。

- タブ1「レーティング照会」: 最新スナップショットと、レース内の Elo・Elo 式勝率を
  再学習なしで即時表示する。
- タブ2「On/Off A/B」: レーティング特徴量の有無で学習した 2 モデルを選び、
  ROI・AUC・logloss・キャリブレーションを左右に並べて効果を比較する。

機能ごとの On/Off は ELO_FEATURE_COLS を起点に拡張する設計（Phase 2-5 で列が増える）。
計算ロジックは app/_model_eval.py / app/_model_compare.py / src の純粋関数を再利用する。
"""

import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import json
import os

import numpy as np
import pandas as pd
import streamlit as st

from app._data_loader import find_model_paths
from app._data_loader import load_model_from_path
from app._model_eval import compute_calib_curves
from app._model_eval import compute_full_backtest
from app._model_eval import compute_stacking_auc
from src.constants._feature_cols import COND_DIMENSIONS
from src.constants._feature_cols import COND_TS_FEATURE_COLS
from src.constants._feature_cols import ELO_FEATURE_COLS
from src.constants._feature_cols import TS_FEATURE_COLS
from src.constants._local_paths import LocalPaths
from src.constants._results_cols import ResultsCols
from src.preprocessing._ratings import elo_win_probabilities
from src.preprocessing._trueskill import trueskill_win_probabilities

st.set_page_config(page_title="レーティング・ラボ — KeibaAM", page_icon="📊", layout="wide")
st.title("📊 レーティング・ラボ（Elo / TrueSkill / 条件別）")
st.caption(
    "各馬の地力を対戦結果から推定するレーティング（Phase 1: ペアワイズ Elo・着差補正 / "
    "Phase 2: TrueSkill μ/σ / Phase 3: 条件別 TrueSkill）の照会と、特徴量 On/Off の A/B 比較。"
)

# フラットなスナップショットを持つレーティングファミリー。
_FAMILIES = {
    "Elo": {
        "path": LocalPaths.HORSE_RATINGS_PATH,
        "feature_cols": ELO_FEATURE_COLS,
        "primary": "elo_rating",          # スナップショットの主キー指標
        "snap_sort": "rating",
    },
    "TrueSkill": {
        "path": LocalPaths.HORSE_TRUESKILL_PATH,
        "feature_cols": TS_FEATURE_COLS,
        "primary": "ts_conservative",
        "snap_sort": "mu",
    },
}
_COND_FAMILY = "条件別TrueSkill"  # ネスト snapshot のため特別扱い


# ------------------------------------------------------------------
# データ読込（キャッシュ）
# ------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def _load_snapshot(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


@st.cache_data(show_spinner="特徴量データを読み込み中…")
def _load_featured() -> pd.DataFrame | None:
    path = LocalPaths.FEATURED_DATA_PATH
    if not os.path.exists(path):
        return None
    return pd.read_pickle(path)


@st.cache_resource(show_spinner="モデルを読み込み中…")
def _load_model(path: str):
    return load_model_from_path(path)


featured = _load_featured()

tab1, tab2 = st.tabs(["🔎 レーティング照会", "⚖️ On/Off A/B"])

# ==================================================================
# タブ1: レーティング照会（再学習不要・即時）
# ==================================================================
with tab1:
    family = st.radio(
        "レーティングモデル", [*_FAMILIES.keys(), _COND_FAMILY], horizontal=True
    )

    if family == _COND_FAMILY:
        # 条件別: snapshot がネスト構造のため、選択レースの条件別レーティングを表示。
        st.subheader("レース内 条件別 TrueSkill（即時・再学習不要）")
        st.caption(
            "各次元（surface=芝/ダ・distance=距離帯・around=回り）について、当該レース条件"
            "での保守的スキル（μ-3σ）とフィールド相対値を表示します。"
        )
        cond_primary = COND_TS_FEATURE_COLS[0] if COND_TS_FEATURE_COLS else None
        if featured is None:
            st.info("featured_data.pkl がありません。先に取込/学習を実行してください。")
        elif cond_primary not in featured.columns:
            st.warning(
                "featured_data に条件別 TrueSkill 列がありません。最新コードで "
                "ingest/retrain を実行して特徴量を再生成してください。"
            )
        else:
            race_ids = sorted(featured.index.astype(str).unique().tolist(), reverse=True)
            race_id = st.selectbox("レースを選択（race_id）", race_ids, key="race_cond")
            race_df = featured.loc[[race_id]] if race_id in featured.index else featured.loc[
                featured.index.astype(str) == race_id
            ]
            cols = [c for c in [ResultsCols.UMABAN, "horse_id", *COND_TS_FEATURE_COLS]
                    if c in race_df.columns]
            view = race_df[cols].copy()
            sort_key = f"ts_{COND_DIMENSIONS[0]}_conservative"
            if sort_key in view.columns:
                view = view.sort_values(sort_key, ascending=False)
            num_cols = [c for c in view.columns if c not in (ResultsCols.UMABAN, "horse_id")]
            st.dataframe(
                view.style.format({c: "{:+.2f}" if c.endswith("vs_field") else "{:.2f}"
                                   for c in num_cols}),
                use_container_width=True,
                hide_index=True,
            )
    else:
        fam = _FAMILIES[family]
        snapshot = _load_snapshot(fam["path"])

        st.subheader(f"最新スナップショット — {family}")
        if not snapshot:
            st.info(
                f"{family} のスナップショット（`{os.path.basename(fam['path'])}`）がありません。"
                "`run_pipeline ingest` または `retrain` を実行すると生成されます。"
            )
        else:
            snap_df = (
                pd.DataFrame.from_dict(snapshot, orient="index")
                .rename_axis("horse_id")
                .reset_index()
            )
            sort_col = fam["snap_sort"] if fam["snap_sort"] in snap_df.columns else snap_df.columns[1]
            snap_df = snap_df.sort_values(sort_col, ascending=False).reset_index(drop=True)
            c1, c2, c3 = st.columns(3)
            c1.metric("登録頭数", f"{len(snap_df):,}")
            c2.metric(f"最高 {sort_col}", f"{snap_df[sort_col].max():.2f}")
            c3.metric(f"中央値 {sort_col}", f"{snap_df[sort_col].median():.2f}")
            top_n = st.slider("表示件数（上位）", 10, 200, 50, step=10)
            st.dataframe(snap_df.head(top_n), use_container_width=True, hide_index=True)

        st.divider()
        st.subheader(f"レース内 {family}・式勝率（即時・再学習不要）")
        primary = fam["primary"]
        if featured is None:
            st.info("featured_data.pkl がありません。先に取込/学習を実行してください。")
        elif primary not in featured.columns:
            st.warning(
                f"featured_data に {primary} 列がありません。{family} 列を含めて特徴量を再生成"
                "してください（最新コードで ingest/retrain を実行）。"
            )
        else:
            race_ids = sorted(featured.index.astype(str).unique().tolist(), reverse=True)
            race_id = st.selectbox("レースを選択（race_id）", race_ids, key=f"race_{family}")
            race_df = featured.loc[[race_id]] if race_id in featured.index else featured.loc[
                featured.index.astype(str) == race_id
            ]
            cols = [c for c in [ResultsCols.UMABAN, "horse_id", *fam["feature_cols"]]
                    if c in race_df.columns]
            view = race_df[cols].copy()
            if family == "Elo":
                view["win_prob"] = elo_win_probabilities(view["elo_rating"].to_numpy(dtype=float))
                st.caption("win_prob は p_i ∝ 10^(rating/400) の Elo 式勝率（モデル予測とは独立）。")
            else:
                view["win_prob"] = trueskill_win_probabilities(
                    view["ts_mu"].to_numpy(dtype=float), view["ts_sigma"].to_numpy(dtype=float)
                )
                st.caption(
                    "win_prob は μ/σ から算出した TrueSkill 近似勝率（Thurstone 型 softmax、"
                    "モデル予測とは独立）。"
                )
            view = view.sort_values("win_prob", ascending=False)
            num_cols = [c for c in view.columns if c not in (ResultsCols.UMABAN, "horse_id")]
            fmt = {c: "{:.2f}" for c in num_cols}
            fmt["win_prob"] = "{:.1%}"
            st.dataframe(view.style.format(fmt), use_container_width=True, hide_index=True)

# ==================================================================
# タブ2: On/Off A/B（レーティング有無 2 モデルの比較）
# ==================================================================
with tab2:
    st.subheader("レーティング特徴量 On/Off の効果比較")
    st.caption(
        "レーティングを含むモデルと含まないモデル（`retrain --no-rating-features` で生成）を"
        "選び、同一テスト区間で ROI・AUC・logloss・キャリブレーションを比較します。"
    )

    paths = find_model_paths()
    if not paths:
        st.info("モデルがありません。先に `run_pipeline retrain` を実行してください。")
        st.stop()

    # version 名 → パス対応（version_history と pickle パスの突合は名前一致で行う）
    label_to_path: dict[str, str] = {}
    for p in paths:
        # models/<version>/<version>.pickle 形式を想定し、親ディレクトリ名をラベルにする
        label = os.path.basename(os.path.dirname(p)) or os.path.basename(p)
        label_to_path[label] = p

    chosen = st.multiselect(
        "比較するモデル（推奨: rating あり / なし、または Elo のみ / TrueSkill のみ）",
        options=list(label_to_path.keys()),
        default=list(label_to_path.keys())[:2],
        max_selections=4,
    )
    ev_threshold = st.slider("EV 閾値（単勝バックテスト）", 1.0, 3.0, 1.5, step=0.1)

    def _rating_families(model) -> str:
        """モデルの feature_names_ から含まれるレーティングファミリーを判定する。"""
        names = set(getattr(model, "feature_names_", None) or [])
        if not names:
            return "不明"
        tags = []
        if any(c in names for c in ELO_FEATURE_COLS):
            tags.append("Elo")
        if any(c in names for c in TS_FEATURE_COLS):
            tags.append("TS")
        if any(c in names for c in COND_TS_FEATURE_COLS):
            tags.append("Cond")
        return "+".join(tags) if tags else "なし"

    if featured is None:
        st.info("featured_data.pkl がないため A/B バックテストを実行できません。")
    elif len(chosen) < 1:
        st.info("モデルを 1 つ以上選択してください。")
    elif st.button("A/B 比較を実行", type="primary"):
        rows = []
        profit_curves: dict[str, pd.Series] = {}
        calib_data: dict[str, dict] = {}
        for label in chosen:
            with st.spinner(f"{label} を評価中…"):
                model = _load_model(label_to_path[label])
                families = _rating_families(model)
                bt = compute_full_backtest(model, featured, ev_threshold=ev_threshold)
                summary = bt["summary"]
                per_race = bt["per_race"]
                if "cumulative_profit" in per_race.columns:
                    profit_curves[label] = per_race["cumulative_profit"].reset_index(drop=True)

                # AUC / logloss（スタッキング meta 確率から算出。取得できない場合は NaN）
                auc = logloss = float("nan")
                sa = compute_stacking_auc(model, featured)
                if sa is not None:
                    from sklearn.metrics import log_loss, roc_auc_score

                    y = sa["y_true"]
                    p = np.clip(sa["meta_probs"], 1e-6, 1 - 1e-6)
                    try:
                        auc = float(roc_auc_score(y, p))
                        logloss = float(log_loss(y, p))
                    except ValueError:
                        pass

                calib = compute_calib_curves(model, featured)
                if calib is not None:
                    calib_data[label] = calib

                rows.append(
                    {
                        "モデル": label,
                        "rating": families,
                        "回収率": summary.get("return_rate"),
                        "的中率": summary.get("hit_rate"),
                        "AUC": auc,
                        "logloss": logloss,
                        "シャープ": summary.get("sharpe_ratio"),
                        "最大DD": summary.get("max_drawdown"),
                        "損益": summary.get("profit"),
                        "賭け数": summary.get("n_bets"),
                    }
                )

        comp = pd.DataFrame(rows)
        st.dataframe(
            comp.style.format(
                {
                    "回収率": "{:.1%}", "的中率": "{:.1%}", "AUC": "{:.4f}",
                    "logloss": "{:.4f}", "シャープ": "{:.3f}", "最大DD": "{:.0f}",
                    "損益": "{:,.0f}",
                },
                na_rep="—",
            ),
            use_container_width=True,
            hide_index=True,
        )

        if profit_curves:
            st.subheader("資金推移（テスト区間）")
            curve_df = pd.DataFrame(profit_curves)
            st.line_chart(curve_df)

        if len(calib_data) >= 1:
            st.subheader("キャリブレーション（較正後）")
            calib_rows = []
            for label, cd in calib_data.items():
                y_true = np.asarray(cd["y_true"])
                prob = np.asarray(cd["prob_post"])
                bins = np.linspace(0, 1, 11)
                idx = np.clip(np.digitize(prob, bins) - 1, 0, 9)
                for b in range(10):
                    mask = idx == b
                    if mask.sum() == 0:
                        continue
                    calib_rows.append(
                        {"モデル": label, "予測確率": prob[mask].mean(),
                         "実測勝率": y_true[mask].mean()}
                    )
            if calib_rows:
                cal_df = pd.DataFrame(calib_rows)
                pivot = cal_df.pivot_table(
                    index="予測確率", columns="モデル", values="実測勝率"
                )
                st.line_chart(pivot)
                st.caption("対角線に近いほど較正が良い（予測確率 = 実測勝率）。")
