"""レーティングラボ — ペアワイズ Elo レーティングの効果を目視で照会・比較する。

Phase 1（ペアワイズ Elo + 着差補正）の効果検証 UI。設計方針:
- タブ1「レーティング照会」: 再学習不要。各レースの出走前 Elo・Elo式勝率・実着順を並置し、
  最新スナップショットの上位馬や較正・本命的中・順位相関を即座に確認できる。
- タブ2「On/Off A/B」: --no-rating-features 有無で学習した 2 モデルを選び、ROI/logloss/AUC を
  比較する手順を案内（モデルラボの比較基盤を再利用）。
"""

import sys
from pathlib import Path

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import json
import os

import pandas as pd
import streamlit as st

from app._data_loader import list_model_versions
from app._model_eval import load_featured_data
from app import _rating_eval as RE
from src.constants._local_paths import LocalPaths

st.set_page_config(page_title="レーティングラボ — KeibaAM", page_icon="📊", layout="wide")
st.title("📊 レーティングラボ（ペアワイズ Elo）")
st.caption(
    "馬の地力を着順履歴から推定した Elo レーティングの効果を照会します。"
    "タブ1は再学習不要の即時照会、タブ2は学習済みモデルの On/Off A/B です。"
)


@st.cache_data(show_spinner=False)
def _load_featured():
    return load_featured_data()


@st.cache_data(show_spinner=False)
def _load_snapshot():
    path = LocalPaths.HORSE_RATINGS_PATH
    if not os.path.exists(path):
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


tab1, tab2 = st.tabs(["🔍 レーティング照会（再学習不要）", "🆚 On/Off A/B（学習済みモデル）"])

# ---------------------------------------------------------------------------
# タブ1: レーティング照会
# ---------------------------------------------------------------------------
with tab1:
    df = _load_featured()
    if df is None:
        st.warning("featured_data.pkl がありません。ingest / rebuild-featured を実行してください。")
    elif not RE.has_ratings(df):
        st.warning(
            "featured_data に Elo 列（elo_rating 等）がありません。"
            "`run_pipeline rebuild-featured` で Elo 特徴を含めて再生成してください。"
        )
    else:
        snapshot = _load_snapshot()

        st.subheader("効果サマリ（再学習なし・素の Elo の説明力）")
        hits = RE.top_pick_hit_rates(df)
        rho = RE.rank_correlation(df)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Elo 本命の1着的中率", f"{hits['elo_rate']*100:.1f}%" if hits["n_races"] else "—",
                  help="各レースで Elo 最上位の馬が1着だった割合")
        c2.metric("市場本命（単勝最小）的中率", f"{hits['fav_rate']*100:.1f}%" if hits["n_races"] else "—",
                  help="比較基準: 各レースで単勝オッズ最小の馬が1着だった割合")
        c3.metric("順位相関 ρ(Elo,着順)", f"{rho:.3f}" if rho == rho else "—",
                  help="負ほど良い（高レーティングほど好走）")
        c4.metric("対象レース数", f"{hits['n_races']:,}")

        st.divider()
        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**較正曲線（Elo式勝率 vs 実勝率）**")
            calib = RE.standalone_calibration(df)
            if not calib.empty:
                st.scatter_chart(calib, x="mean_pred", y="mean_actual")
                st.caption("対角線に近いほど較正が良い。点の大きさは無視（count 列参照）。")
                with st.expander("較正テーブル"):
                    st.dataframe(calib, use_container_width=True)
            else:
                st.info("較正に必要な列（elo_win_prob / rank_win）が不足しています。")
        with col_r:
            st.markdown("**最新スナップショット 上位馬**")
            min_races = st.slider("最低出走数", 1, 20, 3)
            rank_tbl = RE.snapshot_ranking(snapshot, top=30, min_races=min_races)
            if rank_tbl.empty:
                st.info("スナップショット（horse_ratings.json）がまだありません。")
            else:
                st.dataframe(rank_tbl, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("レース別照会")
        # race_id をインデックスから取得
        rid_index = df.index.astype(str)
        race_ids = pd.Series(rid_index.unique())
        sel = st.selectbox("race_id を選択", options=race_ids.tolist()[:5000])
        if sel:
            race = df[rid_index == sel].copy()
            show_cols = [c for c in ["馬番", "elo_rating", "elo_n_races", "elo_vs_field",
                                     "elo_win_prob", RE.TANSHO, RE.RANK] if c in race.columns]
            race_view = race[show_cols].sort_values("elo_rating", ascending=False)
            st.dataframe(race_view, use_container_width=True)
            st.caption("Elo 降順に並べ、elo_win_prob（Elo式勝率）・実際の単勝/着順と見比べられます。")

# ---------------------------------------------------------------------------
# タブ2: On/Off A/B
# ---------------------------------------------------------------------------
with tab2:
    st.subheader("レーティング特徴の On/Off A/B")
    st.markdown(
        "学習は `X_train.values`（位置ベース）で行うため、特徴のライブ Off は不可です。"
        "効果を測るには **2 つのモデルを学習** して比較します:\n\n"
        "```\n"
        "# Elo あり（通常）\n"
        "python -m src.pipeline.run_pipeline retrain --version-name with_rating\n"
        "# Elo なし\n"
        "python -m src.pipeline.run_pipeline retrain --no-rating-features --version-name no_rating\n"
        "```\n"
        "その後 backtest で比較します（`--no-rating-features` 側は列一致のため同フラグ必須）:\n\n"
        "```\n"
        "python -m src.pipeline.run_pipeline backtest --version with_rating --years 2023 --edge-diagnostic\n"
        "python -m src.pipeline.run_pipeline backtest --version no_rating "
        "--no-rating-features --years 2023 --edge-diagnostic\n"
        "```"
    )
    versions = [v.get("version") for v in list_model_versions()]
    if versions:
        st.markdown("**学習済みバージョン**（モデルラボ 🆚 タブで ROI/logloss/AUC を並置比較できます）")
        st.write(", ".join(str(v) for v in versions[:20]))
    else:
        st.info("学習済みモデルがまだありません。上のコマンドで with_rating / no_rating を学習してください。")
    st.caption(
        "詳細な ROI・キャリブレーション・スタッキング AUC の左右並置比較は "
        "『🧪 モデルラボ』の「モデル比較シミュレーション」タブを使ってください（同一基盤を再利用）。"
    )
