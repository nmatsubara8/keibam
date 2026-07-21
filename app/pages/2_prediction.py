"""予測・EV・推奨馬券ページ。

較正勝率 × 予測オッズ → EV → フラクショナル・ケリー推奨額 を表示し、
advisory モードでは「承認して記録」ボタンで投票履歴に保存する。
"""

import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import os

import pandas as pd
import streamlit as st

from app._betting_history import DEFAULT_HISTORY_PATH
from app._betting_history import append_history
from app._data_loader import find_model_paths
from app._data_loader import list_model_versions
from app._data_loader import load_model_from_path
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
def _load_model(path: str):
    """選択された .pickle パスのモデルを読み込む（path をキーにキャッシュ）。"""
    return load_model_from_path(path)


@st.cache_data(show_spinner=False)
def _load_config():
    return load_operation_config()


@st.cache_resource(show_spinner="特徴量データを読み込み中…")
def _load_featured(path: str):
    """特徴量 pickle を1回だけ読み込み再利用する。

    毎リラン（selectbox 変更等）で 800MB 超の pickle を読み直すと
    メモリを使い果たして OOM (Killed) するため、cache_resource で
    プロセス内に1コピーだけ保持する。
    """
    return pd.read_pickle(path)


op_config = _load_config()

# ------------------------------------------------------------------
# 適用モデルの選択（既定=最新。再学習で蓄積した複数バージョンから選べる）
# ------------------------------------------------------------------
model_paths = find_model_paths()  # 新しい順
if not model_paths:
    st.error("モデルが見つかりません。先に `run_pipeline.py --job retrain` を実行してください。")
    st.stop()

_meta_by_version = {m["version"]: m for m in list_model_versions()}


def _model_label(path: str) -> str:
    version = os.path.basename(path).replace(".pickle", "")
    meta = _meta_by_version.get(version)
    if meta and meta.get("auc_test") is not None:
        return f"{version}（AUC {meta['auc_test']:.4f}）"
    return version


st.sidebar.subheader("適用モデル")
sel_path = st.sidebar.selectbox(
    "モデルバージョン",
    model_paths,
    index=0,  # 既定は最新
    format_func=_model_label,
    help="再学習で蓄積された複数バージョンから予測に使うモデルを選択（既定=最新）",
)
st.sidebar.caption(f"適用中: `{_model_label(sel_path)}`")

# 6 分割（全国/地方 × 芝/ダート/障害）のカテゴリ別 Place ヘッドを、選択レースに応じて自動選択する。
_auto_cat = st.sidebar.checkbox(
    "レース種別で自動選択（推奨）",
    value=True,
    help="全国/地方 × 芝/ダート/障害 の 6 分割から、選択レースに対応する Place ヘッドを自動で使う"
    "（該当が無ければ統合モデルにフォールバック）",
)

# ------------------------------------------------------------------
# レース選択
# ------------------------------------------------------------------
st.subheader("レース選択")

featured_path = LocalPaths.FEATURED_DATA_PATH
try:
    featured_df = _load_featured(featured_path)
    available_races = sorted(featured_df.index.unique().tolist(), reverse=True)
except FileNotFoundError:
    st.error("特徴量データが見つかりません。先に ingestion を実行してください。")
    st.stop()

race_id = st.selectbox("race_id", available_races, index=0)

if not race_id:
    st.stop()

X = featured_df.loc[[race_id]]

# ------------------------------------------------------------------
# 選択レースのカテゴリに応じて Place ヘッドを解決（統合フォールバック付き）
# ------------------------------------------------------------------
from app._data_loader import resolve_place_model_path_for_race
from src.constants._model_category import CATEGORY_LABELS
from src.constants._model_category import categorize
from src.training._category_split import recover_race_type

_rt_series = recover_race_type(X).dropna()
_race_type = _rt_series.iloc[0] if not _rt_series.empty else None
_race_category = categorize(race_id, _race_type)

if _auto_cat:
    eff_path, used_category = resolve_place_model_path_for_race(sel_path, race_id, _race_type)
else:
    eff_path, used_category = sel_path, (_race_category or "combined")

_target_label = CATEGORY_LABELS.get(_race_category, "分類不能") if _race_category else "分類不能"
st.sidebar.caption(f"レース種別: {_target_label} / 使用ヘッド: {CATEGORY_LABELS.get(used_category, used_category)}")
if _auto_cat and _race_category is not None and used_category != _race_category:
    st.sidebar.info(f"「{_target_label}」専用 Place ヘッドが無いため統合モデルで予測します。")

model = _load_model(eff_path)

if model is None:
    st.error("モデルの読み込みに失敗しました。")
    st.stop()

# ------------------------------------------------------------------
# 予測実行
# ------------------------------------------------------------------
with st.spinner("予測中…"):
    try:
        # モデルラボで保存した券種別最適化パラメータがあれば EV 選定に反映する。
        from src.policies._bet_type_params import bet_type_params_path
        from src.policies._bet_type_params import latest_bet_type_params

        bt_params = latest_bet_type_params(bet_type_params_path("models")) or None
        # 較正済み控除率（calibrate-takeout の出力）があれば連系推定オッズに反映される。
        from src.policies._takeout_calibration import latest_takeout_map
        from src.policies._takeout_calibration import takeout_calibration_path

        _calib_takeout = latest_takeout_map(takeout_calibration_path("models"))
        if _calib_takeout:
            st.caption(
                "🎯 較正済み控除率を連系推定オッズに適用中: "
                + ", ".join(f"{bt}={t:.3f}" for bt, t in _calib_takeout.items())
            )
        candidates = run_prediction(
            model.effective_model, X, op_config, bet_type_params=bt_params
        )
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
# 発注カートへ追加（発注ページ 🛒 で金額編集・発注票出力・清算ができる）
# ------------------------------------------------------------------
st.divider()
from app._order_service import add_orders
from app._order_service import candidates_to_orders
from app._order_service import load_basket
from app._order_service import save_basket

if st.button("🛒 発注カートへ追加"):
    new_orders = candidates_to_orders(candidates)
    basket = add_orders(load_basket(), new_orders)
    save_basket(basket)
    st.success(f"{len(new_orders)} 件をカートに追加しました（発注ページで確認・編集できます）。")

# ------------------------------------------------------------------
# 損失ストップ（kill switch）: 当日実現損失が上限超なら記録/発注を停止
# ------------------------------------------------------------------
from app._betting_history import load_history
from src.operation._risk_guard import evaluate_kill_switch

_guard = evaluate_kill_switch(load_history(DEFAULT_HISTORY_PATH), op_config)
if _guard.blocked:
    st.error(
        f"🛑 取引停止中: {_guard.reason}"
        f"（当日損失 ¥{_guard.daily_loss:,.0f} / 上限 ¥{_guard.limit:,.0f}）。"
        "本日の記録・発注は停止されています。"
    )
    st.stop()

# ------------------------------------------------------------------
# Advisory モード: 承認ボタン
# ------------------------------------------------------------------
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
