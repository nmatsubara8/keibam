"""KeibaAM Streamlit アプリ — エントリポイント。

起動:
    streamlit run app/Home.py

マルチページ構成。左サイドバーの自動ナビゲーション（pages/ ディレクトリから生成）で
各画面へ遷移する。
"""

import streamlit as st

st.set_page_config(
    page_title="KeibaAM",
    page_icon="🏇",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------
# サイドバー: システム状態バッジ
# ------------------------------------------------------------------
from app._data_loader import load_system_status  # noqa: E402

try:
    status = load_system_status()
    with st.sidebar:
        st.title("🏇 KeibaAM")
        st.caption(f"モード: **{status['operation_mode']}**")
        if status["model_version"]:
            st.caption(f"モデル: `{status['model_version']}`")
            if status["model_auc"]:
                st.caption(f"AUC(test): **{status['model_auc']:.4f}**")
        else:
            st.warning("モデル未読込")
        st.divider()
        st.caption(f"スナップショット: {status['n_snapshots']:,} 件")
        if status["last_ingest"]:
            st.caption(f"最終取込: {status['last_ingest']}")
except Exception as e:
    with st.sidebar:
        st.error(f"状態取得エラー: {e}")

# ------------------------------------------------------------------
# ホーム画面
# ------------------------------------------------------------------
st.title("🏇 KeibaAM — 競馬予想AIシステム")
st.markdown(
    """
左のサイドバーから各ページへ移動してください。

| ページ | 内容 |
|---|---|
| 📊 ダッシュボード | 本日開催・データ収集状況・次締切カウントダウン |
| 🎯 予測・推奨 | 較正勝率 × EV × フラクショナル・ケリー推奨 |
| 📈 オッズ推移 | 段階スナップショットによるオッズ変動モニタ |
| 🏆 成績・設定 | 回収率・AUC推移・投票履歴・モデル管理 |
"""
)
