"""発注ページ — 馬券購入の発注カート・発注票出力・清算。

- 予測ページの推奨や手動追加をカートに集約し、金額（100 円単位）を編集
- 資金上限（bankroll × max_daily_ratio）をチェック
- 運用モード別の発注実行:
    advisory  — 推奨として記録のみ
    semi_auto — IPAT 入力支援テキスト + CSV を出力し「発注済み」を記録
    full_auto — 既定無効（規約・法的リスク）
- 結果確定後の清算（払戻計算 → 履歴更新 → 回収率）
"""

import sys
from pathlib import Path

# リポジトリルートを import パスに追加（ページ直接起動でも `app`/`src` を解決できるように）。
_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

import pandas as pd
import streamlit as st

from app._betting_history import DEFAULT_HISTORY_PATH
from app._betting_history import append_history
from app._betting_history import history_to_dataframe
from app._betting_history import load_history
from app._data_loader import load_operation_config
from app._order_service import BET_TYPE_LABELS
from app._order_service import LABEL_TO_BET_TYPE
from app._order_service import add_orders
from app._order_service import basket_to_csv_bytes
from app._order_service import basket_to_frame
from app._order_service import basket_total
from app._order_service import clear_basket
from app._order_service import exceeds_daily_cap
from app._order_service import format_ipat_text
from app._order_service import load_basket
from app._order_service import orders_to_history_records
from app._order_service import race_label
from app._order_service import rewrite_history
from app._order_service import round_stake
from app._order_service import save_basket
from app._order_service import settle_records
from src.operation._config import ADVISORY
from src.operation._config import FULL_AUTO
from src.operation._config import SEMI_AUTO

st.set_page_config(page_title="発注 — KeibaAM", page_icon="🛒", layout="wide")
st.title("🛒 馬券発注")

op_config = load_operation_config()
basket = load_basket()

# ------------------------------------------------------------------
# 資金サマリ
# ------------------------------------------------------------------
total = basket_total(basket)
over_cap, cap = exceeds_daily_cap(total, op_config)
col1, col2, col3, col4 = st.columns(4)
col1.metric("運用モード", op_config.operation_mode)
col2.metric("bankroll", f"¥{int(op_config.bankroll):,}")
col3.metric("カート合計", f"¥{total:,}")
col4.metric("当日上限", f"¥{int(cap):,}")
if over_cap:
    st.error(
        f"カート合計が当日上限（bankroll × {op_config.max_daily_ratio:.0%} = "
        f"¥{int(cap):,}）を超えています。金額を調整してください。"
    )

st.divider()

# ------------------------------------------------------------------
# ① カートへの追加
# ------------------------------------------------------------------
st.subheader("① カートへ追加")
st.caption("予測ページ（🎯 予測・推奨）の「🛒 発注カートへ追加」ボタン、または下の手動追加でカートに入ります。")

with st.expander("✍️ 手動追加", expanded=not basket):
    with st.form("manual_add"):
        c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
        with c1:
            manual_race_id = st.text_input("race_id（12 桁）", placeholder="202605030211")
        with c2:
            manual_label = st.selectbox("式別", list(BET_TYPE_LABELS.values()))
        with c3:
            manual_combo = st.text_input("買い目（- 区切り）", placeholder="7 または 3-5")
        with c4:
            manual_stake = st.number_input("金額（円）", min_value=100, step=100, value=100)
        submitted = st.form_submit_button("カートへ追加")
    if submitted:
        try:
            combo = [int(x) for x in manual_combo.replace("→", "-").split("-") if x.strip()]
            if not combo:
                raise ValueError("買い目が空です")
            order = {
                "order_id": f"manual{len(basket)}",
                "race_id": manual_race_id.strip(),
                "bet_type": LABEL_TO_BET_TYPE[manual_label],
                "combo": combo,
                "odds": 0.0,
                "probability": 0.0,
                "expected_value": 0.0,
                "stake": round_stake(manual_stake),
                "added_at": pd.Timestamp.now().isoformat(),
            }
            basket = add_orders(basket, [order])
            save_basket(basket)
            st.success(f"追加しました: {race_label(order['race_id'])} {manual_label} {manual_combo}")
            st.rerun()
        except Exception as e:  # noqa: BLE001
            st.error(f"追加できませんでした: {e}")

# ------------------------------------------------------------------
# ② カート編集
# ------------------------------------------------------------------
st.subheader("② 発注カート")
if not basket:
    st.info("カートは空です。予測ページから推奨を追加するか、手動追加してください。")
else:
    frame = basket_to_frame(basket)
    frame.insert(0, "発注", True)
    edited = st.data_editor(
        frame,
        column_config={
            "発注": st.column_config.CheckboxColumn("発注", help="チェックを外した行は発注対象から除外"),
            "金額": st.column_config.NumberColumn("金額", min_value=100, step=100, format="¥%d"),
            "order_id": None,  # 非表示
        },
        disabled=["レース", "式別", "買い目", "オッズ", "EV"],
        hide_index=True,
        use_container_width=True,
        key="basket_editor",
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        if st.button("💾 編集を保存（金額・選択を反映）"):
            kept_ids = set(edited[edited["発注"]]["order_id"])
            stake_by_id = dict(zip(edited["order_id"], edited["金額"], strict=False))
            updated = []
            for o in basket:
                if o["order_id"] not in kept_ids:
                    continue
                o = dict(o)
                o["stake"] = round_stake(float(stake_by_id.get(o["order_id"], o["stake"])))
                updated.append(o)
            save_basket(updated)
            st.success(f"{len(updated)} 件を保存しました。")
            st.rerun()
    with col_b:
        if st.button("🗑️ カートを空にする"):
            clear_basket()
            st.rerun()

# ------------------------------------------------------------------
# ③ 発注実行（運用モード別）
# ------------------------------------------------------------------
st.divider()
st.subheader("③ 発注実行")

if not basket:
    st.caption("カートに馬券を追加すると発注できます。")
elif over_cap:
    st.warning("当日上限を超えているため発注できません。②で金額を調整してください。")
else:
    mode = op_config.operation_mode
    if mode == ADVISORY:
        st.caption("advisory モード: 推奨として履歴に記録します。実際の購入は IPAT 等で人間が行ってください。")
        if st.button("✅ 推奨として記録", type="primary"):
            records = orders_to_history_records(basket, status="recommended")
            for r in records:
                append_history(r, DEFAULT_HISTORY_PATH)
            clear_basket()
            st.success(f"{len(records)} 件を投票履歴に記録しました。")
            st.rerun()

    elif mode == SEMI_AUTO:
        st.caption(
            "semi_auto モード: IPAT 入力支援テキストと CSV を出力します。"
            "IPAT で投票後に「発注済みとして記録」を押してください。"
        )
        ipat_text = format_ipat_text(basket)
        st.code(ipat_text, language=None)
        col_x, col_y, col_z = st.columns(3)
        with col_x:
            st.download_button(
                "📥 発注票 CSV",
                basket_to_csv_bytes(basket),
                file_name=f"orders_{pd.Timestamp.now():%Y%m%d_%H%M}.csv",
                mime="text/csv",
            )
        with col_y:
            st.download_button(
                "📥 IPAT テキスト",
                ipat_text.encode("utf-8"),
                file_name=f"ipat_{pd.Timestamp.now():%Y%m%d_%H%M}.txt",
                mime="text/plain",
            )
        with col_z:
            if st.button("🎫 発注済みとして記録", type="primary"):
                records = orders_to_history_records(basket, status="placed")
                for r in records:
                    append_history(r, DEFAULT_HISTORY_PATH)
                clear_basket()
                st.success(f"{len(records)} 件を発注済みとして記録しました。")
                st.rerun()

    elif mode == FULL_AUTO:
        st.error(
            "full_auto（自動発注）は規約・法的リスクのため既定で無効です。"
            "config.yaml の operation_mode を advisory / semi_auto にしてください。"
        )

# ------------------------------------------------------------------
# ④ 発注済み・清算
# ------------------------------------------------------------------
st.divider()
st.subheader("④ 発注済み・清算")

history = load_history(DEFAULT_HISTORY_PATH)
open_records = [
    r for r in history
    if r.get("payout") is None and r.get("status") in ("placed", "queued", "recommended")
]
settled_records = [r for r in history if r.get("status") == "settled"]

col_l, col_r = st.columns([3, 1])
with col_l:
    if open_records:
        st.markdown(f"**未清算 {len(open_records)} 件**（結果取得後に清算できます）")
        st.dataframe(history_to_dataframe(open_records), use_container_width=True, hide_index=True)
    else:
        st.caption("未清算の発注はありません。")
with col_r:
    if open_records and st.button("🧾 結果で清算する"):
        try:
            from src.constants._local_paths import LocalPaths
            from src.preprocessing._return_processor import ReturnProcessor
            from src.simulation._betting_tickets import BettingTickets

            tickets = BettingTickets(ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH))
            updated, n_settled = settle_records(history, tickets)
            rewrite_history(updated, DEFAULT_HISTORY_PATH)
            if n_settled:
                st.success(f"{n_settled} 件を清算しました。")
                st.rerun()
            else:
                st.info("清算できる結果がまだありません（ingest 後に再実行してください）。")
        except Exception as e:  # noqa: BLE001
            st.error(f"清算エラー: {e}")

if settled_records:
    st.markdown("**清算済み**")
    df = history_to_dataframe(settled_records)
    total_stake = sum(float(r.get("stake", 0)) for r in settled_records)
    total_payout = sum(float(r.get("payout", 0)) for r in settled_records)
    n_hits = sum(1 for r in settled_records if r.get("hit"))
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("総購入額", f"¥{int(total_stake):,}")
    m2.metric("総払戻", f"¥{int(total_payout):,}")
    m3.metric("回収率", f"{total_payout / total_stake * 100:.1f}%" if total_stake else "—")
    m4.metric("的中", f"{n_hits}/{len(settled_records)}")
    st.dataframe(df, use_container_width=True, hide_index=True)
