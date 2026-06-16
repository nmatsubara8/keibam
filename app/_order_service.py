"""発注 UI のサービスロジック（Streamlit 非依存・テスト可能）。

発注カート（data/order_basket.json）の管理、IPAT 入力支援テキスト・CSV の生成、
資金上限チェック、発注記録、結果確定後の清算（払戻計算）を担う。

実際の投票は IPAT 等で人間が行う（semi_auto まで）。full_auto は規約・法的
リスクのため bet_executor 側で既定無効。
"""

from __future__ import annotations

import datetime as dt
import json
import os
import uuid

import pandas as pd

from src.constants._bet_types import BetType
from src.constants._master import Master

DEFAULT_BASKET_PATH = "data/order_basket.json"

# 馬券種 → 日本語表示
BET_TYPE_LABELS = {
    BetType.TANSHO: "単勝",
    BetType.FUKUSHO: "複勝",
    BetType.WAKUREN: "枠連",
    BetType.UMAREN: "馬連",
    BetType.UMATAN: "馬単",
    BetType.WIDE: "ワイド",
    BetType.SANRENPUKU: "三連複",
    BetType.SANRENTAN: "三連単",
}
LABEL_TO_BET_TYPE = {v: k for k, v in BET_TYPE_LABELS.items()}

# 順序を区別する馬券（→ 区切り）。それ以外は - 区切り
_ARROW_TYPES = {BetType.UMATAN, BetType.SANRENTAN}

# place_id（2 桁コード）→ 場名
_PLACE_NAMES = {code: name for name, code in Master.PLACE_DICT.items()}


def race_label(race_id) -> str:
    """race_id（YYYYPPKKDDRR）→ 「東京 11R (202605030211)」形式の表示名。"""
    rid = str(race_id)
    if len(rid) != 12 or not rid.isdigit():
        return rid
    place = _PLACE_NAMES.get(rid[4:6], rid[4:6])
    race_no = int(rid[10:12])
    return f"{place} {race_no}R ({rid})"


def combo_label(bet_type: str, combo) -> str:
    """買い目の表示（順序付きは →、順不同は -）。"""
    sep = "→" if bet_type in _ARROW_TYPES else "-"
    return sep.join(str(int(c)) for c in combo)


def round_stake(stake: float, unit: int = 100) -> int:
    """掛け金を購入単位（100 円）に丸める。正の掛け金は最低 1 単位を保証する。"""
    if stake <= 0:
        return 0
    return max(unit, int(round(stake / unit)) * unit)


# ---------------------------------------------------------------------------
# 発注カート
# ---------------------------------------------------------------------------


def candidates_to_orders(candidates) -> list[dict]:
    """BetCandidate のリストを発注カート行へ変換する（stake は 100 円単位に丸め）。"""
    orders = []
    for c in candidates:
        stake = round_stake(c.stake)
        if stake <= 0:
            continue
        orders.append(
            {
                "order_id": uuid.uuid4().hex[:12],
                "race_id": str(c.race_id),
                "bet_type": c.bet_type,
                "combo": [int(x) for x in c.combo],
                "odds": float(c.odds),
                "probability": float(c.probability),
                "expected_value": float(c.expected_value),
                "stake": stake,
                "added_at": dt.datetime.now().isoformat(),
            }
        )
    return orders


def order_key(order: dict) -> tuple:
    """同一馬券の判定キー（race_id, bet_type, combo）。"""
    return (str(order["race_id"]), order["bet_type"], tuple(order["combo"]))


def add_orders(basket: list[dict], new_orders: list[dict]) -> list[dict]:
    """カートに追加する。同一馬券（race/式別/買い目）は新しい方で置き換える。"""
    merged = {order_key(o): o for o in basket}
    for o in new_orders:
        merged[order_key(o)] = o
    return list(merged.values())


def load_basket(path: str = DEFAULT_BASKET_PATH) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_basket(basket: list[dict], path: str = DEFAULT_BASKET_PATH) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(basket, f, ensure_ascii=False, indent=2)


def clear_basket(path: str = DEFAULT_BASKET_PATH) -> None:
    if os.path.exists(path):
        os.remove(path)


def basket_total(basket: list[dict]) -> int:
    return int(sum(o.get("stake", 0) for o in basket))


def exceeds_daily_cap(total: int, op_config) -> tuple[bool, float]:
    """当日上限（bankroll × max_daily_ratio）を超えるか。(超過フラグ, 上限額) を返す。"""
    cap = float(op_config.bankroll) * float(op_config.max_daily_ratio)
    return total > cap, cap


# ---------------------------------------------------------------------------
# 発注票の出力（IPAT 入力支援・CSV）
# ---------------------------------------------------------------------------


def format_ipat_text(basket: list[dict]) -> str:
    """IPAT（即PAT）入力支援テキストを生成する（レース別にグループ化）。"""
    if not basket:
        return ""
    lines: list[str] = []
    df = pd.DataFrame(basket)
    for race_id, grp in df.groupby("race_id"):
        lines.append(f"【{race_label(race_id)}】")
        for _, row in grp.iterrows():
            label = BET_TYPE_LABELS.get(row["bet_type"], row["bet_type"])
            combo = combo_label(row["bet_type"], row["combo"])
            lines.append(f"  {label} {combo} — {int(row['stake']):,}円")
        subtotal = int(grp["stake"].sum())
        lines.append(f"  小計 {subtotal:,}円")
        lines.append("")
    lines.append(f"合計 {basket_total(basket):,}円")
    return "\n".join(lines)


def basket_to_frame(basket: list[dict]) -> pd.DataFrame:
    """カートを表示・編集用の DataFrame に変換する。"""
    if not basket:
        return pd.DataFrame(
            columns=["order_id", "レース", "式別", "買い目", "オッズ", "EV", "金額"]
        )
    rows = [
        {
            "order_id": o["order_id"],
            "レース": race_label(o["race_id"]),
            "式別": BET_TYPE_LABELS.get(o["bet_type"], o["bet_type"]),
            "買い目": combo_label(o["bet_type"], o["combo"]),
            "オッズ": round(float(o.get("odds", 0)), 1),
            "EV": round(float(o.get("expected_value", 0)), 2),
            "金額": int(o["stake"]),
        }
        for o in basket
    ]
    return pd.DataFrame(rows)


def basket_to_csv_bytes(basket: list[dict]) -> bytes:
    """発注票 CSV（Excel 互換 BOM 付き UTF-8）。"""
    return basket_to_frame(basket).drop(columns=["order_id"]).to_csv(index=False).encode("utf-8-sig")


# ---------------------------------------------------------------------------
# 発注記録・清算
# ---------------------------------------------------------------------------


def orders_to_history_records(basket: list[dict], status: str = "placed") -> list[dict]:
    """カートを投票履歴レコード（betting_history.jsonl 互換）へ変換する。"""
    now = dt.datetime.now().isoformat()
    return [
        {
            "race_id": o["race_id"],
            "bet_type": o["bet_type"],
            "combo": list(o["combo"]),
            "odds": o.get("odds"),
            "probability": o.get("probability"),
            "expected_value": o.get("expected_value"),
            "stake": o["stake"],
            "status": status,
            "ordered_at": now,
        }
        for o in basket
    ]


# Simulator と同じ馬券種 → BettingTickets メソッドのディスパッチ表
_SETTLE_DISPATCH = {
    BetType.TANSHO: "bet_tansho",
    BetType.FUKUSHO: "bet_fukusho",
    BetType.WAKUREN: "bet_wakuren_box",
    BetType.UMAREN: "bet_umaren_box",
    BetType.UMATAN: "bet_umatan_box",
    BetType.WIDE: "bet_wide_box",
    BetType.SANRENPUKU: "bet_sanrenpuku_box",
    BetType.SANRENTAN: "bet_sanrentan_box",
}


def settle_records(records: list[dict], betting_tickets) -> tuple[list[dict], int]:
    """発注済みレコードの払戻を確定する（結果が取得済みのレースのみ）。

    Parameters
    ----------
    records : 投票履歴（payout 未確定のものだけ清算を試みる）。
    betting_tickets : BettingTickets 互換オブジェクト（DI。テストではスタブ）。

    Returns
    -------
    (更新後レコード, 清算件数)
    """
    settled = 0
    out = []
    for r in records:
        r = dict(r)
        if r.get("payout") is None and r.get("status") in ("placed", "queued", "recommended"):
            method_name = _SETTLE_DISPATCH.get(r.get("bet_type"))
            if method_name is not None:
                try:
                    method = getattr(betting_tickets, method_name)
                    _, bet_amount, return_amount = method(
                        int(r["race_id"]), [int(x) for x in r["combo"]], int(r["stake"])
                    )
                    r["payout"] = float(return_amount)
                    r["hit"] = bool(return_amount > 0)
                    r["status"] = "settled"
                    r["settled_at"] = dt.datetime.now().isoformat()
                    settled += 1
                except KeyError:
                    pass  # 結果（払戻テーブル）が未取得のレースはスキップ
                except Exception:  # noqa: BLE001 — 1 件の失敗で清算全体を止めない
                    pass
        out.append(r)
    return out, settled


def rewrite_history(records: list[dict], path: str) -> None:
    """投票履歴 JSONL を全件書き換える（清算結果の反映用）。"""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
