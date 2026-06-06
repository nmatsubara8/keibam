"""投票履歴の読み書き。

advisory モードの「承認して記録」ボタンが呼び出す I/O。
JSON Lines 形式（1行1レコード）で追記書き込みし、冪等に読み込む。
pandas/json のみに依存するためテスト可能。
"""

from __future__ import annotations

import json
import os

import pandas as pd

DEFAULT_HISTORY_PATH = "data/betting_history.jsonl"


def append_history(record: dict, path: str = DEFAULT_HISTORY_PATH) -> None:
    """1 件の投票記録を JSON Lines ファイルに追記する。"""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")


def load_history(path: str = DEFAULT_HISTORY_PATH) -> list[dict]:
    """JSON Lines ファイルを読み込む（ファイルがなければ空リスト）。"""
    if not os.path.exists(path):
        return []
    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return records


def history_to_dataframe(history: list[dict]) -> pd.DataFrame:
    """履歴リストを表示用 DataFrame に変換する。"""
    if not history:
        return pd.DataFrame(columns=["race_id", "bet_type", "combo", "odds", "stake", "status"])
    df = pd.DataFrame(history)
    # combo はリストとして保存されているため文字列化
    if "combo" in df.columns:
        df["combo"] = df["combo"].apply(lambda x: "-".join(str(v) for v in x) if isinstance(x, list) else str(x))
    return df


def calc_summary_stats(history: list[dict]) -> dict:
    """投票履歴から収支サマリを算出する（純粋関数）。

    payout が記録されていれば回収率も計算する。
    """
    if not history:
        return {"n_bets": 0, "total_stake": 0.0, "total_payout": 0.0, "return_rate": None}
    total_stake = sum(float(r.get("stake", 0)) for r in history)
    total_payout = sum(float(r.get("payout", 0)) for r in history)
    return_rate = (total_payout / total_stake) if total_stake > 0 else None
    return {
        "n_bets": len(history),
        "total_stake": total_stake,
        "total_payout": total_payout,
        "return_rate": round(return_rate, 4) if return_rate is not None else None,
    }
