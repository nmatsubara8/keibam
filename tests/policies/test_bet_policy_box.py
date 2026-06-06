"""BetPolicy*（閾値ベースの旧ポリシー群）のキャラクタリゼーションテスト。

リファクタリング #2（Box 集約 + @staticmethod 修正）の回帰ガード。
bet_dict 形式（{race_id: {馬券種: [馬番...]}}）と最小頭数フィルタ、および
クラス呼び出し / インスタンス呼び出し両方で動作することを固定する。
"""

from __future__ import annotations

import pandas as pd
import pytest

from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import (
    BetPolicyFukusho,
    BetPolicySanrenpukuBox,
    BetPolicySanrentanBox,
    BetPolicyTansho,
    BetPolicyUmarenBox,
    BetPolicyUmatanBox,
    BetPolicyWakurenBox,
    BetPolicyWideBox,
)


def _score_table(rows):
    """rows: (race_id, umaban, score[, wakuban, wakuban_flag])"""
    data = []
    for r in rows:
        race_id, umaban, score = r[0], r[1], r[2]
        d = {"score": score, ResultsCols.UMABAN: umaban}
        if len(r) > 3:
            d[ResultsCols.WAKUBAN] = r[3]
            d["wakuban_flag"] = r[4]
        data.append(d)
    return pd.DataFrame(data, index=[r[0] for r in rows])


# ──────────────────────────────────────────
# 単勝 / 複勝（最小頭数フィルタなし）
# ──────────────────────────────────────────

class TestTanshoFukusho:
    def test_tansho_filters_by_threshold(self):
        table = _score_table([("r1", 3, 0.9), ("r1", 5, 0.2), ("r2", 1, 0.8)])
        result = BetPolicyTansho.judge(table, 0.5)
        assert result == {"r1": {"tansho": [3]}, "r2": {"tansho": [1]}}

    def test_fukusho_key_and_filter(self):
        table = _score_table([("r1", 3, 0.9), ("r1", 5, 0.6)])
        result = BetPolicyFukusho.judge(table, 0.5)
        assert set(result["r1"]["fukusho"]) == {3, 5}

    def test_empty_when_none_pass(self):
        table = _score_table([("r1", 3, 0.1)])
        assert BetPolicyTansho.judge(table, 0.5) == {}


# ──────────────────────────────────────────
# Box 系（最小頭数フィルタあり: 馬連/馬単/ワイド=2、三連複/単=3）
# 各: (クラス, キー, 最小頭数)
# ──────────────────────────────────────────

_BOX_CASES = [
    (BetPolicyUmarenBox, "umaren", 2),
    (BetPolicyUmatanBox, "umatan", 2),
    (BetPolicyWideBox, "wide", 2),
    (BetPolicySanrenpukuBox, "sanrenpuku", 3),
    (BetPolicySanrentanBox, "sanrentan", 3),
]


@pytest.mark.parametrize("policy,key,min_horses", _BOX_CASES)
class TestBoxPolicies:
    def test_includes_race_with_enough_horses(self, policy, key, min_horses):
        rows = [("r1", i, 0.9) for i in range(1, min_horses + 1)]
        table = _score_table(rows)
        result = policy.judge(table, 0.5)
        assert key in result["r1"]
        assert set(result["r1"][key]) == set(range(1, min_horses + 1))

    def test_excludes_race_below_min_horses(self, policy, key, min_horses):
        # min_horses-1 頭しか閾値超えしないレースは除外される
        rows = [("r1", i, 0.9) for i in range(1, min_horses)]
        table = _score_table(rows)
        result = policy.judge(table, 0.5)
        assert result == {}

    def test_instance_call_works(self, policy, key, min_horses):
        """@staticmethod 修正後はインスタンス経由でも動作する（バグ回帰防止）。"""
        rows = [("r1", i, 0.9) for i in range(1, min_horses + 1)]
        table = _score_table(rows)
        result = policy().judge(table, 0.5)
        assert key in result["r1"]


# ──────────────────────────────────────────
# 枠連（wakuban_flag + WAKUBAN 使用の特殊系）
# ──────────────────────────────────────────

class TestWakurenBox:
    def test_uses_wakuban_and_flag(self):
        table = _score_table(
            [("r1", 1, 0.9, 2, 1), ("r1", 2, 0.9, 4, 1), ("r1", 3, 0.9, 6, 0)]
        )
        result = BetPolicyWakurenBox.judge(table, 0.5)
        # wakuban_flag==1 の枠 2,4 のみ採用（6 は flag 0 で除外）
        assert set(result["r1"]["wakuren"]) == {2, 4}

    def test_instance_call_works(self):
        table = _score_table([("r1", 1, 0.9, 2, 1), ("r1", 2, 0.9, 4, 1)])
        result = BetPolicyWakurenBox().judge(table, 0.5)
        assert "wakuren" in result["r1"]
