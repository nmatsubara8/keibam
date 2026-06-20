"""BettingTickets キャラクタリゼーションテスト（Strategy リファクタの回帰ガード）。

ディスパッチ表（src/simulation/_simulator.py の 8 馬券種）が呼ぶ 8 メソッドについて、
- 的中時の払戻計算（return_amount = return_X * amount / 100）
- 賭け枚数（n_bets）と賭け金額（bet_amount = n_bets * amount）の算出
- 最小頭数未満や空入力での早期 return
- BOX 馬券の組合せ／順列展開
- ワイドのような多重的中
を固定する。Strategy パターン移行後も同一の戻り値になることを保証する。

ReturnProcessor は preprocessed_data だけを参照するため、辞書を持つフェイクで差し替える。
"""

from __future__ import annotations

import math

import pandas as pd
import pytest

from src.simulation._betting_tickets import BettingTickets


class _FakeReturnProcessor:
    """BettingTickets が触る preprocessed_data 属性だけを持つフェイク。"""

    def __init__(self, **tables):
        # 馬券種ごとに欠けているテーブルは空 DataFrame で埋める
        keys = ("tansho", "fukusho", "wakuren", "umaren", "umatan", "wide", "sanrenpuku", "sanrentan")
        self.preprocessed_data = {k: tables.get(k, pd.DataFrame()) for k in keys}


def _df(rows: dict, index):
    return pd.DataFrame(rows, index=index)


class TestRaceIdTypeAgnostic:
    """払戻テーブルの race_id 型(pickle=int / DB復元=str)と照合キー型が違っても的中する。"""

    def test_str_indexed_table_matches_int_arg(self):
        # DB 復元由来: index が str。照合キーが int でも str 正規化で的中する。
        tansho = _df({"win_0": [5], "return_0": [350]}, index=["202401010101"])
        bt = BettingTickets(_FakeReturnProcessor(tansho=tansho))
        n, bet, ret = bt.bet_tansho(202401010101, [5], 100)
        assert (n, bet, ret) == (1, 100, 350.0)

    def test_int_indexed_table_matches_str_arg(self):
        # pickle 由来: index が int。照合キーが str でも的中する。
        tansho = _df({"win_0": [5], "return_0": [350]}, index=[202401010101])
        bt = BettingTickets(_FakeReturnProcessor(tansho=tansho))
        n, bet, ret = bt.bet_tansho("202401010101", [5], 100)
        assert (n, bet, ret) == (1, 100, 350.0)


class TestTansho:
    def _bt(self):
        # race=1: 5番が単勝、return=350 (×amount/100)
        tansho = _df({"win_0": [5], "return_0": [350]}, index=[1])
        return BettingTickets(_FakeReturnProcessor(tansho=tansho))

    def test_hit_single(self):
        n, bet, ret = self._bt().bet_tansho(1, [5], 100)
        assert n == 1
        assert bet == 100
        assert ret == 350.0  # 350 * 100 / 100

    def test_miss(self):
        n, bet, ret = self._bt().bet_tansho(1, [3], 100)
        assert (n, bet, ret) == (1, 100, 0)

    def test_race_not_in_payout_table_returns_zero(self):
        # 払戻テーブルに無い race_id（return_tables 未取得）は KeyError で落ちず
        # (0, 0, 0) を返して集計から除外される。
        n, bet, ret = self._bt().bet_tansho(999, [5], 100)
        assert (n, bet, ret) == (0, 0, 0)

    def test_multi_horse_partial_hit(self):
        n, bet, ret = self._bt().bet_tansho(1, [3, 5, 7], 100)
        assert n == 3
        assert bet == 300  # 3 * 100
        assert ret == 350.0

    def test_empty_returns_triple_zero(self):
        assert self._bt().bet_tansho(1, [], 100) == (0, 0, 0)


class TestFukusho:
    def _bt(self):
        # race=1: 上位3頭 (5,3,7) がそれぞれ複勝対象
        fukusho = _df(
            {
                "win_0": [5], "return_0": [120],
                "win_1": [3], "return_1": [180],
                "win_2": [7], "return_2": [220],
            },
            index=[1],
        )
        return BettingTickets(_FakeReturnProcessor(fukusho=fukusho))

    def test_hit_one_of_three_winners(self):
        n, bet, ret = self._bt().bet_fukusho(1, [3], 100)
        assert (n, bet, ret) == (1, 100, 180.0)

    def test_hit_all_three_winners_sums(self):
        n, bet, ret = self._bt().bet_fukusho(1, [3, 5, 7], 100)
        assert n == 3
        assert bet == 300
        assert ret == pytest.approx(120 + 180 + 220)

    def test_miss_all(self):
        n, bet, ret = self._bt().bet_fukusho(1, [1, 2], 100)
        assert (n, bet, ret) == (2, 200, 0)

    def test_empty(self):
        assert self._bt().bet_fukusho(1, [], 100) == (0, 0, 0)


class TestWakurenBox:
    def _bt(self):
        # race=10: 枠連の的中組合せが (3, 5)。
        # 注意: bet_wakuren_box は range(length - 1) で走るため、length=1 の場合
        # 走査は発生せず常に return=0 になる（既存の挙動・保持する）。
        # length=2 にするため余りの win_1/return_1 をダミーで持たせる。
        wakuren = _df(
            {
                "win_0": [(3, 5)], "return_0": [500],
                "win_1": [(0, 0)], "return_1": [0],
            },
            index=[10],
        )
        return BettingTickets(_FakeReturnProcessor(wakuren=wakuren))

    def test_hit_with_two_horses(self):
        # 2頭 BOX: combinations_with_replacement([3,5], 2) -> {(3,3),(3,5),(5,5)} の 3 券
        n, bet, ret = self._bt().bet_wakuren_box(10, [3, 5], 100)
        assert n == 3
        assert bet == 300  # 3 * 100
        assert ret == pytest.approx(500.0)  # 1 件的中

    def test_below_min_returns_zero(self):
        assert self._bt().bet_wakuren_box(10, [3], 100) == (0, 0, 0)

    def test_none_returns_zero(self):
        assert self._bt().bet_wakuren_box(10, None, 100) == (0, 0, 0)


class TestUmarenBox:
    def _bt(self):
        umaren = _df({"win_0": [[3, 5]], "return_0": [1500]}, index=[1])
        return BettingTickets(_FakeReturnProcessor(umaren=umaren))

    def test_hit_two_horse_box(self):
        n, bet, ret = self._bt().bet_umaren_box(1, [3, 5], 100)
        assert n == 1  # 2C2 = 1
        assert bet == 100
        assert ret == pytest.approx(1500.0)

    def test_three_horse_box_one_hit(self):
        # 3C2 = 3 通り。うち (3,5) だけ的中
        n, bet, ret = self._bt().bet_umaren_box(1, [3, 5, 7], 100)
        assert n == 3
        assert bet == 300
        assert ret == pytest.approx(1500.0)

    def test_miss(self):
        n, bet, ret = self._bt().bet_umaren_box(1, [1, 2], 100)
        assert n == 1 and bet == 100 and ret == 0

    def test_below_min(self):
        assert self._bt().bet_umaren_box(1, [3], 100) == (0, 0, 0)


class TestUmatanBox:
    def _bt(self):
        # 1着→2着が (3,5) のレース
        umatan = _df({"win_0": [[3, 5]], "return_0": [4500]}, index=[1])
        return BettingTickets(_FakeReturnProcessor(umatan=umatan))

    def test_hit_correct_order(self):
        # 2 頭 BOX = 2P2 = 2 券 ((3,5), (5,3))。(3,5) のみ的中。
        n, bet, ret = self._bt().bet_umatan_box(1, [3, 5], 100)
        assert n == 2
        assert bet == 200
        assert ret == pytest.approx(4500.0)

    def test_three_horse_box(self):
        # 3P2 = 6 券
        n, bet, ret = self._bt().bet_umatan_box(1, [3, 5, 7], 100)
        assert n == 6
        assert bet == 600
        assert ret == pytest.approx(4500.0)

    def test_below_min(self):
        assert self._bt().bet_umatan_box(1, [3], 100) == (0, 0, 0)


class TestWideBox:
    def _bt(self):
        # ワイドは上位3頭の組合せが全て的中するため、3 枠（3つの的中ペア）
        wide = _df(
            {
                "win_0": [[3, 5]], "return_0": [800],
                "win_1": [[3, 7]], "return_1": [900],
                "win_2": [[5, 7]], "return_2": [1100],
            },
            index=[1],
        )
        return BettingTickets(_FakeReturnProcessor(wide=wide))

    def test_two_horse_box_one_hit(self):
        n, bet, ret = self._bt().bet_wide_box(1, [3, 5], 100)
        assert n == 1
        assert bet == 100
        assert ret == pytest.approx(800.0)

    def test_three_horse_box_all_hits(self):
        # 3頭 (3,5,7) なら 3C2=3 券で 3 件全て的中
        n, bet, ret = self._bt().bet_wide_box(1, [3, 5, 7], 100)
        assert n == 3
        assert bet == 300
        assert ret == pytest.approx(800 + 900 + 1100)

    def test_below_min(self):
        assert self._bt().bet_wide_box(1, [3], 100) == (0, 0, 0)


class TestSanrenpukuBox:
    def _bt(self):
        sanrenpuku = _df({"win_0": [[3, 5, 7]], "return_0": [12000]}, index=[1])
        return BettingTickets(_FakeReturnProcessor(sanrenpuku=sanrenpuku))

    def test_hit_three_horse_box(self):
        # 3C3 = 1 券、的中
        n, bet, ret = self._bt().bet_sanrenpuku_box(1, [3, 5, 7], 100)
        assert n == 1
        assert bet == 100
        assert ret == pytest.approx(12000.0)

    def test_four_horse_box(self):
        # 4C3 = 4 券、うち (3,5,7) だけ的中
        n, bet, ret = self._bt().bet_sanrenpuku_box(1, [3, 5, 7, 9], 100)
        assert n == 4
        assert bet == 400
        assert ret == pytest.approx(12000.0)

    def test_below_min(self):
        assert self._bt().bet_sanrenpuku_box(1, [3, 5], 100) == (0, 0, 0)


class TestSanrentanBox:
    def _bt(self):
        sanrentan = _df({"win_0": [[3, 5, 7]], "return_0": [80000]}, index=[1])
        return BettingTickets(_FakeReturnProcessor(sanrentan=sanrentan))

    def test_hit_three_horse_box_one_perm(self):
        # 3P3 = 6 券。(3,5,7) 一致のみ的中。
        n, bet, ret = self._bt().bet_sanrentan_box(1, [3, 5, 7], 100)
        assert n == 6
        assert bet == 600
        assert ret == pytest.approx(80000.0)

    def test_four_horse_box(self):
        # 4P3 = 24 券、うち 1 件のみ的中
        n, bet, ret = self._bt().bet_sanrentan_box(1, [3, 5, 7, 9], 100)
        assert n == 24
        assert bet == 2400
        assert ret == pytest.approx(80000.0)

    def test_below_min(self):
        assert self._bt().bet_sanrentan_box(1, [3, 5], 100) == (0, 0, 0)


class TestBetAmountAlwaysProductOfNBetsAndAmount:
    """bet_amount = n_bets * amount の不変条件を全 8 メソッドで確認する。"""

    @pytest.mark.parametrize(
        "method,umaban,amount,expected_n",
        [
            ("bet_tansho", [3, 5, 7], 200, 3),
            ("bet_fukusho", [3, 5, 7], 200, 3),
            ("bet_wakuren_box", [3, 5, 7], 200, 6),  # combinations_with_replacement の dedup
            ("bet_umaren_box", [3, 5, 7], 200, 3),  # 3C2
            ("bet_umatan_box", [3, 5, 7], 200, 6),  # 3P2
            ("bet_wide_box", [3, 5, 7], 200, 3),  # 3C2
            ("bet_sanrenpuku_box", [3, 5, 7, 9], 200, 4),  # 4C3
            ("bet_sanrentan_box", [3, 5, 7, 9], 200, 24),  # 4P3
        ],
    )
    def test_amount_invariant(self, method, umaban, amount, expected_n):
        # 空 DataFrame でも n_bets / bet_amount は計算される（return_amount は 0）。
        # 各馬券種に該当 race_id を含む最小限の DataFrame を投入する。
        race_id = 99
        tables = {
            "tansho": _df({"win_0": [0], "return_0": [0]}, index=[race_id]),
            "fukusho": _df({"win_0": [0], "return_0": [0]}, index=[race_id]),
            "wakuren": _df(
                {"win_0": [(0, 0)], "return_0": [0], "win_1": [(0, 0)], "return_1": [0]},
                index=[race_id],
            ),
            "umaren": _df({"win_0": [[0, 0]], "return_0": [0]}, index=[race_id]),
            "umatan": _df({"win_0": [[0, 0]], "return_0": [0]}, index=[race_id]),
            "wide": _df({"win_0": [[0, 0]], "return_0": [0]}, index=[race_id]),
            "sanrenpuku": _df({"win_0": [[0, 0, 0]], "return_0": [0]}, index=[race_id]),
            "sanrentan": _df({"win_0": [[0, 0, 0]], "return_0": [0]}, index=[race_id]),
        }
        bt = BettingTickets(_FakeReturnProcessor(**tables))
        n, bet, ret = getattr(bt, method)(race_id, umaban, amount)
        assert n == expected_n
        assert bet == expected_n * amount
        assert math.isclose(ret, 0)
