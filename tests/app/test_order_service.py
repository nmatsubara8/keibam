"""発注サービス（app._order_service）のテスト。"""

import pytest

from app._order_service import BET_TYPE_LABELS
from app._order_service import add_orders
from app._order_service import basket_to_csv_bytes
from app._order_service import basket_to_frame
from app._order_service import basket_total
from app._order_service import candidates_to_orders
from app._order_service import combo_label
from app._order_service import exceeds_daily_cap
from app._order_service import format_ipat_text
from app._order_service import load_basket
from app._order_service import orders_to_history_records
from app._order_service import race_label
from app._order_service import rewrite_history
from app._order_service import round_stake
from app._order_service import save_basket
from app._order_service import settle_records
from src.constants._bet_types import BetType
from src.policies._bet_candidate import BetCandidate


def _candidate(stake=550.0, bet_type=BetType.TANSHO, combo=(7,)):
    return BetCandidate(
        race_id="202605030211",
        bet_type=bet_type,
        combo=combo,
        probability=0.3,
        odds=4.0,
        expected_value=1.2,
        stake=stake,
    )


class TestFormatting:
    def test_race_label_parses_place_and_race_no(self):
        assert race_label("202605030211") == "東京 11R (202605030211)"

    def test_race_label_passthrough_for_unknown(self):
        assert race_label("r1") == "r1"

    def test_combo_label_ordered_vs_unordered(self):
        assert combo_label(BetType.UMAREN, [3, 5]) == "3-5"
        assert combo_label(BetType.SANRENTAN, [1, 2, 3]) == "1→2→3"

    def test_round_stake_100_yen_units(self):
        assert round_stake(550) == 600
        assert round_stake(549) == 500
        assert round_stake(30) == 100  # 正の掛け金は最低 1 単位
        assert round_stake(0) == 0
        assert round_stake(-10) == 0


class TestBasket:
    def test_candidates_to_orders_rounds_and_skips_zero(self):
        orders = candidates_to_orders([_candidate(550.0), _candidate(0.0)])
        assert len(orders) == 1
        assert orders[0]["stake"] == 600
        assert orders[0]["combo"] == [7]

    def test_add_orders_replaces_same_bet(self):
        a = candidates_to_orders([_candidate(500.0)])
        b = candidates_to_orders([_candidate(900.0)])
        merged = add_orders(a, b)
        assert len(merged) == 1
        assert merged[0]["stake"] == 900

    def test_add_orders_keeps_distinct_bets(self):
        a = candidates_to_orders([_candidate(500.0)])
        b = candidates_to_orders([_candidate(500.0, combo=(8,))])
        assert len(add_orders(a, b)) == 2

    def test_save_load_roundtrip(self, tmp_path):
        path = str(tmp_path / "basket.json")
        orders = candidates_to_orders([_candidate()])
        save_basket(orders, path)
        assert load_basket(path) == orders

    def test_load_missing_returns_empty(self, tmp_path):
        assert load_basket(str(tmp_path / "nope.json")) == []

    def test_total_and_daily_cap(self):
        import types

        orders = candidates_to_orders([_candidate(500.0), _candidate(500.0, combo=(8,))])
        assert basket_total(orders) == 1000
        cfg = types.SimpleNamespace(bankroll=10000.0, max_daily_ratio=0.05)
        over, cap = exceeds_daily_cap(basket_total(orders), cfg)
        assert cap == 500.0
        assert over is True


class TestOutputs:
    def test_ipat_text_groups_by_race(self):
        orders = candidates_to_orders(
            [_candidate(500.0), _candidate(300.0, bet_type=BetType.UMAREN, combo=(3, 5))]
        )
        text = format_ipat_text(orders)
        assert "【東京 11R (202605030211)】" in text
        assert "単勝 7 — 500円" in text
        assert "馬連 3-5 — 300円" in text
        assert "合計 800円" in text

    def test_empty_basket_text(self):
        assert format_ipat_text([]) == ""

    def test_csv_bytes(self):
        orders = candidates_to_orders([_candidate()])
        csv = basket_to_csv_bytes(orders).decode("utf-8-sig")
        assert "レース" in csv and "単勝" in csv

    def test_frame_columns(self):
        frame = basket_to_frame(candidates_to_orders([_candidate()]))
        assert list(frame.columns) == ["order_id", "レース", "式別", "買い目", "オッズ", "EV", "金額"]


class TestHistoryAndSettlement:
    def test_orders_to_history_records(self):
        records = orders_to_history_records(candidates_to_orders([_candidate()]), status="placed")
        assert records[0]["status"] == "placed"
        assert records[0]["stake"] == 600
        assert "ordered_at" in records[0]

    def test_settle_records_with_stub_tickets(self):
        class _StubTickets:
            def bet_tansho(self, race_id, umaban, amount):
                if race_id == 202605030211:
                    return 1, amount, amount * 4.0  # 的中
                raise KeyError(race_id)  # 結果未取得

        records = [
            {"race_id": "202605030211", "bet_type": BetType.TANSHO, "combo": [7],
             "stake": 600, "status": "placed"},
            {"race_id": "209901010101", "bet_type": BetType.TANSHO, "combo": [1],
             "stake": 100, "status": "placed"},
            {"race_id": "202605030211", "bet_type": BetType.TANSHO, "combo": [9],
             "stake": 100, "status": "settled", "payout": 0.0},  # 既清算はスキップ
        ]
        updated, n = settle_records(records, _StubTickets())
        assert n == 1
        assert updated[0]["status"] == "settled"
        assert updated[0]["payout"] == 2400.0
        assert updated[0]["hit"] is True
        assert updated[1]["status"] == "placed"  # 結果未取得はそのまま
        assert updated[2]["payout"] == 0.0

    def test_rewrite_history_roundtrip(self, tmp_path):
        from app._betting_history import load_history

        path = str(tmp_path / "history.jsonl")
        records = [{"race_id": "r1", "bet_type": BetType.TANSHO, "combo": [1],
                    "stake": 100, "status": "settled", "payout": 340.0}]
        rewrite_history(records, path)
        loaded = load_history(path)
        assert len(loaded) == 1
        assert loaded[0]["payout"] == 340.0


def test_bet_type_labels_cover_all_bet_types():
    from src.constants._bet_types import COMBO_SIZE

    assert set(BET_TYPE_LABELS) == set(COMBO_SIZE)


def test_settle_dispatch_covers_all_labels():
    from app._order_service import _SETTLE_DISPATCH

    assert set(_SETTLE_DISPATCH) == set(BET_TYPE_LABELS)
