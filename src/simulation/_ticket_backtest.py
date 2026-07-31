"""物理シムの着順標本(top3_orders)から券種確率を直接集計し、買い方(戦略)別に回収率を測る。

■ 核心（既存 `_harville`/`multibet_roi_test` との差異化）
連系（馬連〜三連単）の確率を「周辺勝率・複勝率の積」ではなく、モンテカルロの着順標本
top3_orders の **同時頻度** から直接推定する。着位間の相関——先行総崩れ→差し 1-2-3、展開一括
逃げ残りなど、物理シムが内生する着順の従属——を確率へそのまま織り込める。Harville は
「各馬の強さ→周辺確率」から連を組み立てるため、この従属を構造的に表せない。ここが本モジュール
固有の価値であり、`aggregate_ticket_probabilities` がその橋渡し。

■ リーク方針
- 券種確率・買い目生成は as-of 特徴のみ由来の sim から作る（着順・確定オッズを入力にしない）。
- 購入時オッズが保存されているのは単勝・複勝のみ。連系はレース前オッズを持たないため、
  EV 選別（p×odds>t）は単勝・複勝に限定し、連系は「戦略が決めた買い目を固定購入」。
  決済（払戻）は確定払戻ルックアップ（`_payoffs`）を使う——これは購入判断に使わない後決済。

■ 入出力
top3_orders: (n_sim, 3) 各 sim の 1着/2着/3着の **馬インデックス**（field 内の並び順の位置）。
             `monte_carlo(..., return_orders=True)["top3_orders"]` がこれ。
umaban:      (n_horses,) インデックス→実馬番。券種確率・買い目は馬番で表現し確定払戻と照合する。
"""
from __future__ import annotations

from collections import Counter

import numpy as np

# 券種キー（`_payoffs` / BetType のラベルと対応）
TANSHO = "tansho"
FUKUSHO = "fukusho"
UMAREN = "umaren"
UMATAN = "umatan"
WIDE = "wide"
SANRENPUKU = "sanrenpuku"
SANRENTAN = "sanrentan"


def aggregate_ticket_probabilities(top3_orders, umaban) -> dict[str, dict]:
    """着順標本(top3_orders)から券種別の {買い目: 確率} を **同時頻度** で直接推定する。

    連系確率＝周辺確率の積、ではない。各 sim の (1着,2着,3着) を 1 標本として数え上げるので、
    着位間の相関が確率にそのまま入る。返す買い目キー:
      tansho     : 馬番(int)                      P(その馬が1着)
      fukusho    : 馬番(int)                      P(その馬が3着以内)
      umatan     : (1着馬番, 2着馬番)              順序あり
      umaren     : (小, 大) 馬番の昇順タプル        順序なし top2
      wide       : (小, 大) 馬番の昇順タプル        3着以内の2頭（1sim最大3組）
      sanrentan  : (1着,2着,3着) 馬番              順序あり
      sanrenpuku : (昇順3馬番) タプル              順序なし top3
    出現しなかった買い目はキーを持たない（疎）。確率は 0..1（n_sim で正規化）。
    """
    orders = np.asarray(top3_orders)
    if orders.ndim != 2 or orders.shape[0] == 0:
        return {k: {} for k in (TANSHO, FUKUSHO, UMATAN, UMAREN, WIDE, SANRENTAN, SANRENPUKU)}
    uma = np.asarray(umaban)
    n_sim = orders.shape[0]
    w = 1.0 / n_sim

    c_tan: Counter = Counter()
    c_fuku: Counter = Counter()
    c_umatan: Counter = Counter()
    c_umaren: Counter = Counter()
    c_wide: Counter = Counter()
    c_stan: Counter = Counter()
    c_spuku: Counter = Counter()

    for row in orders:
        a, b, c = (int(uma[int(row[0])]), int(uma[int(row[1])]), int(uma[int(row[2])]))
        c_tan[a] += 1
        for u in (a, b, c):
            c_fuku[u] += 1
        c_umatan[(a, b)] += 1
        c_umaren[tuple(sorted((a, b)))] += 1
        c_stan[(a, b, c)] += 1
        c_spuku[tuple(sorted((a, b, c)))] += 1
        for pair in ((a, b), (a, c), (b, c)):
            c_wide[tuple(sorted(pair))] += 1

    def _norm(counter: Counter) -> dict:
        return {k: v * w for k, v in counter.items()}

    return {
        TANSHO: _norm(c_tan),
        FUKUSHO: _norm(c_fuku),
        UMATAN: _norm(c_umatan),
        UMAREN: _norm(c_umaren),
        WIDE: _norm(c_wide),
        SANRENTAN: _norm(c_stan),
        SANRENPUKU: _norm(c_spuku),
    }


def sim_rank(win_probs, umaban) -> list[int]:
    """sim 勝率(周辺)の降順に並べた馬番リスト（買い目生成の軸＝「sim が上位と見た順」）。"""
    win = np.asarray(win_probs, dtype=float)
    uma = np.asarray(umaban)
    order = np.argsort(-win, kind="stable")
    return [int(uma[i]) for i in order]


# ────────────────────────── 買い目生成器（戦略テンプレの部品） ──────────────────────────
# 各生成器は sim_rank（馬番の降順リスト）を受け、購入する買い目のリストを返す。純関数・
# オッズ非依存（買い方だけを定義）。EV 選別・固定購入・決済は呼び出し側で行う。

def trifecta_top2_reverse(rank: list[int], third_slots=(2, 3, 4, 5)) -> list[tuple]:
    """三連単「1↔2位 → n位」: 上位2頭を1着2着で入替(2通り)、3着を third_slots の馬に流す。

    S4（利用者の代表例「1↔2位→3～6位 8点」）。third_slots は 0 始まりの sim 順位。
    3着候補が top2 と重なる場合は除外（三連単は3頭相異）。
    """
    if len(rank) < 3:
        return []
    a, b = rank[0], rank[1]
    thirds = [rank[i] for i in third_slots if i < len(rank) and rank[i] not in (a, b)]
    out: list[tuple] = []
    for first, second in ((a, b), (b, a)):
        for c in thirds:
            out.append((first, second, c))
    return out


def trifecta_single_winner(rank: list[int], winner_slot=0, sub_slots=(1, 2, 3, 4)) -> list[tuple]:
    """三連単「1位固定 → 2・3着流し」: winner を1着固定、2着3着を sub_slots の順列で買う。"""
    if len(rank) < 3:
        return []
    if winner_slot >= len(rank):
        return []
    w = rank[winner_slot]
    subs = [rank[i] for i in sub_slots if i < len(rank) and rank[i] != w]
    out: list[tuple] = []
    for i in range(len(subs)):
        for j in range(len(subs)):
            if i != j:
                out.append((w, subs[i], subs[j]))
    return out


def trio_top_n(rank: list[int], n: int = 4) -> list[tuple]:
    """三連複「上位n頭ボックス」: sim 上位 n 頭から3頭の組合せ（順序なし・昇順キー）。"""
    from itertools import combinations
    picks = rank[:n]
    return [tuple(sorted(c)) for c in combinations(picks, 3)]


def quinella_top_n(rank: list[int], n: int = 3) -> list[tuple]:
    """馬連「上位n頭ボックス」: sim 上位 n 頭から2頭の組合せ（順序なし・昇順キー）。"""
    from itertools import combinations
    picks = rank[:n]
    return [tuple(sorted(c)) for c in combinations(picks, 2)]


def wide_top_n(rank: list[int], n: int = 3) -> list[tuple]:
    """ワイド「上位n頭ボックス」: 馬連と同形（決済は 3着以内2頭で当たり）。"""
    return quinella_top_n(rank, n)


# ────────────────────────── 戦略テンプレ（買い方＝券種×生成器の組） ──────────────────────────
# 各戦略は sim_rank（馬番の降順リスト）→ [(bet_type, combo), ...] を返す純関数。買い方だけを
# 定義し、確率/オッズ/決済は runner が担う。S4=利用者の代表例「三連単 1↔2位→3～6位 8点」。

def _s_tansho_top1(rank):     # 対照: 単勝1位（購入時オッズあり＝EV選別可）
    return [(TANSHO, (rank[0],))] if rank else []


def _s_fukusho_top1(rank):    # 対照: 複勝1位
    return [(FUKUSHO, (rank[0],))] if rank else []


def _s1_trio_top4(rank):      # S1: 三連複 上位4頭ボックス（4点）
    return [(SANRENPUKU, c) for c in trio_top_n(rank, 4)]


def _s2_quinella_top3(rank):  # S2: 馬連 上位3頭ボックス（3点）
    return [(UMAREN, c) for c in quinella_top_n(rank, 3)]


def _s3_wide_top3(rank):      # S3: ワイド 上位3頭ボックス（3点）
    return [(WIDE, c) for c in wide_top_n(rank, 3)]


def _s4_trifecta_top2_reverse(rank):  # S4: 三連単 1↔2位→3～6位（8点）＝利用者代表例
    return [(SANRENTAN, c) for c in trifecta_top2_reverse(rank, (2, 3, 4, 5))]


def _s5_trifecta_single_winner(rank):  # S5: 三連単 1位固定→2・3着流し（上位2-5頭の順列）
    return [(SANRENTAN, c) for c in trifecta_single_winner(rank, 0, (1, 2, 3, 4))]


def _s6_trio_top5(rank):      # S6: 三連複 上位5頭ボックス（10点・的中率↑/配当↓）
    return [(SANRENPUKU, c) for c in trio_top_n(rank, 5)]


def _s0_skip(rank):           # S0: 何も買わない（帰無・取りこぼし基準）
    return []


STRATEGY_TEMPLATES = {
    "S0_skip": _s0_skip,
    "単勝1位": _s_tansho_top1,
    "複勝1位": _s_fukusho_top1,
    "S1_三連複box4": _s1_trio_top4,
    "S2_馬連box3": _s2_quinella_top3,
    "S3_ワイドbox3": _s3_wide_top3,
    "S4_三連単1↔2→3-6": _s4_trifecta_top2_reverse,
    "S5_三連単1着固定流し": _s5_trifecta_single_winner,
    "S6_三連複box5": _s6_trio_top5,
}


# ────────────────────────── runner（sim→買い目→確定払戻決済） ──────────────────────────
# 既存資産の再利用: 決済は `BettingTickets.settle_one`（8券種・厳密照合）、集計/ファットテール
# 指標は `_backtest.BetTypeStats`（roi_ex_top=除最大1件・reliable・top_share）。ここでは
# sim 着順標本→買い目生成→BetCandidate 化までと、既存に無い「年別/レース単位 bootstrap CI」を担う。

def _prob_key(bet_type: str, combo):
    """券種別の確率ルックアップ・キー（単複=int馬番 / 連系=canonical_combo）。"""
    from src.constants._bet_types import COMBO_SIZE, canonical_combo
    if COMBO_SIZE.get(bet_type, 1) == 1:
        return int(combo[0])
    return canonical_combo(bet_type, combo)


def build_candidates(race_id, rank: list[int], probs: dict, strategy,
                     *, odds_lookup: dict | None = None):
    """1レースの sim 結果に戦略を適用し `BetCandidate` のリストを返す。

    probs = `aggregate_ticket_probabilities` の出力。odds_lookup={(race_id,馬番):倍率} は
    購入時オッズが存在する単複だけに与える（連系はレース前オッズを持たない＝EV選別しない＝
    odds/EV=0 の固定購入）。リーク安全: 確率・買い目は sim 由来のみ。
    """
    from src.policies._bet_candidate import BetCandidate
    out = []
    for bet_type, combo in strategy(rank):
        combo = tuple(int(x) for x in combo)
        p = float(probs.get(bet_type, {}).get(_prob_key(bet_type, combo), 0.0))
        odds = 0.0
        if odds_lookup is not None and len(combo) == 1:
            odds = float(odds_lookup.get((str(race_id), combo[0]), 0.0))
        out.append(BetCandidate(race_id=str(race_id), bet_type=bet_type, combo=combo,
                                probability=p, odds=odds, expected_value=p * odds))
    return out


def settle_per_race(candidates, return_processor, unit: int = 1) -> dict:
    """候補を race_id 単位に決済し {race_id: {"stake","returned","n_bets","n_hits"}} を返す。

    `_backtest.settle_candidates` は券種別に潰すため年別/レース単位 CI が取れない。こちらは
    レース粒度を保つ（`BettingTickets.settle_one` を各点に適用）。払戻テーブルに無いレースの
    点は除外（評価不能を 0 回収で混ぜない＝既存 `_settle_detailed` と同方針）。
    """
    from src.simulation._betting_tickets import BettingTickets
    tickets = BettingTickets(return_processor)
    per: dict = {}
    for c in candidates:
        n_bets, bet_amount, returned = tickets.settle_one(c.bet_type, c.race_id, c.combo, unit)
        if n_bets == 0:
            continue
        rid = str(c.race_id)
        d = per.setdefault(rid, {"stake": 0.0, "returned": 0.0, "n_bets": 0, "n_hits": 0})
        d["stake"] += bet_amount
        d["returned"] += returned
        d["n_bets"] += n_bets
        d["n_hits"] += 1 if returned > 0 else 0
    return per


def roi_by_year(per_race: dict) -> dict:
    """{race_id: {...}} → {year: roi}（race_id 先頭4桁を年とみなす）。年安定性の確認用。"""
    agg: dict = {}
    for rid, d in per_race.items():
        yr = str(rid)[:4]
        if not yr.isdigit():
            continue
        a = agg.setdefault(yr, {"stake": 0.0, "returned": 0.0})
        a["stake"] += d["stake"]
        a["returned"] += d["returned"]
    return {y: (a["returned"] / a["stake"] if a["stake"] else 0.0) for y, a in sorted(agg.items())}


def race_bootstrap_ci(per_race: dict, *, n_boot: int = 2000, alpha: float = 0.05,
                      seed: int = 0) -> dict:
    """レース単位リサンプルで ROI の信頼区間を出す（三連単等ファットテールの頑健評価）。

    馬券は同一レース内で強く相関する（同じ結果で複数点が同時に当外）ため、点単位でなく
    **レース単位** でブートストラップする。返す: {"roi","lo","hi","n_races"}。
    """
    import numpy as np
    rids = list(per_race.keys())
    n = len(rids)
    if n == 0:
        return {"roi": 0.0, "lo": 0.0, "hi": 0.0, "n_races": 0}
    stake = np.array([per_race[r]["stake"] for r in rids], dtype=float)
    ret = np.array([per_race[r]["returned"] for r in rids], dtype=float)
    point = ret.sum() / stake.sum() if stake.sum() else 0.0
    rng = np.random.default_rng(seed)
    rois = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.integers(0, n, size=n)
        s = stake[idx].sum()
        rois[b] = ret[idx].sum() / s if s else 0.0
    lo, hi = np.quantile(rois, [alpha / 2, 1 - alpha / 2])
    return {"roi": float(point), "lo": float(lo), "hi": float(hi), "n_races": n}
