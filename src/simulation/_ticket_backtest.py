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


def validate_ranking(ranking: list[int], race_id: str = "") -> None:
    """順位配列に馬番の重複が無いか検証（ROI 以前のデータ整合性ガード）。

    正常な順位列は各馬が1度ずつ現れる順列。重複があれば買い目生成が壊れるため即エラー。
    """
    valid = [int(h) for h in ranking if h is not None]
    if len(valid) != len(set(valid)):
        dups = sorted({h for h in valid if valid.count(h) > 1})
        raise ValueError(f"{race_id}: ranking に重複馬番: {dups}")


def s4_point_audit(rank: list[int]) -> dict:
    """S4 の期待点数(8)と実際・不足理由を返す（点数不足はほぼ 6頭未満の小頭数が原因）。

    正常な順位列なら S4 は 6頭以上で必ず 8点。不足は third_slots(2..5) が頭数を超えるため
    （＝小頭数）で、重複除外ではない。reason: "full" / "small_field(n=..)"。
    """
    tickets = trifecta_top2_reverse(rank, (2, 3, 4, 5))
    expected = 8 if len(rank) >= 6 else 0
    n = len(rank)
    reason = "full" if len(tickets) == 8 else f"small_field(n={n})"
    return {"expected": expected, "actual": len(tickets), "n_horses": n, "reason": reason}


# 馬単生成器（順序あり＝1着→2着）。
def exacta_top2_reverse(rank: list[int]) -> list[tuple]:
    """馬単「1↔2位折り返し」: Sim1位→2位 と 2位→1位 の2点。"""
    if len(rank) < 2:
        return []
    return [(rank[0], rank[1]), (rank[1], rank[0])]


def exacta_single_winner(rank: list[int], sub_slots=(1, 2, 3)) -> list[tuple]:
    """馬単「1位固定→2着流し」: Sim1位→Sim(2..) の順序券。既定は 2〜4位で3点。"""
    if len(rank) < 2:
        return []
    w = rank[0]
    return [(w, rank[i]) for i in sub_slots if i < len(rank) and rank[i] != w]


def wide_axis_flow(rank: list[int], sub_slots=(1, 2, 3, 4)) -> list[tuple]:
    """ワイド「1位軸→2〜5位」: Sim1位と Sim(2..5) のワイド（3着内2頭で当たり）。既定4点。

    上位3頭ボックスと違い Sim1位の複勝圏信頼度を使う戦略（相手は広めに流す）。
    """
    if len(rank) < 2:
        return []
    a = rank[0]
    return [tuple(sorted((a, rank[i]))) for i in sub_slots if i < len(rank) and rank[i] != a]


def joint_topk(probs: dict, bet_type: str, k: int) -> list[tuple]:
    """券種の同時確率上位 k 点の買い目を返す（MC の順位依存構造を直接使う joint 版）。

    probs=`aggregate_ticket_probabilities` の出力。周辺勝率順位ではなく、着順標本から推定した
    同時確率が高い組合せを選ぶ。rank 版と同じ点数で並べると「MC の順位分布を使う価値」を測れる。
    """
    items = sorted(probs.get(bet_type, {}).items(), key=lambda kv: -kv[1])
    return [combo for combo, p in items[:k] if p > 0]


# ────────────────────────── 戦略テンプレ（買い方＝券種×生成器の組） ──────────────────────────
# 各戦略は (rank, probs) → [(bet_type, combo), ...] を返す純関数。rank=Sim勝率(周辺)降順の馬番、
# probs=同時確率(aggregate_ticket_probabilities)。rank 版は probs を無視、joint 版は probs を使う。
# rank 版と同点数の joint 版を並べ、「MC の順位依存構造を使う価値」を直接比較する（S4↔J2 等）。

def _s_tansho_top1(rank, probs):    # 対照: 単勝1位
    return [(TANSHO, (rank[0],))] if rank else []


def _s_fukusho_top1(rank, probs):   # 対照: 複勝1位
    return [(FUKUSHO, (rank[0],))] if rank else []


def _s1_trio_top4(rank, probs):     # S1: 三連複 上位4頭ボックス（4点・rank）
    return [(SANRENPUKU, c) for c in trio_top_n(rank, 4)]


def _s2_quinella_top3(rank, probs):  # S2: 上位3頭ペア 馬連（3点・rank）
    return [(UMAREN, c) for c in quinella_top_n(rank, 3)]


def _s3_wide_top3(rank, probs):     # S3: 上位3頭ペア ワイド（3点・S2と同じ買い目/venue対照）
    return [(WIDE, c) for c in wide_top_n(rank, 3)]


def _s3b_wide_axis(rank, probs):    # S3b: ワイド 1位軸→2〜5位（4点・rank・1位の複勝圏信頼）
    return [(WIDE, c) for c in wide_axis_flow(rank, (1, 2, 3, 4))]


def _s4_trifecta_top2_reverse(rank, probs):  # S4: 三連単 1↔2位→3〜6位（8点・rank）
    return [(SANRENTAN, c) for c in trifecta_top2_reverse(rank, (2, 3, 4, 5))]


def _s5_trifecta_single_winner(rank, probs):  # S5: 三連単 1位固定→2・3着(2〜5位)順列（常時・rank）
    return [(SANRENTAN, c) for c in trifecta_single_winner(rank, 0, (1, 2, 3, 4))]


def _s6_trio_top5(rank, probs):     # S6: 三連複 上位5頭ボックス（10点・rank）
    return [(SANRENPUKU, c) for c in trio_top_n(rank, 5)]


def _s7_exacta_reverse(rank, probs):  # S7: 馬単 1↔2位折り返し（2点・rank）
    return [(UMATAN, c) for c in exacta_top2_reverse(rank)]


def _s8_exacta_axis(rank, probs):   # S8: 馬単 1位固定→2〜4位（3点・rank）
    return [(UMATAN, c) for c in exacta_single_winner(rank, (1, 2, 3))]


def _s9_trifecta_p1(rank, probs, threshold: float = 0.50):
    # S9: 三連単 1着固定→2・3着流し、ただし Sim1位の勝率 p1>=threshold のときだけ購入（12点/見送り）。
    # ⚠ Sim勝率が未校正だと閾値に意味がない→先に予測p1帯別の実勝率を確認すること。
    if not rank:
        return []
    p1 = float(probs.get(TANSHO, {}).get(int(rank[0]), 0.0))
    if p1 < threshold:
        return []
    return [(SANRENTAN, c) for c in trifecta_single_winner(rank, 0, (1, 2, 3, 4))]


def _j1_trio_joint4(rank, probs):   # J1: 三連複 同時確率上位4点（joint・S1と同点数対照）
    return [(SANRENPUKU, c) for c in joint_topk(probs, SANRENPUKU, 4)]


def _j2_trifecta_joint8(rank, probs):  # J2: 三連単 同時確率上位8点（joint・S4と同点数対照）
    return [(SANRENTAN, c) for c in joint_topk(probs, SANRENTAN, 8)]


def _j3_exacta_joint2(rank, probs):  # J3: 馬単 同時確率上位2点（joint・S7と同点数対照）
    return [(UMATAN, c) for c in joint_topk(probs, UMATAN, 2)]


def _s0_skip(rank, probs):          # S0: 何も買わない（帰無・取りこぼし基準）
    return []


STRATEGY_TEMPLATES = {
    "S0_skip": _s0_skip,
    "単勝1位": _s_tansho_top1,
    "複勝1位": _s_fukusho_top1,
    "S1_三連複box4": _s1_trio_top4,
    "S2_上位3頭ペア_馬連": _s2_quinella_top3,
    "S3_上位3頭ペア_ワイド": _s3_wide_top3,
    "S3b_ワイド1軸→2-5": _s3b_wide_axis,
    "S4_三連単1↔2→3-6": _s4_trifecta_top2_reverse,
    "S5_三連単1着固定流し": _s5_trifecta_single_winner,
    "S6_三連複box5": _s6_trio_top5,
    "S7_馬単1↔2": _s7_exacta_reverse,
    "S8_馬単1軸→2-4": _s8_exacta_axis,
    "S9_三連単1着固定_p1≥0.50": _s9_trifecta_p1,
    "J1_三連複joint4": _j1_trio_joint4,
    "J2_三連単joint8": _j2_trifecta_joint8,
    "J3_馬単joint2": _j3_exacta_joint2,
}

# rank↔joint の同点数対照ペア（「MC の順位依存構造を使う価値」の直接比較）。
RANK_JOINT_PAIRS = [
    ("S1_三連複box4", "J1_三連複joint4"),
    ("S4_三連単1↔2→3-6", "J2_三連単joint8"),
    ("S7_馬単1↔2", "J3_馬単joint2"),
]


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
    for bet_type, combo in strategy(rank, probs):
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


# 券種グループ（三連系の大量投資が全券種合算を支配するのを切り分けるため）。
BET_GROUP = {
    "tansho": "単複", "fukusho": "単複",
    "umaren": "馬連馬単ワイド", "umatan": "馬連馬単ワイド", "wide": "馬連馬単ワイド",
    "wakuren": "馬連馬単ワイド",
    "sanrenpuku": "三連複", "sanrentan": "三連単",
}
BET_GROUP_ORDER = ["単複", "馬連馬単ワイド", "三連複", "三連単"]


def settle_tickets_detailed(candidates, return_processor, unit: int = 1) -> list[tuple]:
    """各買い目を1点決済し (race_id, bet_type, stake, returned) の明細を返す（払戻表に無い点は除外）。

    `_backtest._settle_detailed` は race_id を落とすため、年別・レース単位 maxDD・除上位k払戻に
    使えるよう race_id を保持した明細版。個票の returned（1点の払戻）を後段の各指標が使う。
    """
    from src.simulation._betting_tickets import BettingTickets
    tickets = BettingTickets(return_processor)
    rows: list[tuple] = []
    for c in candidates:
        n_bets, stake, returned = tickets.settle_one(c.bet_type, c.race_id, c.combo, unit)
        if n_bets == 0:
            continue
        rows.append((str(c.race_id), c.bet_type, float(stake), float(returned)))
    return rows


def _max_drawdown(rows: list[tuple], race_order: list | None) -> float:
    """レース単位のポートフォリオ純収支(円)の最大ドローダウン。全戦略を同時運用した資金曲線。

    同一レースの全点を1ステップ（合算純収支）に束ね、race_order の時系列で累積する。
    race_order 無しは race_id 昇順（≈時系列）。
    """
    net_by_race: dict = {}
    for rid, _bt, stake, ret in rows:
        net_by_race[rid] = net_by_race.get(rid, 0.0) + (ret - stake)
    order = [r for r in (race_order or sorted(net_by_race)) if r in net_by_race]
    for r in net_by_race:                       # race_order に無い race も末尾に足す
        if r not in order:
            order.append(r)
    cum = peak = max_dd = 0.0
    for r in order:
        cum += net_by_race[r]
        peak = max(peak, cum)
        max_dd = max(max_dd, peak - cum)
    return max_dd


def portfolio_metrics(rows: list[tuple], *, race_order: list | None = None,
                      top_k: int = 5) -> dict:
    """明細(settle_tickets_detailed)→「全戦略を同時に全購入した仮想ポートフォリオ」のTOTAL指標。

    投資額加重 ROI=Σreturned/Σstake。除最大1/除上位k払戻・購入レース数・総点数・
    1レース平均投資・最大DD・年別TOTAL ROI・券種グループ別TOTAL を返す。
    """
    total_stake = sum(r[2] for r in rows)
    total_ret = sum(r[3] for r in rows)
    payouts = sorted((r[3] for r in rows), reverse=True)
    races = {r[0] for r in rows}
    groups: dict = {}
    for rid, bt, stake, ret in rows:
        g = BET_GROUP.get(bt, bt)
        d = groups.setdefault(g, {"stake": 0.0, "returned": 0.0, "n_bets": 0, "n_hits": 0,
                                  "max_return": 0.0})
        d["stake"] += stake
        d["returned"] += ret
        d["n_bets"] += 1
        d["n_hits"] += 1 if ret > 0 else 0
        d["max_return"] = max(d["max_return"], ret)
    yr: dict = {}
    for rid, _bt, stake, ret in rows:
        y = str(rid)[:4]
        if y.isdigit():
            a = yr.setdefault(y, {"stake": 0.0, "returned": 0.0})
            a["stake"] += stake
            a["returned"] += ret
    for g in groups.values():
        g["roi"] = g["returned"] / g["stake"] if g["stake"] else 0.0
    return {
        "n_tickets": len(rows), "n_races": len(races),
        "total_stake": total_stake, "total_return": total_ret,
        "profit": total_ret - total_stake,
        "roi": total_ret / total_stake if total_stake else 0.0,
        "roi_ex_top1": (total_ret - (payouts[0] if payouts else 0.0)) / total_stake
        if total_stake else 0.0,
        "roi_ex_top5": (total_ret - sum(payouts[:top_k])) / total_stake if total_stake else 0.0,
        "avg_stake_per_race": total_stake / len(races) if races else 0.0,
        "max_dd": _max_drawdown(rows, race_order),
        "by_year": {y: (a["returned"] / a["stake"] if a["stake"] else 0.0)
                    for y, a in sorted(yr.items())},
        "by_group": groups,
    }


def market_favorite(win_odds_by_race: dict) -> dict:
    """{race_id: {馬番: 単勝オッズ}} → {race_id: 1番人気馬番}（最小オッズ）。購入時点オッズで決める。

    リーク回避の要: 市場1番人気は **購入可能時点** の単勝オッズで決める（確定オッズは精算のみ）。
    """
    fav: dict = {}
    for rid, od in win_odds_by_race.items():
        valid = {int(u): float(o) for u, o in od.items() if o and float(o) > 0}
        if valid:
            fav[str(rid)] = min(valid, key=valid.get)
    return fav


def paired_delta_roi_ci(per_race_sim: dict, per_race_mkt: dict, *, n_boot: int = 2000,
                        alpha: float = 0.05, seed: int = 0) -> dict:
    """同一レース集合で ΔROI=ROI_sim−ROI_mkt をレース単位 paired bootstrap し 95%CI を返す。

    別々に bootstrap するより、同じレースを一緒に再標本化する方が検出力が高い（共変動を保持）。
    共通レースのみ対象。返す: {"delta","lo","hi","roi_sim","roi_mkt","n_races"}。
    """
    import numpy as np
    rids = [r for r in per_race_sim if r in per_race_mkt]
    n = len(rids)
    if n == 0:
        return {"delta": 0.0, "lo": 0.0, "hi": 0.0, "roi_sim": 0.0, "roi_mkt": 0.0, "n_races": 0}
    ss = np.array([per_race_sim[r]["stake"] for r in rids], float)
    rs = np.array([per_race_sim[r]["returned"] for r in rids], float)
    sm = np.array([per_race_mkt[r]["stake"] for r in rids], float)
    rm = np.array([per_race_mkt[r]["returned"] for r in rids], float)

    def _roi(a, b):
        return a.sum() / b.sum() if b.sum() else 0.0

    roi_s, roi_m = _roi(rs, ss), _roi(rm, sm)
    rng = np.random.default_rng(seed)
    deltas = np.empty(n_boot)
    for b in range(n_boot):
        idx = rng.integers(0, n, size=n)
        deltas[b] = _roi(rs[idx], ss[idx]) - _roi(rm[idx], sm[idx])
    lo, hi = np.quantile(deltas, [alpha / 2, 1 - alpha / 2])
    return {"delta": float(roi_s - roi_m), "lo": float(lo), "hi": float(hi),
            "roi_sim": float(roi_s), "roi_mkt": float(roi_m), "n_races": n}


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
