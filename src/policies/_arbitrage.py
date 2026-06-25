"""パリミュチュエルのプール逆算と裁定（lock）判定 — 芦谷(2012) の手法。

出典: 芦谷政浩 (2012)「日本の公営競馬における『競馬必勝法』の具体例」国民経済雑誌 205(6).

日本の公営競馬のオッズは券種ごとに
    ``オッズ = base + (S / s) × factor``   （地方: base=0.1, factor=0.738 / JRA単勝 factor=0.788）
で決まり、小数第2位以下は切り捨てられる（S=同種総売上枚数, s=当該馬券売上枚数）。
この式から:

1. **総投票数 S（＝プールサイズ ≒ 出来高 V_t）を公開オッズから逆算できる**
   → 「総投票量は観測不能」というオッズ力学モデルの前提を**解消**する（κ/流動性に使える）。
2. **自己購入によるオッズ低下を厳密に計算できる**（EV/バックテストのプール影響モデリング）。
3. **裁定指標 Σ_a C_a < 1**（各馬の単勝を最安合成した費用の和）で、リスクフリー lock の有無を判定。

純粋関数のみ（IO 非依存・constants 以外に非依存）。地方競馬は薄いプールゆえ lock が出やすく、
JRA は大プールでほぼ効率的（裁定は稀）だが、**プール逆算とオッズ影響は JRA でも有用**。
"""

from __future__ import annotations

from typing import Iterable, Mapping, Sequence

# パリミュチュエル・オッズ式の係数
ODDS_BASE = 0.1
FACTOR_CHIHO = 0.738       # 地方（控除率 ~26.2%）
FACTOR_JRA_WIN = 0.788     # JRA 単勝（控除率 ~21.2%）


def cost_per_unit_payout(odds: float) -> float:
    """的中時に払戻 ¥1 を得るための購入金額（= 1/odds）。odds<=0 は無限大扱い。"""
    return 1.0 / odds if odds > 0 else float("inf")


def odds_of(pool_total: float, ticket_count: float, *, base: float = ODDS_BASE,
            factor: float = FACTOR_CHIHO, truncate: bool = True) -> float:
    """売上枚数 (S, s) からオッズを計算する（既定で小数第2位切り捨て）。

    truncate=False は切り捨て前の生値（プール逆算アルゴリズムの検証・近似用）。
    """
    if ticket_count <= 0:
        return float("inf")
    raw = base + (pool_total / ticket_count) * factor
    return (int(raw * 10) / 10.0) if truncate else raw  # 小数第2位以下切り捨て


def count_from_odds(odds: float, pool_total: float, *, base: float = ODDS_BASE,
                    factor: float = FACTOR_CHIHO) -> float:
    """オッズと総売上 S から当該馬券の売上枚数 s = S·factor/(odds-base) を求める。"""
    denom = odds - base
    if denom <= 0:
        return float("inf")
    return pool_total * factor / denom


def recover_pool_total(
    odds_values: Iterable[float],
    *,
    base: float = ODDS_BASE,
    factor: float = FACTOR_CHIHO,
    max_count: int = 1000,
    tol: float = 0.5,
) -> int | None:
    """券種内の複数オッズから総売上枚数 S を逆算する（芦谷 2012 §3.2）。

    最大オッズ（＝最小売上）の馬券の売上枚数 n を 1,2,… と仮定すると
    ``S = n·(odds_max-base)/factor`` が定まる。各 n の S について
    ``s_i = S·factor/(odds_i-base)`` が整数にどれだけ近いかを評価し、最も整合する S を返す。
    観測された全馬券は売上≥1（オッズが付いている）＝ n≥1 なので小さな偽 S を自然に排除する。
    切り捨て誤差があるため近似（厳密オッズなら厳密一致）。見つからなければ None。
    """
    odds = sorted((o for o in odds_values if o > base), reverse=True)
    if not odds:
        return None
    ratios = [factor / (o - base) for o in odds]  # s_i = S * ratio_i
    r_max = ratios[0]  # 最大オッズ＝最小売上の馬券

    def frac_error(s: int) -> float:
        return sum(abs(s * r - round(s * r)) for r in ratios)  # 整数からの絶対距離の和

    best_s, best_err = None, float("inf")
    for n in range(1, max_count + 1):
        s = round(n / r_max)  # s_max = S·r_max = n → S = n / r_max
        if s < 1:
            continue
        e = frac_error(s)
        if e < best_err - 1e-12:
            best_err, best_s = e, s
            if e < tol:
                return s
    return best_s


def odds_after_purchase(
    odds: float, pool_total: float, ticket_count: float, added: int,
    *, base: float = ODDS_BASE, factor: float = FACTOR_CHIHO, truncate: bool = True,
) -> float:
    """この馬券を added 枚買い増した後のオッズ（同種総売上にも added を加算）。

    truncate=False は EV モデリング用の生値（1桁切り捨ての tick ノイズを避ける）。
    """
    return odds_of(pool_total + added, ticket_count + added, base=base, factor=factor, truncate=truncate)


def effective_odds(
    odds: float, pool_total: float, added_tickets: int,
    *, base: float = ODDS_BASE, factor: float = FACTOR_CHIHO,
) -> float:
    """自分が added_tickets 枚買った後の確定オッズ（＝実際に受け取る配当倍率）。

    パリミュチュエルでは払戻は確定オッズ基準なので、k 枚買うと全 k 枚がこの低下後の
    オッズで払い戻される。現在オッズとプール S から当該馬券の枚数 s を逆算して適用する。
    """
    s = count_from_odds(odds, pool_total, base=base, factor=factor)
    if s == float("inf"):
        return odds
    # EV モデリング用に生値（切り捨てなし）を使う
    return odds_after_purchase(odds, pool_total, s, added_tickets, base=base, factor=factor, truncate=False)


def ev_with_impact(
    prob: float, odds: float, pool_total: float, added_tickets: int,
    *, base: float = ODDS_BASE, factor: float = FACTOR_CHIHO,
) -> float:
    """自己購入のオッズ低下を織り込んだ期待値 EV = prob·確定オッズ − 1。

    added_tickets を増やすほど確定オッズが下がり EV も下がる（プール影響）。
    """
    eo = effective_odds(odds, pool_total, added_tickets, base=base, factor=factor)
    return prob * eo - 1.0


def max_profit_bet(
    prob: float, odds: float, pool_total: float,
    *, base: float = ODDS_BASE, factor: float = FACTOR_CHIHO, max_tickets: int = 100_000,
) -> tuple[int, float]:
    """期待利益（= k·(prob·確定オッズ(k) − 1)）を最大化する購入枚数 k と利益（枚単位）。

    オッズは k とともに低下するため、正の EV でも最適購入枚数は有限（芦谷/ベンターのプール影響）。
    EV が最初から負なら (0, 0.0)。利益は 1 枚=1 単位（×100円で円換算）。
    """
    if ev_with_impact(prob, odds, pool_total, 1, base=base, factor=factor) <= 0:
        return 0, 0.0
    best_k, best_profit = 0, 0.0
    k = 1
    while k <= max_tickets:
        eo = effective_odds(odds, pool_total, k, base=base, factor=factor)
        profit = k * (prob * eo - 1.0)
        if profit > best_profit:
            best_profit, best_k = profit, k
        elif profit < best_profit:
            break  # 単峰（k とともにオッズ単調低下）なので下降に転じたら終了
        k += 1
    return best_k, best_profit


def min_win_cost(
    horse: int,
    win_odds: Mapping[int, float],
    *,
    exacta_odds: Mapping[tuple[int, int], float] | None = None,
    trifecta_odds: Mapping[tuple[int, int, int], float] | None = None,
    horses: Sequence[int] | None = None,
) -> float:
    """馬 horse が1着になったとき払戻 ¥1 を保証する**最安**購入費用 C_a（芦谷 2012 §3.3）。

    候補: 単勝直接 / 連単「horse 総流し」(Σ_b 1/odds(horse→b)) / 三連単「horse 総流し」。
    各合成法は全相手を網羅できる場合のみ採用（欠損があればその方法はスキップ）。
    """
    field = list(horses) if horses is not None else list(win_odds.keys())
    others = [h for h in field if h != horse]
    candidates: list[float] = []

    if horse in win_odds:
        candidates.append(cost_per_unit_payout(win_odds[horse]))

    if exacta_odds is not None:
        pairs = [(horse, b) for b in others]
        if all(p in exacta_odds for p in pairs):
            candidates.append(sum(cost_per_unit_payout(exacta_odds[p]) for p in pairs))

    if trifecta_odds is not None:
        triples = [(horse, b, c) for b in others for c in others if c != b]
        if triples and all(t in trifecta_odds for t in triples):
            candidates.append(sum(cost_per_unit_payout(trifecta_odds[t]) for t in triples))

    return min(candidates) if candidates else float("inf")


def arbitrage_indicator(
    horses: Sequence[int],
    win_odds: Mapping[int, float],
    *,
    exacta_odds: Mapping[tuple[int, int], float] | None = None,
    trifecta_odds: Mapping[tuple[int, int, int], float] | None = None,
) -> float:
    """裁定指標 A = Σ_a C_a（各馬の最安単勝合成費用の和）。

    A < 1 なら「どの馬が1着でも ¥1 を A 円で確保できる」＝**リスクフリー lock が存在**
    （最小購入単位・オッズ変化を無視した必要条件）。保証収益率 ≒ 1/A。A>=1 は裁定余地なし。
    """
    return sum(
        min_win_cost(a, win_odds, exacta_odds=exacta_odds,
                     trifecta_odds=trifecta_odds, horses=horses)
        for a in horses
    )


def has_arbitrage(indicator: float) -> bool:
    """裁定指標が 1 未満なら lock 候補（最小単位・オッズ変化の検証は別途必要）。"""
    return indicator < 1.0
