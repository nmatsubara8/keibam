"""ホールドアウト期間での券種別 EV バックテスト（回収率/的中率）。

2ヘッド予測（Place=複勝 / Win=単勝）→ 確定オッズ（StoredFinalOddsProvider）で
EV 選定 → 実払戻（ReturnProcessor）で決済、という一連を回し、馬券種ごとの
回収率・的中率・損益を集計する。

フラット 1 単位/点で**各候補をその組合せだけ**決済する（BettingTickets.place に
正確な combo を渡すと BOX 展開されず単一点になる）。Simulator の
judge→BOX 経路と異なり、EV 選定した買い目を過不足なく評価できる。

リーク注意: 確定オッズ・市場歪み特徴はいずれも発走前の確定値（結果に非依存）。
ただしモデルの学習期間にホールドアウト期間を含めると楽観バイアスになるため、
呼び出し側で「学習年 < 評価年」を保証すること（CLI の --years でフィルタ）。
"""

from __future__ import annotations

import dataclasses
from typing import Mapping, Sequence

import pandas as pd

from src.constants._bet_types import BetType
from src.constants._bet_thresholds import MIN_BETS_FOR_RELIABLE_STAT
from src.policies._thresholds import bet_threshold_map
from src.constants._results_cols import ResultsCols
from src.policies._bet_policy import ExpectedValueBetPolicy
from src.policies._odds_provider import AbstractOddsProvider
from src.policies._odds_provider import HistoricalOddsProvider
from src.policies._odds_provider import StoredFinalOddsProvider
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy
from src.preprocessing._return_processor import ReturnProcessor
from src.simulation._betting_tickets import BettingTickets


def default_thresholds() -> dict:
    """馬券種の EV 閾値 dict を返す（constants の単一ソースへ委譲）。"""
    return bet_threshold_map()


@dataclasses.dataclass
class BetTypeStats:
    """1 馬券種（または全体）の集計。stake/returned は単位券あたりの合算。"""

    bet_type: str
    n_bets: int = 0
    n_hits: int = 0
    stake: float = 0.0
    returned: float = 0.0
    max_return: float = 0.0  # 単一の的中の最大払戻（フロック=ファットテール検知用）

    @property
    def roi(self) -> float:
        """回収率 = 払戻合計 / 投票合計。"""
        return self.returned / self.stake if self.stake else 0.0

    @property
    def hit_rate(self) -> float:
        """的中率 = 的中点数 / 投票点数。"""
        return self.n_hits / self.n_bets if self.n_bets else 0.0

    @property
    def profit(self) -> float:
        return self.returned - self.stake

    @property
    def reliable(self) -> bool:
        """的中数が統計的に十分か（MIN_BETS_FOR_RELIABLE_STAT 以上）。

        的中が数件しかない券種の回収率は万馬券1本で激変する（ファットテール）ため、
        見出し指標に含めるべきでない。これが False の行は「参考値」。
        """
        return self.n_hits >= MIN_BETS_FOR_RELIABLE_STAT

    @property
    def roi_ex_top(self) -> float:
        """最大の単一払戻 1 点を除いた回収率（フロック感度）。

        roi との差が大きいほど「1本の万馬券に依存した回収率」であることを示す。
        """
        return (self.returned - self.max_return) / self.stake if self.stake else 0.0

    @property
    def top_share(self) -> float:
        """全払戻に占める最大単一払戻の割合（1.0 に近いほどフロック依存）。"""
        return self.max_return / self.returned if self.returned else 0.0

    def as_dict(self) -> dict:
        return {
            "bet_type": self.bet_type,
            "n_bets": self.n_bets,
            "n_hits": self.n_hits,
            "hit_rate": self.hit_rate,
            "stake": self.stake,
            "returned": self.returned,
            "profit": self.profit,
            "roi": self.roi,
            "reliable": self.reliable,
            "max_return": self.max_return,
            "roi_ex_top": self.roi_ex_top,
            "top_share": self.top_share,
        }


def settle_candidates(
    candidates: Sequence,
    return_processor: ReturnProcessor,
    unit: int = 1,
) -> dict[str, BetTypeStats]:
    """各 BetCandidate を「その組合せ 1 点だけ」決済して馬券種別に集計する。

    `BettingTickets.settle_one` を使い BOX 再展開を避ける（順序あり券種でも
    候補の順序どおり 1 点で評価）。払戻テーブルに該当レースが無い候補（n_bets=0）は
    集計から除外する（評価不能なものを 0 回収として混ぜると回収率が歪むため）。
    """
    tickets = BettingTickets(return_processor)
    stats: dict[str, BetTypeStats] = {}
    for cand in candidates:
        n_bets, bet_amount, returned = tickets.settle_one(
            cand.bet_type, cand.race_id, cand.combo, unit
        )
        if n_bets == 0:
            continue
        s = stats.setdefault(cand.bet_type, BetTypeStats(cand.bet_type))
        s.n_bets += n_bets
        s.stake += bet_amount
        s.returned += returned
        s.n_hits += 1 if returned > 0 else 0
        if returned > s.max_return:
            s.max_return = returned  # 最大単一払戻（ファットテール検知）
    return stats


def select_candidates(
    place_model,
    X: pd.DataFrame,
    *,
    win_model=None,
    final_odds_lookup: Mapping | None = None,
    thresholds: dict | None = None,
    bet_type_params: dict | None = None,
    place_exponents=None,
    win_calibrator=None,
    blend_weights=None,
    unratable_fallback=False,
    takeout=0.2,
) -> list:
    """2ヘッド予測 + 確定オッズで EV 選定し BetCandidate のリストを返す。

    run_prediction（app 層）の選定部だけを抜き出し、確信度/ケリー配分は行わない
    （バックテストはフラット 1 単位で回収率を測るため）。確定オッズが無い組合せは
    fallback（単勝からの Harville 推定）に委譲する。

    place_exponents / win_calibrator / blend_weights は EV ポリシーへの opt-in 配線
    （Benter べき乗補正 Harville / r̂ 較正 / 市場合成）。いずれも None で従来挙動を保持。
    unratable_fallback=True で初出走馬を公衆 implied 勝率に置換する（ベンター §3）。
    """
    thresholds = thresholds or default_thresholds()
    table = ExpectedValueScorePolicy.calc(place_model, X)
    win_table = (
        ExpectedValueScorePolicy.calc(win_model, X) if win_model is not None else None
    )

    fallback: AbstractOddsProvider = HistoricalOddsProvider.from_score_table(
        table, ResultsCols.UMABAN, CURRENT_ODDS, takeout=takeout
    )
    provider: AbstractOddsProvider = (
        StoredFinalOddsProvider(final_odds_lookup, fallback)
        if final_odds_lookup
        else fallback
    )
    policy = ExpectedValueBetPolicy(
        provider, thresholds=thresholds, bet_type_params=bet_type_params,
        place_exponents=place_exponents, win_calibrator=win_calibrator,
        blend_weights=blend_weights, unratable_fallback=unratable_fallback,
    )
    unratable_by_race = None
    if unratable_fallback:
        from src.policies._unratable import build_unratable_by_race

        unratable_by_race = build_unratable_by_race(X)
    place_cols = table[[ResultsCols.UMABAN, PROB]]
    if win_table is not None:
        return policy.select(
            win_table[[ResultsCols.UMABAN, PROB]], place_prob_table=place_cols,
            unratable_by_race=unratable_by_race,
        )
    return policy.select(place_cols, unratable_by_race=unratable_by_race)


def run_backtest(
    place_model,
    X: pd.DataFrame,
    return_processor: ReturnProcessor,
    *,
    win_model=None,
    final_odds_lookup: Mapping | None = None,
    thresholds: dict | None = None,
    bet_type_params: dict | None = None,
    place_exponents=None,
    win_calibrator=None,
    blend_weights=None,
    unratable_fallback=False,
    takeout=0.2,
    unit: int = 1,
) -> dict:
    """ホールドアウト X 上で EV 選定→決済し、券種別 + 全体の成績を返す。

    Returns
    -------
    {
      "per_bet_type": {bet_type: BetTypeStats},
      "overall": BetTypeStats,        # 全券種合算
      "n_races": int,                 # 候補が出たレース数
      "n_candidates": int,            # 選定された買い目総数
    }
    """
    candidates = select_candidates(
        place_model,
        X,
        win_model=win_model,
        final_odds_lookup=final_odds_lookup,
        thresholds=thresholds,
        bet_type_params=bet_type_params,
        place_exponents=place_exponents,
        win_calibrator=win_calibrator,
        blend_weights=blend_weights,
        unratable_fallback=unratable_fallback,
        takeout=takeout,
    )
    per = settle_candidates(candidates, return_processor, unit=unit)
    overall = BetTypeStats("ALL")
    reliable = BetTypeStats("ALL(信頼)")  # 的中≥閾値の券種のみ集計（万馬券フロックを除外）
    for s in per.values():
        overall.n_bets += s.n_bets
        overall.n_hits += s.n_hits
        overall.stake += s.stake
        overall.returned += s.returned
        overall.max_return = max(overall.max_return, s.max_return)
        if s.reliable:
            reliable.n_bets += s.n_bets
            reliable.n_hits += s.n_hits
            reliable.stake += s.stake
            reliable.returned += s.returned
            reliable.max_return = max(reliable.max_return, s.max_return)
    return {
        "per_bet_type": per,
        "overall": overall,
        "reliable_overall": reliable,
        "n_races": len({c.race_id for c in candidates}),
        "n_candidates": len(candidates),
    }


def format_report(result: dict) -> str:
    """run_backtest の結果を人が読める表に整形する。

    的中数が MIN_BETS_FOR_RELIABLE_STAT 未満の券種は「参考」印を付け、見出しは
    信頼できる券種のみ集計した ALL(信頼) を併記する。さらに最大単一払戻の占有率と
    それを除いた回収率を注記し、万馬券 1 本に依存した回収率（フロック）を可視化する。
    """
    lines = []
    header = (
        f"{'馬券種':<12}{'点数':>7}{'的中':>6}{'的中率':>8}{'投票':>9}"
        f"{'払戻':>11}{'回収率':>9}{'除外後':>9}  信頼"
    )
    lines.append(header)
    lines.append("-" * (len(header) + 2))
    order = [
        BetType.TANSHO, BetType.FUKUSHO, BetType.WAKUREN, BetType.UMAREN,
        BetType.UMATAN, BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
    ]
    per = result["per_bet_type"]

    def _row(label: str, s) -> str:
        mark = "✓" if s.reliable else f"参考(的中<{MIN_BETS_FOR_RELIABLE_STAT})"
        return (
            f"{label:<12}{s.n_bets:>7d}{s.n_hits:>6d}{s.hit_rate:>7.1%}"
            f"{s.stake:>9.0f}{s.returned:>11.1f}{s.roi:>8.1%}{s.roi_ex_top:>8.1%}  {mark}"
        )

    for bt in order:
        s = per.get(bt)
        if s is None:
            continue
        lines.append(_row(str(bt), s))

    o = result["overall"]
    rel = result.get("reliable_overall")
    lines.append("-" * (len(header) + 2))
    lines.append(_row("ALL", o))
    if rel is not None and rel.n_bets:
        lines.append(_row("ALL(信頼)", rel))

    note = (
        f"\nレース数={result['n_races']}  買い目総数={result['n_candidates']}"
        f"  損益={o.profit:+.1f}（単位券）"
    )
    # フロック注記: 最大単一払戻が全体に占める割合と、それを除いた回収率。
    if o.returned > 0 and o.max_return > 0:
        note += (
            f"\n⚠ 最大単一払戻={o.max_return:.0f}（全払戻の{o.top_share:.0%}）"
            f" → これを除くと全体回収率 {o.roi:.1%}→{o.roi_ex_top:.1%}"
        )
    note += (
        "\n※「回収率」は万馬券1本で激変しうる。的中<"
        f"{MIN_BETS_FOR_RELIABLE_STAT}の券種は参考値、ALL(信頼)と除外後ROIで判断すること。"
    )
    lines.append(note)
    return "\n".join(lines)
