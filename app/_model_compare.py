"""モデル比較シミュレーションのヘルパ（UI から呼ぶ計算ロジック）。

選択した複数のモデルバージョンについて、同一条件（期間・馬券種・閾値）で
バックテストを実行し、回収率・的中率等の指標と資金推移を返す。
Streamlit 依存は持たない（テスト可能な純粋計算 + ファイル読込のみ）。
"""

from __future__ import annotations

import logging

import pandas as pd

from src.constants._local_paths import LocalPaths

logger = logging.getLogger(__name__)

# UI で選択できる馬券種 → (BetPolicy クラス名, Simulator の action キー)
# 全 8 券種に対応（枠連・馬単・三連単を含む）。枠連 BOX は score_table の
# wakuban_flag 列で枠単位に集約するため、StdScorePolicy 等 _calc 由来の
# score_table（wakuban_flag を含む）が必要。
BET_POLICY_CHOICES = {
    "単勝": ("BetPolicyTansho", "tansho"),
    "複勝": ("BetPolicyFukusho", "fukusho"),
    "複勝本命(損失最小)": ("BetPolicyFukushoHonmei", "fukusho"),
    "枠連BOX": ("BetPolicyWakurenBox", "wakuren"),
    "馬連BOX": ("BetPolicyUmarenBox", "umaren"),
    "馬単BOX": ("BetPolicyUmatanBox", "umatan"),
    "ワイドBOX": ("BetPolicyWideBox", "wide"),
    "三連複BOX": ("BetPolicySanrenpukuBox", "sanrenpuku"),
    "三連単BOX": ("BetPolicySanrentanBox", "sanrentan"),
}


def recent_race_slice(featured: pd.DataFrame, test_frac: float = 0.2) -> pd.DataFrame:
    """featured_data の日付末尾 test_frac 分のレースを返す（DataSplitter の test 分割相当）。

    モデルの学習に使われていない直近期間で比較するのが目的。date 列が無い場合は
    race_id 降順の末尾を使う。
    """
    if featured is None or featured.empty:
        return pd.DataFrame()
    if "date" in featured.columns:
        order = featured["date"].sort_values().index.unique()
    else:
        order = featured.index.sort_values().unique()
    n_test = max(1, int(len(order) * test_frac))
    test_ids = order[-n_test:]
    return featured.loc[featured.index.isin(set(test_ids))]


def simulate_model(
    ai,
    featured_slice: pd.DataFrame,
    bet_label: str,
    threshold: float,
    return_processor=None,
) -> tuple[dict, pd.DataFrame, dict]:
    """1 モデルのバックテストを実行する。

    Parameters
    ----------
    return_processor : 払戻テーブル供給（DI）。None なら LocalPaths から読み込む
        （テスト時は合成払戻テーブルを注入できる）。

    Returns
    -------
    (summary, per_race, diag) :
        summary  — 回収率・的中率・シャープレシオ・最大DD 等（summarize_returns 出力）。
        per_race — レース毎の bet_amount / return_amount / hit_or_not。
        diag     — 診断情報（n_matched_races: 閾値を超えて賭けたレース数,
                   n_covered_races: そのうち払戻テーブルにデータがあったレース数）。
                   結果が空のとき「閾値が高い」のか「払戻データ欠損」かを区別する。
    """
    import src.policies as policies
    from src.constants._bet_types import BetType
    from src.policies._score_policy import StdScorePolicy
    from src.preprocessing._return_processor import ReturnProcessor
    from src.simulation._simulator import Simulator

    policy_cls_name, action_key = BET_POLICY_CHOICES[bet_label]
    policy_cls = getattr(policies, policy_cls_name)

    score_table = ai.calc_score(featured_slice, StdScorePolicy)
    actions = policy_cls.judge(score_table, threshold)
    # featured_data の race_id は str、払戻テーブル（ReturnProcessor）の index は
    # int64 のため、payout 照合キーを int に正規化する（race_id は常に数値）。
    actions = {int(race_id): bets for race_id, bets in actions.items()}

    if return_processor is None:
        return_processor = ReturnProcessor(LocalPaths.RAW_RETURN_TABLES_PATH)

    # 診断: 閾値を超えて賭けたレースのうち、払戻テーブルに存在する割合を測る。
    # action_key を該当 BetType にマップして、その馬券種の払戻テーブルで照合する。
    _action_to_bet_type = {
        "tansho": BetType.TANSHO, "fukusho": BetType.FUKUSHO,
        "wakuren": BetType.WAKUREN, "umaren": BetType.UMAREN, "umatan": BetType.UMATAN,
        "wide": BetType.WIDE, "sanrenpuku": BetType.SANRENPUKU, "sanrentan": BetType.SANRENTAN,
    }
    bet_type = _action_to_bet_type.get(action_key, BetType.TANSHO)
    payout_index = set(int(x) for x in return_processor.preprocessed_data[bet_type].index)
    matched_ids = set(actions.keys())
    covered_ids = matched_ids & payout_index
    diag = {
        "n_matched_races": len(matched_ids),
        "n_covered_races": len(covered_ids),
    }

    simulator = Simulator(return_processor)
    per_race = simulator.calc_returns_per_race(actions)
    summary = simulator.calc_returns(actions)
    return summary, per_race, diag


def cumulative_profit(per_race: pd.DataFrame) -> pd.Series:
    """レース毎の損益を時系列（race_id 昇順）で累積した資金推移を返す。"""
    if per_race is None or per_race.empty:
        return pd.Series(dtype=float)
    ordered = per_race.sort_index()
    profit = ordered["return_amount"] - ordered["bet_amount"]
    return profit.cumsum()


def align_profit_curves(profits: dict[str, pd.Series]) -> pd.DataFrame:
    """モデル別の累積損益を共通の race_id 軸に整列する。

    モデルごとに賭けたレースが異なるため、和集合のレース軸に reindex して
    前方補完（賭けなかったレースは直前の累積値を維持）する。先頭の欠損は 0。
    """
    if not profits:
        return pd.DataFrame()
    df = pd.DataFrame(profits).sort_index()
    return df.ffill().fillna(0.0)


def comparison_table(results: dict[str, dict]) -> pd.DataFrame:
    """{version: summary} を比較表に整形する（回収率降順）。"""
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results).T
    sort_col = "return_rate" if "return_rate" in df.columns else df.columns[0]
    return df.sort_values(sort_col, ascending=False)
