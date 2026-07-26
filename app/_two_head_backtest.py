"""2ヘッド（Place/Win）+ 確定オッズの券種別 EV バックテスト（UI 用アダプタ）。

CLI ``run_pipeline backtest``（:func:`src.simulation._backtest.run_backtest`）と同じ
評価を Streamlit から呼べるようにする薄いアダプタ。重い評価ロジックは
``src.simulation._backtest`` に委譲し、ここでは入力（年フィルタ・確定オッズ・券種絞り
込み）の解決と、結果（:class:`BetTypeStats` 群）→ 表示用 DataFrame への整形だけを行う。

Streamlit に依存しない純粋関数で構成し、単体テスト可能にする（page 側は描画のみ）。

リーク注意（CLI と同じ）: 確定オッズ・市場歪み特徴は発走前の確定値で結果に非依存だが、
モデルの学習期間に評価期間を含めると楽観バイアスになる。評価年は学習年と重ねないこと
（:func:`filter_featured_by_years` で評価期間を絞る）。
"""

from __future__ import annotations

import pandas as pd

from src.constants._bet_types import BetType
from src.simulation._backtest import default_thresholds
from src.simulation._backtest import run_backtest

# 表示用の日本語ラベルと表示順（report の order に合わせる）。
BET_TYPE_LABELS: dict[str, str] = {
    BetType.TANSHO: "単勝",
    BetType.FUKUSHO: "複勝",
    BetType.WAKUREN: "枠連",
    BetType.UMAREN: "馬連",
    BetType.UMATAN: "馬単",
    BetType.WIDE: "ワイド",
    BetType.SANRENPUKU: "三連複",
    BetType.SANRENTAN: "三連単",
}

# default_thresholds() が対象とする券種（枠連は Harville 非対応で含まれない）。
DISPLAY_ORDER: list[str] = [
    BetType.TANSHO, BetType.FUKUSHO, BetType.UMAREN, BetType.UMATAN,
    BetType.WIDE, BetType.SANRENPUKU, BetType.SANRENTAN,
]


def available_years(featured: pd.DataFrame) -> list[str]:
    """featured_data の race_id（index 先頭 4 桁）から評価可能な年を新しい順で返す。"""
    if featured is None or featured.empty:
        return []
    years = featured.index.astype(str).str[:4]
    uniq = sorted({y for y in years if y.isdigit() and len(y) == 4}, reverse=True)
    return uniq


def filter_featured_by_years(featured: pd.DataFrame, years) -> pd.DataFrame:
    """評価対象 featured を年（race_id 先頭 4 桁）で絞り込む。years 空なら無加工。"""
    if not years:
        return featured
    yset = {str(y) for y in years}
    rid = featured.index.astype(str)
    return featured[rid.str[:4].isin(yset)]


def selectable_bet_types() -> list[str]:
    """券種絞り込みの選択肢（default_thresholds の対象券種）を表示順で返す。"""
    targets = set(default_thresholds().keys())
    return [bt for bt in DISPLAY_ORDER if bt in targets]


def resolve_thresholds(bet_types=None) -> dict:
    """評価する EV 閾値 dict を返す。bet_types 指定時はその券種だけに絞る。"""
    thresholds = default_thresholds()
    if bet_types:
        want = set(bet_types)
        thresholds = {k: v for k, v in thresholds.items() if k in want}
    return thresholds


def _stats_row(stats) -> dict:
    """BetTypeStats → 表示用 1 行（日本語ラベル・%・損益）。"""
    bt = stats.bet_type
    label = "全体" if bt == "ALL" else BET_TYPE_LABELS.get(bt, bt)
    return {
        "馬券種": label,
        "点数": stats.n_bets,
        "的中": stats.n_hits,
        "的中率": stats.hit_rate,
        "投票": stats.stake,
        "払戻": stats.returned,
        "損益": stats.profit,
        "回収率": stats.roi,
    }


def result_to_frame(result: dict) -> pd.DataFrame:
    """run_backtest の結果を券種別 + 全体行の DataFrame に整形する（表示順固定）。"""
    per = result.get("per_bet_type", {})
    rows = [_stats_row(s) for bt in DISPLAY_ORDER if (s := per.get(bt)) is not None]
    overall = result.get("overall")
    if overall is not None and rows:
        rows.append(_stats_row(overall))
    return pd.DataFrame(
        rows,
        columns=["馬券種", "点数", "的中", "的中率", "投票", "払戻", "損益", "回収率"],
    )


def run_two_head_backtest(
    place_ai,
    featured: pd.DataFrame,
    return_processor,
    *,
    win_ai=None,
    final_odds_lookup=None,
    bet_types=None,
    years=None,
    takeout: float = 0.2,
) -> dict:
    """UI 入力から券種別 EV バックテストを実行し、結果 dict に表示用 frame を添える。

    ``place_ai`` / ``win_ai`` は KeibaAI（``effective_model`` を持つ）。CLI ``_backtest``
    と同じく Place を主、Win を連系 Harville 用の勝率供給として渡す。

    Returns
    -------
    {
      ...run_backtest の戻り値（per_bet_type / overall / n_races / n_candidates）,
      "frame": pd.DataFrame,   # 表示用（券種別 + 全体行）
    }
    """
    target = filter_featured_by_years(featured, years)
    thresholds = resolve_thresholds(bet_types)
    result = run_backtest(
        place_ai.effective_model,
        target,
        return_processor,
        win_model=win_ai.effective_model if win_ai is not None else None,
        final_odds_lookup=final_odds_lookup,
        thresholds=thresholds,
        takeout=takeout,
    )
    result["frame"] = result_to_frame(result)
    return result
