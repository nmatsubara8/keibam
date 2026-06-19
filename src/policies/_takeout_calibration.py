"""払戻実績から券種別の実効控除率（takeout）を逆算する較正ロジック。

Harville 推定オッズは ``(1 - takeout) / P(combo)`` で計算するが、控除率 takeout は
従来固定値（既定 0.2）だった。実際の控除率は券種ごとに異なり（単勝/複勝≈20%,
馬連系≈22.5%, 三連系≈25%）、さらに Harville 自体に系統的バイアスがある。

本モジュールは「的中組合せの確定オッズ（払戻金/100）」を真値アンカーとして、

    1 - t_eff = actual_odds(winner) × P_harville(winner)

を多数レースで集計し、券種別の実効控除率 t_eff を推定する。市場が概ね効率的で
Harville が組合せ空間全体で総和 1 の確率分布である限り、的中組（真の着順分布から
1 点サンプル）について ``actual_odds × P_harville(winner)`` の期待値は ``1 - t_true`` に
一致する（Σ_c P_harville(c) = 1）。得られた t_eff は実控除率に加えて Harville の
較正誤差も吸収するため、HistoricalOddsProvider に渡すと「的中組について平均的に
実績と整合する」推定オッズになる。

レイヤ: policies（ドメイン）。I/O は持たず、入力（tansho 勝率・払戻 lookup）は
呼び出し側が組み立てる。永続化は薄い JSON ヘルパに留める。
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from typing import Mapping
from typing import Sequence

from src.constants._bet_types import BetType
from src.constants._bet_types import combo_key
from src.policies import _harville as harville

logger = logging.getLogger(__name__)

TAKEOUT_CALIBRATION_FILENAME = "takeout_calibration.json"

# Harville が組合せ確率を計算できる券種のみ較正可能（枠連は枠単位で非対応）。
# 単勝は実オッズをそのまま返すため takeout 較正の対象外。
CALIBRATABLE_BET_TYPES = (
    BetType.FUKUSHO,
    BetType.UMAREN,
    BetType.UMATAN,
    BetType.WIDE,
    BetType.SANRENPUKU,
    BetType.SANRENTAN,
)

# JRA 公称の控除率（較正サンプルが不足する券種のフォールバック）。
NOMINAL_TAKEOUT: dict[str, float] = {
    BetType.TANSHO: 0.20,
    BetType.FUKUSHO: 0.20,
    BetType.WAKUREN: 0.225,
    BetType.UMAREN: 0.225,
    BetType.UMATAN: 0.225,
    BetType.WIDE: 0.225,
    BetType.SANRENPUKU: 0.25,
    BetType.SANRENTAN: 0.25,
}

DEFAULT_TAKEOUT = 0.2
MIN_SAMPLES = 20  # この件数未満の券種は公称控除率へフォールバック
TRIM_FRAC = 0.1  # 集計時に上下から切り捨てる割合（外れ値耐性）
Z_95 = 1.959964  # 95% 信頼区間の正規分位点


def parse_win_combo(win) -> list[int] | None:
    """払戻テーブルの win セル（int / "1-2" / "1→2" / list）を馬番リストにする。"""
    if win is None:
        return None
    if isinstance(win, (list, tuple)):
        try:
            return [int(x) for x in win]
        except (TypeError, ValueError):
            return None
    s = str(win).strip()
    if not s or s == "0":
        return None
    try:
        return [int(p) for p in re.split(r"[-→]", s) if p != ""]
    except ValueError:
        return None


def payout_lookup_from_return_processor(return_processor) -> dict:
    """払戻テーブルから {(race_id, bet_type, combo_key): 確定オッズ} を作る（純粋計算）。

    的中組合せの **払戻金 / 100 = その組合せの確定オッズ**。過去レースの連系確定オッズが
    オッズページから取得できなくても、払戻データ（ingest 済み・全 8 券種）から
    「当たった組合せの確定オッズ」は確実に得られる。

    `return_processor` は ``preprocessed_data: {bet_type: DataFrame}`` を持つダックタイプ
    （src.preprocessing._return_processor.ReturnProcessor）。policies → preprocessing の
    import を避けるため型は受け取らず属性のみ参照する。
    """
    out: dict = {}
    data = getattr(return_processor, "preprocessed_data", {}) or {}
    for bet_type, table in data.items():
        if table is None or getattr(table, "empty", True):
            continue
        n_win = sum(1 for c in table.columns if str(c).startswith("win_"))
        for race_id, row in table.iterrows():
            for i in range(n_win):
                combo = parse_win_combo(row.get(f"win_{i}", 0))
                ret = row.get(f"return_{i}", 0)
                if not combo or not ret:
                    continue
                try:
                    key = (str(race_id), bet_type, combo_key(bet_type, combo))
                    out[key] = round(float(ret) / 100.0, 1)
                except (TypeError, ValueError):
                    continue
    return out


def tansho_odds_by_race_from_table(table, umaban_col: str, odds_col: str) -> dict:
    """results/featured テーブルから {race_id: {馬番: 単勝オッズ}} を構築する（純粋計算）。

    snapshots は fetch-final-odds 済みレースに限られるが、results は ingest 済み全レースを
    カバーするため、較正のサンプル数を最大化できる。race_id は index 前提。
    """
    out: dict = {}
    if table is None or getattr(table, "empty", True):
        return out
    if umaban_col not in table.columns or odds_col not in table.columns:
        return out
    for race_id, race_df in table.groupby(level=0):
        race_map: dict = {}
        for umaban, odds in zip(race_df[umaban_col], race_df[odds_col], strict=False):
            try:
                u = int(umaban)
                o = float(odds)
            except (TypeError, ValueError):
                continue
            if o > 0:
                race_map[u] = o
        if len(race_map) >= 2:
            out[str(race_id)] = race_map
    return out


def _parse_combo_key(combo_str: str) -> list[int]:
    """``"3-7-11"`` 形式の combo_key を馬番リストへ復元する。"""
    out: list[int] = []
    for part in combo_str.split("-"):
        if part == "":
            continue
        try:
            out.append(int(part))
        except ValueError:
            return []
    return out


def _trimmed_stats(values: Sequence[float], trim: float = TRIM_FRAC) -> tuple[float, float, int]:
    """上下 trim 割合を切り捨てた (平均, 不偏標準偏差, 残存件数) を返す。

    平均は外れ値に頑健な点推定。標準偏差は CI 用（残存サンプルの標本標準偏差）。
    """
    s = sorted(values)
    k = int(len(s) * trim)
    core = s[k: len(s) - k] if len(s) - 2 * k >= 1 else s
    n = len(core)
    mean = sum(core) / n
    if n > 1:
        var = sum((x - mean) ** 2 for x in core) / (n - 1)
        std = var ** 0.5
    else:
        std = 0.0
    return mean, std, n


def _trimmed_mean(values: Sequence[float], trim: float = TRIM_FRAC) -> float:
    """上下 trim 割合を切り捨てた平均（不偏寄りかつ外れ値に頑健）。"""
    mean, _, _ = _trimmed_stats(values, trim)
    return mean


def winner_return_rate(
    tansho_odds_by_race: Mapping,
    race_id: str,
    bet_type: str,
    combo: Sequence[int],
    actual_odds: float,
) -> float | None:
    """的中組について ``actual_odds × P_harville(winner)`` （≈ 1 - t_eff）を返す。

    単勝オッズが無いレース・馬番欠落・確率 0 は ``None``（較正に使えない）。
    """
    odds_map = tansho_odds_by_race.get(race_id) or tansho_odds_by_race.get(str(race_id))
    if not odds_map or len(odds_map) < 2:
        return None
    implied = {int(u): 1.0 / float(o) for u, o in odds_map.items() if o and float(o) > 0}
    if len(implied) < 2:
        return None
    try:
        win_probs = harville.normalize(implied)
        prob = harville.combo_probability(bet_type, win_probs, list(combo))
    except (KeyError, ValueError):
        return None
    if prob <= 0:
        return None
    return float(actual_odds) * float(prob)


def calibrate_takeout_from_payouts(
    tansho_odds_by_race: Mapping,
    payout_lookup: Mapping,
    *,
    min_samples: int = MIN_SAMPLES,
) -> dict[str, dict]:
    """的中組の払戻実績から券種別の実効控除率を逆算する。

    Parameters
    ----------
    tansho_odds_by_race : {race_id: {馬番: 単勝オッズ}}
        Harville 勝率の元。`final_odds_lookup_from_payouts` と同じ race_id 体系であること。
    payout_lookup : {(race_id, bet_type, combo_key): 確定オッズ}
        `app._odds_compare.final_odds_lookup_from_payouts` の出力（払戻金/100）。

    Returns
    -------
    {bet_type: {"takeout", "n", "source", "ci_low", "ci_high"}}
        較正対象の全券種を含む。サンプル不足は公称控除率にフォールバック（CI は None）。
        ci_low/ci_high は takeout の 95% 信頼区間（trimmed mean の標準誤差による正規近似）。
        注: 複勝/ワイドは 1 レースに複数サンプルあり race 内相関を無視するため、CI 幅は
        やや楽観的（実際よりわずかに狭い）になりうる。n が大きいので実用上は十分に狭い。
    """
    rates: dict[str, list[float]] = {bt: [] for bt in CALIBRATABLE_BET_TYPES}
    for (race_id, bet_type, combo_str), actual in payout_lookup.items():
        if bet_type not in rates:
            continue
        if not actual or float(actual) <= 0:
            continue
        combo = _parse_combo_key(str(combo_str))
        if not combo:
            continue
        r = winner_return_rate(
            tansho_odds_by_race, str(race_id), bet_type, combo, float(actual)
        )
        if r is not None and r > 0:
            rates[bet_type].append(r)

    def _clip(x: float) -> float:
        return min(max(x, 0.0), 0.95)

    result: dict[str, dict] = {}
    for bt in CALIBRATABLE_BET_TYPES:
        samples = rates[bt]
        nominal = NOMINAL_TAKEOUT.get(bt, DEFAULT_TAKEOUT)
        if len(samples) >= min_samples:
            mean, std, n_kept = _trimmed_stats(samples)
            se = std / (n_kept ** 0.5) if n_kept > 0 else 0.0
            t = _clip(1.0 - mean)
            # mean が大きいほど takeout は小さい → CI の上下が反転する
            ci_low = _clip(1.0 - (mean + Z_95 * se))
            ci_high = _clip(1.0 - (mean - Z_95 * se))
            result[bt] = {
                "takeout": round(t, 4), "n": len(samples), "source": "calibrated",
                "ci_low": round(ci_low, 4), "ci_high": round(ci_high, 4),
            }
        else:
            result[bt] = {
                "takeout": nominal, "n": len(samples), "source": "nominal",
                "ci_low": None, "ci_high": None,
            }
    return result


def takeout_map(calibration: Mapping[str, Mapping]) -> dict[str, float]:
    """較正結果 → ``{bet_type: takeout}`` （HistoricalOddsProvider に渡す形）。"""
    return {bt: float(info["takeout"]) for bt, info in calibration.items()}


# ---------------------------------------------------------------------------
# 永続化（models/takeout_calibration.json）
# ---------------------------------------------------------------------------


def takeout_calibration_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, TAKEOUT_CALIBRATION_FILENAME)


def save_takeout_calibration(calibration: Mapping[str, Mapping], path: str) -> None:
    """較正スナップショットを JSON 追記保存する（同日付は置換、履歴をリストで保持）。"""
    now = dt.datetime.now().isoformat()
    day = now[:10]
    existing = [
        r for r in load_takeout_calibration_records(path) if r.get("saved_at", "")[:10] != day
    ]
    snapshot = {
        "saved_at": now,
        "calibration": {bt: dict(info) for bt, info in calibration.items()},
    }
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        json.dump(existing + [snapshot], f, ensure_ascii=False, indent=2)
    logger.info("[takeout_calibration] %s: %d 券種を保存", path, len(calibration))


def load_takeout_calibration_records(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)


def latest_takeout_map(path: str) -> dict[str, float]:
    """保存済み最新スナップショットの ``{bet_type: takeout}`` を返す（無ければ空）。"""
    records = load_takeout_calibration_records(path)
    if not records:
        return {}
    latest = max(records, key=lambda r: r.get("saved_at", ""))
    return takeout_map(latest.get("calibration", {}))
