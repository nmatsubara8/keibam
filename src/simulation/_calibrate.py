"""EV 較正アーティファクト（γ,δ / r̂較正 / α,β）を OOS データから fit して永続化する。

ベンター(1994) の3つの後段補正を、モデルの**学習期間と重ならないホールドアウト**
（out-of-sample）で推定し JSON に保存する。保存物は `ExpectedValueBetPolicy` /
`run_backtest` に opt-in で読み込ませる（`place_exponents=/win_calibrator=/blend_weights=`）。

- **(γ, δ)** … べき乗補正 Harville の着位別指数。観測 1-2-3着の補正三連単尤度を最大化。
- **r̂ 較正** … 生勝率→実勝率の isotonic 写像（本命過小評価＝人気-穴バイアスの是正）。
- **(α, β)** … モデル勝率 × 公衆 implied 勝率の2段目ロジット合成（市場へ上乗せ）。

リーク注意（Benter §5）: いずれの推定も**モデルの学習年より後の年**で行うこと。in-sample
で fit すると等値写像・過学習評価になり、本番で退化する。CLI 側は `--years` で OOS を切る。

入力は Win ヘッドのモデルと featured フレーム（`着順` 列を持つ）。
`ExpectedValueScorePolicy.calc` で [馬番, prob, 単勝オッズ] を作り、`着順` を位置整合で
付与してレース単位の (勝率, 1-2-3着) / (生勝率, 勝敗) / (P_fund, P_public, winner) を組む。
"""

from __future__ import annotations

import dataclasses
import os
from typing import Mapping, Sequence

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols
from src.policies._blend import BlendRace
from src.policies._blend import BlendWeights
from src.policies._blend import fit_blend
from src.policies._blend import save_blend_weights
from src.policies._calibration import IsotonicCalibrator
from src.policies._calibration import fit_isotonic_calibrator
from src.policies._calibration import save_calibrator
from src.policies._harville import PlaceExponents
from src.policies._harville import fit_place_exponents
from src.policies._harville import save_place_exponents
from src.policies._score_policy import CURRENT_ODDS
from src.policies._score_policy import PROB
from src.policies._score_policy import ExpectedValueScorePolicy

# 既定の保存先（models/ 配下）。takeout_calibration.json 等と同じ規約。
PLACE_EXPONENTS_FILE = "place_exponents.json"
WIN_CALIBRATOR_FILE = "win_calibrator.json"
BLEND_WEIGHTS_FILE = "blend_weights.json"


def place_exponents_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, PLACE_EXPONENTS_FILE)


def win_calibrator_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, WIN_CALIBRATOR_FILE)


def blend_weights_path(models_dir: str = "models") -> str:
    return os.path.join(models_dir, BLEND_WEIGHTS_FILE)


@dataclasses.dataclass
class CalibrationInputs:
    """fit 関数群に渡す中間データ（全アーティファクトで共有）。

    raw_probs/outcomes  … 較正用（生勝率と勝敗 0/1、各馬1行の平坦配列）
    place_races         … (γ,δ)用 [(win_probs, (1着,2着,3着)馬番)]
    blend_races         … (α,β)用 [(P_fund, P_public, winner馬番)]
    n_races             … 入力に使えたレース数（着順が揃ったもの）
    """

    raw_probs: np.ndarray
    outcomes: np.ndarray
    place_races: list[tuple[Mapping[int, float], tuple[int, int, int]]]
    blend_races: list[BlendRace]
    n_races: int


def _normalize(d: Mapping[int, float]) -> dict[int, float]:
    s = sum(d.values())
    return {k: v / s for k, v in d.items()} if s > 0 else dict(d)


def build_calibration_inputs(win_model, X: pd.DataFrame) -> CalibrationInputs:
    """Win モデルと featured X から較正アーティファクトの fit 入力を組み立てる。

    `ExpectedValueScorePolicy.calc` は行順を保つので、`着順`/`馬番` は位置で整合付与する。
    `着順` が欠損（取消等）の馬は較正サンプルから外す。1-2-3着が揃わないレースは
    place_races から、勝ち馬が無いレースは blend_races から除外する。
    """
    table = ExpectedValueScorePolicy.calc(win_model, X)  # index=race_id, [馬番, prob, 単勝オッズ]
    table = table.copy()
    table[ResultsCols.RANK] = pd.to_numeric(
        np.asarray(X[ResultsCols.RANK]), errors="coerce"
    )

    # 較正用（全馬・着順既知のもの）
    known = table[table[ResultsCols.RANK].notna()]
    raw_probs = known[PROB].to_numpy(dtype=float)
    outcomes = (known[ResultsCols.RANK].to_numpy(dtype=float) == 1.0).astype(float)

    place_races: list[tuple[Mapping[int, float], tuple[int, int, int]]] = []
    blend_races: list[BlendRace] = []
    n_races = 0
    for _, race in table.groupby(level=0, sort=False):
        race = race[race[ResultsCols.RANK].notna()]
        if race.empty:
            continue
        n_races += 1
        umaban = race[ResultsCols.UMABAN].astype(int).to_numpy()
        prob = race[PROB].to_numpy(dtype=float)
        odds = race[CURRENT_ODDS].to_numpy(dtype=float)
        rank = race[ResultsCols.RANK].to_numpy(dtype=float)

        win_probs = {int(u): float(p) for u, p in zip(umaban, prob, strict=False)}

        # (γ,δ): 着順で 1-2-3着の馬番を取り出す（3頭未満は除外）
        order = umaban[np.argsort(rank, kind="stable")]
        if len(order) >= 3:
            place_races.append((win_probs, (int(order[0]), int(order[1]), int(order[2]))))

        # (α,β): P_fund=モデル勝率(正規化), P_public=1/オッズ(正規化), winner=1着
        valid_odds = {
            int(u): 1.0 / float(o)
            for u, o in zip(umaban, odds, strict=False)
            if o and o > 0 and np.isfinite(o)
        }
        winners = umaban[rank == 1.0]
        if valid_odds and len(winners) == 1:
            p_fund = _normalize(win_probs)
            p_public = _normalize(valid_odds)
            blend_races.append((p_fund, p_public, int(winners[0])))

    return CalibrationInputs(
        raw_probs=raw_probs,
        outcomes=outcomes,
        place_races=place_races,
        blend_races=blend_races,
        n_races=n_races,
    )


def fit_and_save_place_exponents(
    inputs: CalibrationInputs,
    path: str,
    *,
    init: tuple[float, float] = (0.81, 0.65),
) -> PlaceExponents:
    """(γ, δ) を MLE して保存する。"""
    exp = fit_place_exponents(inputs.place_races, init=init)
    save_place_exponents(exp, path)
    return exp


def fit_and_save_calibrator(inputs: CalibrationInputs, path: str) -> IsotonicCalibrator:
    """r̂ の isotonic 較正写像を fit して保存する。"""
    cal = fit_isotonic_calibrator(inputs.raw_probs, inputs.outcomes)
    save_calibrator(cal, path)
    return cal


def fit_and_save_blend(
    inputs: CalibrationInputs,
    path: str,
    *,
    init: tuple[float, float] = (1.0, 1.0),
) -> BlendWeights:
    """合成重み (α, β) を MLE して保存する。"""
    w = fit_blend(inputs.blend_races, init=init)
    save_blend_weights(w, path)
    return w


def fit_all(
    win_model,
    X: pd.DataFrame,
    *,
    models_dir: str = "models",
    which: Sequence[str] = ("exponents", "calibrator", "blend"),
) -> dict:
    """指定アーティファクトをまとめて fit→保存し、結果サマリを返す。

    which は {"exponents","calibrator","blend"} の部分集合。CLI から渡す。
    """
    inputs = build_calibration_inputs(win_model, X)
    out: dict = {
        "n_races": inputs.n_races,
        "n_place_races": len(inputs.place_races),
        "n_blend_races": len(inputs.blend_races),
        "n_calib_samples": int(inputs.raw_probs.size),
    }
    if "exponents" in which:
        exp = fit_and_save_place_exponents(inputs, place_exponents_path(models_dir))
        out["exponents"] = {"gamma": exp.gamma, "delta": exp.delta,
                            "path": place_exponents_path(models_dir)}
    if "calibrator" in which:
        cal = fit_and_save_calibrator(inputs, win_calibrator_path(models_dir))
        out["calibrator"] = {"n_thresholds": len(cal.x),
                            "path": win_calibrator_path(models_dir)}
    if "blend" in which:
        w = fit_and_save_blend(inputs, blend_weights_path(models_dir))
        out["blend"] = {"alpha": w.alpha, "beta": w.beta,
                       "path": blend_weights_path(models_dir)}
    return out
