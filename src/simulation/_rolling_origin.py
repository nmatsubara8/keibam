"""Rolling-origin 評価 — 時間方向リークを**構造的に**遮断する検証ハーネス。

競馬モデル最大の失敗源は未来情報の混入で、特に「較正パラメータが未来を見る」事故
（例: 2024年のレースを、2025年までのデータで fit した γ/δ・β・較正器で評価）は
placebo では検出できない。本ハーネスは

    〜2019 学習 → 2020 評価 → 〜2020 学習 → 2021 評価 → 〜2021 学習 → 2022 評価 → …

の前進 origin で、**fit 関数にはテスト年より過去のレースしか物理的に渡さない**。
Step1〜5 の全較正対象（残差ヘッド・γ/δ・β表・フィルタ超パラ・P(z)予測器）を
fit_fn の中で作る限り、未来見は構造的に起こり得ない（規約でなく機構で保証）。

fit_fn(train_races) -> params（任意のオブジェクト。学習不要なら None を返す）
prob_fn(params, race) -> {馬番: 勝率}
の2関数を モデルごとに渡す。評価は _model_compare.compare_models（ΔNLL/ΔECE/Bootstrap/
LRT/VOI）へ全テストレースをプールして流し、fold 別サマリも返す。純粋計算のみ。
"""
from __future__ import annotations

from typing import Callable, Mapping, Sequence

import numpy as np

from src.simulation._model_compare import compare_models
from src.simulation._model_compare import race_nll


def race_year(race: Mapping) -> int | None:
    """レース辞書から年度を得る。優先: "year" → "date"（先頭4桁）→ "race_id"（先頭4桁）。"""
    y = race.get("year")
    if y is not None:
        return int(y)
    for k in ("date", "race_id"):
        v = race.get(k)
        if v is not None:
            s = str(v)[:4]
            if s.isdigit():
                return int(s)
    return None


def rolling_origin_folds(
    races: Sequence[Mapping], *, min_train_years: int = 3
) -> list[tuple[list, list, int]]:
    """レース列を年度で並べ、(train=過去全年, test=当年, 年) の前進 fold 列を作る。

    テスト年は「先頭から min_train_years 年を学習に確保した後」の各年。train には
    **テスト年より前の全レースのみ**入る（expanding window・ここがリーク遮断の本体）。
    """
    by_year: dict[int, list] = {}
    for r in races:
        y = race_year(r)
        if y is not None:
            by_year.setdefault(y, []).append(r)
    years = sorted(by_year)
    folds: list[tuple[list, list, int]] = []
    for i in range(min_train_years, len(years)):
        test_year = years[i]
        train = [r for y in years[:i] for r in by_year[y]]
        folds.append((train, by_year[test_year], test_year))
    return folds


def rolling_origin_compare(
    races: Sequence[Mapping],
    fit_baseline: Callable[[Sequence[Mapping]], object],
    prob_baseline: Callable[[object, Mapping], dict[int, float]],
    fit_challenger: Callable[[Sequence[Mapping]], object],
    prob_challenger: Callable[[object, Mapping], dict[int, float]],
    *,
    min_train_years: int = 3,
    k_extra_params: int = 0,
    n_boot: int = 1000,
    seed: int = 0,
) -> dict:
    """Rolling-origin で 2 モデルを比較する（fit は各 fold の過去データのみで実行）。

    各 fold で fit_*(train) → params を作り、テスト年のレースに束ねてから、全テスト
    レースをプールして compare_models に流す（Bootstrap/LRT は全 rolling-OOS 標本で実施）。
    fold 別の ΔNLL も返すので「特定年だけ効く」不安定性も見える。

    Returns: {"pooled": compare_models の結果, "folds": [{year, n, d_nll} ...],
              "n_folds": int}
    """
    folds = rolling_origin_folds(races, min_train_years=min_train_years)
    annotated: list[dict] = []
    fold_rows: list[dict] = []
    for train, test, year in folds:
        pb = fit_baseline(train)
        pc = fit_challenger(train)
        rows = [
            {**r, "_pb": pb, "_pc": pc} for r in test if r.get("winner") is not None
        ]
        annotated.extend(rows)
        # fold 別 ΔNLL（安定性診断用の軽量サマリ）
        d = [
            race_nll(prob_challenger(pc, r), r["winner"])
            - race_nll(prob_baseline(pb, r), r["winner"])
            for r in rows
        ]
        fold_rows.append(
            {"year": year, "n": len(rows),
             "d_nll": float(np.mean(d)) if d else float("nan")}
        )

    pooled = compare_models(
        annotated,
        lambda r: prob_baseline(r["_pb"], r),
        lambda r: prob_challenger(r["_pc"], r),
        k_extra_params=k_extra_params,
        n_boot=n_boot,
        seed=seed,
    )
    return {"pooled": pooled, "folds": fold_rows, "n_folds": len(folds)}
