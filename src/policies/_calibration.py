"""勝率 r̂ の確率較正レイヤ（レース内正規化 + isotonic 回帰）。

ベンター/エッジ診断で観測した系統的ミスキャリブレーション（本命を過小評価・人気薄を
過大評価＝人気-穴バイアス）を、単調な isotonic 写像で是正する。
較正後にレース内で Σ=1 へ再正規化して Harville/EV に渡せる勝率にする。

EV 判断（r̂ − p_mkt や r̂·O − 1）は r̂ が較正済みであることを前提にするため、較正は
合成（_blend）・補正 Harville（_harville）の前段の基礎レイヤ。**較正は out-of-sample で
fit すること**（in-sample だと楽観的な等値写像になりやすい）。

予測時は scikit-learn 不要（閾値の np.interp で再現）。fit 時のみ sklearn を遅延 import。
"""

from __future__ import annotations

import dataclasses

import numpy as np


@dataclasses.dataclass(frozen=True)
class IsotonicCalibrator:
    """単調（非減少）較正写像。x=生勝率の閾値, y=較正値（ともに昇順）。

    predict は閾値間を線形補間（= isotonic の段階関数の連続化）。sklearn 非依存。
    """

    x: tuple[float, ...]
    y: tuple[float, ...]

    def predict(self, raw_probs) -> np.ndarray:
        arr = np.asarray(raw_probs, dtype=float)
        if not self.x:
            return arr
        return np.interp(arr, self.x, self.y)


def fit_isotonic_calibrator(raw_probs, outcomes) -> IsotonicCalibrator:
    """(生勝率, 勝敗 0/1) から isotonic 較正写像を学習する。

    生勝率に対する実勝率の単調回帰。例: モデルが本命に 0.185 を付けるが実勝率 0.317 なら
    0.185→~0.317 に持ち上げる。out_of_bounds=clip で範囲外は端点に丸める。
    """
    from sklearn.isotonic import IsotonicRegression

    x = np.asarray(raw_probs, dtype=float)
    y = np.asarray(outcomes, dtype=float)
    iso = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip", increasing=True)
    iso.fit(x, y)
    xt = np.asarray(iso.X_thresholds_, dtype=float)
    yt = np.asarray(iso.y_thresholds_, dtype=float)
    return IsotonicCalibrator(x=tuple(xt.tolist()), y=tuple(yt.tolist()))


def calibrate_within_race(race_ids, raw_probs, calibrator: IsotonicCalibrator) -> np.ndarray:
    """生勝率を較正し、レース内で Σ=1 に再正規化した配列を返す。

    race_ids / raw_probs は同じ長さの平坦配列（同一 race_id がレースの馬群）。
    """
    import pandas as pd

    cal = calibrator.predict(raw_probs)
    s = pd.Series(cal, dtype=float)
    keys = pd.Series(np.asarray(race_ids))
    normed = s.groupby(keys, sort=False).transform(
        lambda x: x / x.sum() if x.sum() > 0 else x
    )
    return normed.to_numpy()


def calibration_error(raw_probs, outcomes, n_bins: int = 10) -> float:
    """信頼性誤差（ECE 近似）: 分位ビンごとの |平均予測 − 実勝率| の加重平均。"""
    import pandas as pd

    df = pd.DataFrame({"p": np.asarray(raw_probs, float), "y": np.asarray(outcomes, float)})
    if df.empty:
        return 0.0
    ranks = df["p"].rank(method="first")
    df["_b"] = np.ceil(ranks / len(df) * n_bins).clip(1, n_bins).astype(int)
    g = df.groupby("_b")
    err = (g["p"].mean() - g["y"].mean()).abs()
    weight = g.size() / len(df)
    return float((err * weight).sum())


def save_calibrator(calibrator: IsotonicCalibrator, path: str) -> None:
    """較正写像（閾値 x, y）を JSON へ保存する。"""
    import json
    import os

    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"x": list(calibrator.x), "y": list(calibrator.y)}, f, ensure_ascii=False, indent=2)


def load_calibrator(path: str) -> IsotonicCalibrator | None:
    """保存済み較正写像を読み込む。無ければ None。"""
    import json
    import os

    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    return IsotonicCalibrator(x=tuple(d["x"]), y=tuple(d["y"]))
