"""JRDB 順序モデルの共通ハーネス（レース読込・place強度較正・座標降下fit）。

trifecta_jrdb_test / multibet_roi_test / trifecta_roi_test / chokuzen_signal_scan が
各々複製していた load()/_place_probs/fit_coef を一元化する。市場=単勝 Harville を帰無に、
JRDB の展開/能力指数で 2/3着の place 強度を softmax 調整する（coef≡0 で素の Harville に退化）。

- ``load_races``   : SED(着順+確定単勝)×KYI(指数)[×TYB(直前)] → (完全順序レース列, 使用signal名)
- ``place_probs_from_signals`` : q を JRDB signal で調整した place 強度分布（旧 _place_probs）
- ``trifecta_nll`` : 実 top3 の位置特化トリフェクタ NLL（旧 _tri_nll）
- ``fit_signal_coef`` : train の trifecta NLL 最小の係数を座標降下で fit（旧 fit_coef）
"""
from __future__ import annotations

import glob

import numpy as np
import pandas as pd

from src.jrdb._parser import parse
from src.policies._harville import prob_trifecta, prob_trifecta_place_strength
from src.policies._market_residual import market_probs

# 2/3着の順序に効く JRDB 前日KYI signal（全て賭け前に入手可能・forward-safe）。
# chokuzen_signal_scan.py の OOS走査で、goal+ichi だけの ΔNLL−0.0097 が、下記フルsuiteで
# joint ΔNLL−0.0223（CI95(-0.036,-0.010)・placebo消失）へ約2倍に拡大することを確認。
# joint非ゼロ: goal/ichi/idm/ten/pace/agari/gekiso(激走)/manken(万券)/joushoudo(上昇度)。
# start_idx/deokure_rate は単独★（位置と直交する出遅れ次元）だが joint では goal と冗長。
# 注意: ΔNLL は較正であって ROI ではない。ROI は multibet_roi_test の CI下限>1.0 で最終判定。
KYI_SIGNALS = ("goal_juni", "ichi_idx", "idm", "ten_idx", "pace_idx", "agari_idx",
               "gekiso_idx", "manken_idx", "joushoudo")
# TYB 直前(発走15分前)の直交情報。paddock_idx=物理評価（純粋直交）/ odds_idx=直前オッズ由来
# （市場変動を含むためリーク気味・要注意）。実運用では 15 分前に取得可能＝賭け前に使える。
TYB_SIGNALS = ("paddock_idx", "odds_idx")
# 中央競馬の場コード（01札幌〜10小倉）。race_id の 5-6 桁目。
CENTRAL_CODES = frozenset(f"{i:02d}" for i in range(1, 11))
_COEF_GRID = np.arange(-0.3, 0.31, 0.05)


def load_races(jrdb_dir: str, central_only: bool = True, with_tyb: bool = False, *,
               signals: tuple[str, ...] | None = None) -> tuple[list[dict], tuple[str, ...]]:
    """SED(着順+確定単勝)×KYI(展開指数)[×TYB(直前)] → (完全順序レース列, 使用signal名)。

    signals=None で KYI_SIGNALS を使う。任意の KYI 列名タプルを渡せば signal を差し替え可能
    （chokuzen_signal_scan の候補走査で使用）。各レース dict は rid/q/top3/sig/waku を持つ。
    """
    base_signals = tuple(signals) if signals is not None else KYI_SIGNALS
    sed_files = sorted(glob.glob(f"{jrdb_dir}/SED*.txt"))
    kyi_files = sorted(glob.glob(f"{jrdb_dir}/KYI*.txt"))
    if not sed_files or not kyi_files:
        raise SystemExit(
            f"JRDB txt が見つかりません（{jrdb_dir}/SED*.txt={len(sed_files)} "
            f"KYI*.txt={len(kyi_files)}）。--jrdb-dir を展開済みディレクトリに。"
            "\nアーカイブは: python -c \"from src.jrdb._extract import extract_dir; "
            "extract_dir('<lzh/zipのフォルダ>','<展開先>')\" で .txt 化してください。")
    sed = pd.concat([parse(f, "SED")[["race_id", "umaban", "kakutei_tansho", "chakujun"]]
                     for f in sed_files], ignore_index=True)
    kyi = pd.concat([parse(f, "KYI")[["race_id", "umaban", "wakuban", *base_signals]]
                     for f in kyi_files], ignore_index=True)
    m = sed.merge(kyi, on=["race_id", "umaban"], how="inner")
    used_signals = base_signals
    if with_tyb:
        tyb_files = sorted(glob.glob(f"{jrdb_dir}/TYB*.txt"))
        if tyb_files:
            tyb = pd.concat([parse(f, "TYB")[["race_id", "umaban", *TYB_SIGNALS]]
                             for f in tyb_files], ignore_index=True)
            m = m.merge(tyb, on=["race_id", "umaban"], how="left")
            used_signals = base_signals + TYB_SIGNALS
    m = m.dropna(subset=["kakutei_tansho", "chakujun"])
    m = m[m["kakutei_tansho"] > 1.0]
    if central_only:
        m = m[m["race_id"].astype(str).str[4:6].isin(CENTRAL_CODES)]
    races = []
    for rid, g in m.groupby(m["race_id"].astype(str)):
        g = g.dropna(subset=["chakujun"])
        if len(g) < 6:
            continue
        q = market_probs({int(u): float(o) for u, o in
                          zip(g["umaban"], g["kakutei_tansho"], strict=False)})
        if len(q) < 6:
            continue
        top3 = [int(x) for x in g.sort_values("chakujun")["umaban"].head(3)]
        if len(set(top3)) < 3 or any(t not in q for t in top3):
            continue
        sig: dict[int, dict[str, float]] = {}
        for c in used_signals:
            v = pd.to_numeric(g[c], errors="coerce").fillna(0.0)
            z = (v - v.mean()) / (v.std() + 1e-6)
            for u, zz in zip(g["umaban"], z, strict=False):
                sig.setdefault(int(u), {})[c] = float(zz) if pd.notna(zz) else 0.0
        waku = {int(u): int(w) for u, w in
                zip(g["umaban"], pd.to_numeric(g["wakuban"], errors="coerce"), strict=False)
                if pd.notna(w) and int(u) in q}
        races.append({"rid": str(rid), "q": q, "top3": tuple(top3), "sig": sig, "waku": waku})
    races.sort(key=lambda r: r["rid"])
    return races, used_signals


def place_probs_from_signals(q: dict, sig: dict, coef: dict) -> dict:
    """JRDB 調整の place 強度 softmax（coef≡0 で q に一致＝帰無）。"""
    if not coef:
        return q
    s = {u: np.log(max(q[u], 1e-9)) + sum(coef[c] * sig.get(u, {}).get(c, 0.0) for c in coef)
         for u in q}
    mx = max(s.values())
    ex = {u: np.exp(v - mx) for u, v in s.items()}
    z = sum(ex.values())
    return {u: v / z for u, v in ex.items()}


def trifecta_nll(race: dict, coef: dict | None) -> float:
    """実 top3 の位置特化トリフェクタ NLL（coef=None/空で素の Harville）。"""
    if not coef:
        p = prob_trifecta(race["q"], *race["top3"])
    else:
        plc = place_probs_from_signals(race["q"], race["sig"], coef)
        p = prob_trifecta_place_strength(race["q"], plc, *race["top3"])
    return -np.log(max(p, 1e-12))


def fit_signal_coef(train: list[dict], signals: tuple[str, ...],
                    grid=None, passes: int = 3) -> dict:
    """train の trifecta NLL 最小の係数を座標降下で fit（任意個の signal 対応）。"""
    if grid is None:
        grid = _COEF_GRID
    coef = {s: 0.0 for s in signals}
    for _ in range(passes):
        for f in signals:
            best_v, best_n = coef[f], float(np.mean([trifecta_nll(r, coef) for r in train]))
            for v in grid:
                c2 = dict(coef)
                c2[f] = float(v)
                n = float(np.mean([trifecta_nll(r, c2) for r in train]))
                if n < best_n:
                    best_n, best_v = n, float(v)
            coef[f] = best_v
    return coef
