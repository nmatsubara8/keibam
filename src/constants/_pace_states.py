"""ペース潜在状態 z の正準定義（Mixture-PL と P(z) 予測器の共有定数）。

constants に置く理由: 消費者が policies（_mixture_pl）と preprocessing（_pace_state）に
またがるため、上方依存（preprocessing→policies）を作らず最下層で共有する。
状態数は 3 で固定（Slow/Normal/Fast・公開データでこれ以上は学習できない）。
"""
from __future__ import annotations

PACE_STATES: tuple[str, ...] = ("slow", "normal", "fast")

# 脚質の正準4分類（β(style, z) 表の行キー）
STYLES: tuple[str, ...] = ("nige", "senko", "sashi", "oikomi")
