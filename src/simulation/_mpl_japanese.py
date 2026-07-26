"""matplotlib の日本語フォント設定ヘルパー。

matplotlib の既定フォント（DejaVu Sans 等）は日本語グリフを持たないため、
日本語ラベルが □（豆腐）に文字化けする。本モジュールは利用可能な日本語
フォントを検出して rcParams に設定する。一度だけ実行すれば以降のすべての
プロットに適用される。

優先順位:
1. japanize-matplotlib がインストールされていればそれを使う
2. font_manager から既知の日本語フォントを検出して設定
3. いずれも無ければ警告し、英数字のみ正常表示（日本語は豆腐のまま）

日本語フォントが無い環境では以下でインストールできる:
    sudo apt-get install -y fonts-ipafont-gothic fonts-noto-cjk
    # またはユーザー領域に: pip install japanize-matplotlib
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# 検出を試みる日本語フォント名（一般的な順）
_CANDIDATES = [
    "IPAexGothic",
    "IPAGothic",
    "IPAPGothic",
    "Noto Sans CJK JP",
    "Noto Sans JP",
    "TakaoGothic",
    "VL Gothic",
    "Meiryo",
    "Hiragino Sans",
    "Yu Gothic",
    "MS Gothic",
]

_configured = False


def setup_japanese_font() -> None:
    """matplotlib に日本語表示可能なフォントを設定する（冪等）。"""
    global _configured
    if _configured:
        return

    import matplotlib

    # マイナス記号が日本語フォントで豆腐化するのを防ぐ
    matplotlib.rcParams["axes.unicode_minus"] = False

    # 1. japanize-matplotlib があれば最優先で使用
    try:
        import japanize_matplotlib  # noqa: F401

        _configured = True
        return
    except Exception:
        pass

    # 2. font_manager から候補フォントを検出
    try:
        from matplotlib import font_manager

        available = {f.name for f in font_manager.fontManager.ttflist}
        for name in _CANDIDATES:
            if name in available:
                matplotlib.rcParams["font.family"] = name
                _configured = True
                logger.info("matplotlib 日本語フォントを設定: %s", name)
                return
    except Exception as e:  # noqa: BLE001
        logger.warning("日本語フォント検出に失敗: %s", e)

    # 3. 見つからない: 警告のみ（英数字は正常表示される）
    logger.warning(
        "日本語フォントが見つかりません。グラフの日本語が文字化けする場合は "
        "`sudo apt-get install -y fonts-ipafont-gothic` 等でインストールしてください。"
    )
    _configured = True  # 再試行しても無駄なので確定
