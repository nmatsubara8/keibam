"""pytest 共有設定。

一部の test は「optuna 未インストール環境でも動く」よう `sys.modules.setdefault("optuna", stub)`
で optuna をスタブ化する。実 optuna がある環境では、それらのスタブ test が**先に import される**と
`sys.modules["optuna"]` を bare stub（`.logging` や `create_study` を持たない）で上書きし、後続の
optuna 利用 test（manji 最適化・券種別最適化 TPE 等）がテスト順（pytest-randomly）次第で
`module 'optuna' has no attribute 'logging'` で落ちる。

ここで実 optuna を**最初に**読み込んでおくと、各 test の `setdefault` は既存エントリを尊重して
no-op になり、実 optuna が全 test で共有される。optuna 未インストール環境では従来どおりスタブが担う。
"""
from __future__ import annotations

import warnings

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import optuna  # noqa: F401
        import optuna.integration.lightgbm  # noqa: F401
except Exception:  # noqa: BLE001 — optuna 未インストール環境ではスタブ test が従来どおり担う
    pass
