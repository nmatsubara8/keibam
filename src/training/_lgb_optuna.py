"""LightGBM Optuna チューナー（lgb_o）の import を一元化する。

optuna v4.9 で `optuna.integration.lightgbm` は deprecated になり v6.0 で削除される。
後継の `optuna_integration.lightgbm`（optuna-integration[lightgbm]）を優先して import し、
未導入の環境（CI はテスト側で optuna.integration.lightgbm をスタブする）では
optuna 同梱の旧モジュールにフォールバックする。

利用側は `from src.training._lgb_optuna import lgb_o` とする。重い import を避けたい
場合は関数内で遅延 import すること（本モジュールの import 時に lightgbm がロードされる）。
"""

try:
    import optuna_integration.lightgbm as lgb_o
except ModuleNotFoundError:
    # optuna-integration 未導入。optuna v6.0 で削除されるため移行期間のみの後方互換。
    import optuna.integration.lightgbm as lgb_o  # type: ignore[no-redef]

__all__ = ["lgb_o"]
