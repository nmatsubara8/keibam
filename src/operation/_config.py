"""運用設定（Configuration）。

投票運用モードやリスク・資金パラメータを一元管理し、コードから分離する。
当面は advisory（人間が実行）。将来 full_auto へは設定変更のみで移行できる。
"""

from __future__ import annotations

import dataclasses

# 運用モード
ADVISORY = "advisory"  # 推奨表示のみ。実行は人間（履歴記録のみ）
SEMI_AUTO = "semi_auto"  # 購入リストを出力（発注は人間）
FULL_AUTO = "full_auto"  # 自動発注（既定無効・将来）

VALID_MODES = (ADVISORY, SEMI_AUTO, FULL_AUTO)


@dataclasses.dataclass(frozen=True)
class OperationConfig:
    operation_mode: str = ADVISORY
    bankroll: float = 100000.0
    kelly_fraction_ratio: float = 0.5
    per_bet_cap_ratio: float = 0.05
    max_daily_ratio: float = 1.0

    def __post_init__(self) -> None:
        if self.operation_mode not in VALID_MODES:
            raise ValueError(f"operation_mode は {VALID_MODES} のいずれか: {self.operation_mode}")

    @classmethod
    def from_dict(cls, data: dict) -> "OperationConfig":
        fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in (data or {}).items() if k in fields})

    @classmethod
    def load(cls, path: str) -> "OperationConfig":
        import yaml

        with open(path, encoding="utf-8") as f:
            return cls.from_dict(yaml.safe_load(f) or {})
