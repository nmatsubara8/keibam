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
    # 検証済み単勝戦略の運用パラメータ。コード既定は無害（フィルタ無効）にして
    # 既存呼び出しを変えず、実値は config.yaml 側で指定する。
    #   max_odds: これ超のオッズ（人気薄）を除外。kelly_backtest で 3–15倍に
    #     エッジが集中し、≤15倍で 2022–2026 の全年度 回収率1.9–2.1 を確認。
    #   tansho_ev_threshold: 単勝EV下限の上書き（None=既定 BetThresholds。検証値=1.1）。
    max_odds: float = float("inf")
    tansho_ev_threshold: float | None = None
    # オッズ力学モデルの予測確定オッズで EV を計算する（odds_watch の最新予測を使用。
    # 予測が無いレース/馬は現在オッズへ自動フォールバック）
    use_predicted_odds: bool = False
    # EV 較正アーティファクト（calibrate-ev の出力）をライブ選定に適用する。
    # models/{place_exponents,win_calibrator,blend_weights}.json を読み補正Harville/r̂較正/
    # 市場合成を有効化（ファイルが無い項目は自動で従来挙動へフォールバック）。既定 False=無効。
    # 注意: これらは OOS で fit したものを使うこと（in-sample は退化。Benter §5）。
    use_ev_calibration: bool = False
    # 初出走（データ無し）馬に公衆 implied 勝率を割り当てる（ベンター §3）。featured の
    # career_starts==0/NaN を初出走と判定し、初出走のみのレースは選定から除外する。既定 False。
    use_unratable_fallback: bool = False
    # 自己購入のオッズ低下（プール影響）でケリー stake を上限する（芦谷/ベンター）。run_prediction に
    # pool_by_race（復元プール）を渡したときのみ作動。最適ベットは小さくなる。既定 False。
    use_pool_impact: bool = False
    # 安全装置（損失ストップ / kill switch）
    kill_switch_enabled: bool = True       # 当日実現損失が上限超で推奨/記録を停止
    max_daily_loss_ratio: float = 0.3      # 当日実現損失が bankroll*この比率を超えたら停止
    initial_bankroll: float = 100000.0     # 実効 bankroll の基準（= initial + 累積純益）

    def __post_init__(self) -> None:
        if self.operation_mode not in VALID_MODES:
            raise ValueError(f"operation_mode は {VALID_MODES} のいずれか: {self.operation_mode}")
        if not (0.0 < self.max_daily_loss_ratio <= 1.0):
            raise ValueError(
                f"max_daily_loss_ratio は 0 < r <= 1: {self.max_daily_loss_ratio}"
            )
        if self.max_odds <= 0:
            raise ValueError(f"max_odds は正の値: {self.max_odds}")
        if self.tansho_ev_threshold is not None and self.tansho_ev_threshold <= 0:
            raise ValueError(f"tansho_ev_threshold は正の値か None: {self.tansho_ev_threshold}")

    @classmethod
    def from_dict(cls, data: dict) -> "OperationConfig":
        fields = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in (data or {}).items() if k in fields})

    @classmethod
    def load(cls, path: str) -> "OperationConfig":
        import yaml  # type: ignore[import-untyped]

        with open(path, encoding="utf-8") as f:
            return cls.from_dict(yaml.safe_load(f) or {})
