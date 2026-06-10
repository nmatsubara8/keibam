"""馬券候補を表す不変 DTO。

EV 馬券選定（_bet_policy）→ 確信度付与（portfolio）→ 資金配分（portfolio）→ 決済（simulation）
の各レイヤ間で受け渡す共通の値オブジェクト。frozen にして途中改変による影響波及を防ぐ。
"""

import dataclasses


@dataclasses.dataclass(frozen=True)
class BetCandidate:
    race_id: object
    bet_type: str
    combo: tuple  # 馬番のタプル（単勝/複勝は1要素、馬連等は2要素…）
    probability: float  # モデル較正勝率から導いた的中確率（Harville）
    odds: float  # 払戻倍率
    expected_value: float  # = probability * odds
    confidence: float = 1.0  # 確信度 [0,1]（既定は中立=1.0、portfolio で上書き）
    stake: float = 0.0  # 配分された掛け金（portfolio で上書き）
