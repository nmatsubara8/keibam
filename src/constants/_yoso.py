"""予想印（pro yoso）関連の定数。

netkeiba の予想印 API で「無料予想家」を判定する goods_kbn の集合。スクレイパ
（preparing._yoso_marks）とコンセンサス集約（preprocessing._yoso_consensus）の双方が
参照するため、最下層の constants に置いてレイヤ逆流を避ける。
"""

from __future__ import annotations

# 無料予想家の goods_kbn（プレミアム以外）。free_only フィルタ・無料印数の集計に使う。
FREE_GOODS_KBN: frozenset[str] = frozenset({"no1_free", "umai_free", "umai_buy"})
