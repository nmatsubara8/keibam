"""人物（騎手/調教師/馬主/生産者）ID の正準化（純粋）。

`preparing._person_yearly`（取得・URL 構築）と `preprocessing._data_merger`（結合キー整形）の
双方が使うため、共有の純粋関数を preprocessing 層に置く（preparing からは再 export）。
"""

from __future__ import annotations

import re
from typing import Any


def canon_person_id(entity_type: str, eid: Any) -> str:
    """人物 ID を正準化する。jockey/trainer は netkeiba の 5 桁ゼロ埋め（例 1009→'01009'）。

    results の jockey_id は int64（先頭ゼロ落ち）なので、URL 用にも結合キー用にもこの形で
    揃える。owner/breeder は ID 形式が異なる（英数）ため数字のときのみゼロ埋めし、他は素通し。
    """
    s = re.sub(r"\.0$", "", str(eid).strip())
    if entity_type in ("jockey", "trainer") and s.isdigit():
        return s.zfill(5)
    return s
