"""[後方互換シム] 旧・神モジュール modules.py の名前空間を維持する再 export。

実体は _raw_parsers.py（bin→raw パーサ）と _scrape_pages.py（取得ループ/HTML util）へ分割した。
既存の ``from src.preparing.modules import X`` / ``modules.X`` を壊さないよう、両モジュールの
全名（アンダースコア始まりや import 済みの time 等も含む）をこのモジュールへ複製する。
新規コードは分割後のモジュールを直接 import すること（このシムは将来撤去予定）。
"""

from src.preparing import _raw_parsers as _rp
from src.preparing import _scrape_pages as _sp

for _mod in (_rp, _sp):
    for _name in dir(_mod):
        if not _name.startswith("__"):
            globals()[_name] = getattr(_mod, _name)

del _mod, _name, _rp, _sp
