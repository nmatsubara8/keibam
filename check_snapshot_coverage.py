"""締切前スナップショットオッズの実在確認。

過去レースのバックテスト/単勝エッジを「締切前オッズ」で再評価できるかを判定するため、
蓄積済み odds_snapshots のフェーズ分布・レース数・featured レースとの重なりを出す。

締切前フェーズ = t5 / t10 / thirty_min（30分前）。これらが featured レースと十分に
重なっていなければ、確定オッズの代わりに締切前オッズで再評価することはできない。

実行: python check_snapshot_coverage.py
"""

from __future__ import annotations

import logging
from collections import Counter

logger = logging.getLogger(__name__)

# 締切前（確定オッズでない）フェーズ
_PRECLOSE = {"t5", "t10", "thirty_min", "just_before"}


def _db_count() -> int:
    try:
        from src.storage import RawDataRepo

        repo = RawDataRepo()
        if repo.has_rows("raw_odds_snapshots"):
            return len(repo.read("raw_odds_snapshots"))
    except Exception as e:  # noqa: BLE001
        logger.warning("DB 読込失敗: %s", e)
    return 0


def main() -> None:
    from src.constants._logging_config import setup_logging

    setup_logging()
    print("=" * 64)
    print("締切前スナップショットオッズ カバレッジ診断")
    print("=" * 64)

    import os

    from src.constants._local_paths import LocalPaths
    from src.preparing.odds_scheduler import load_snapshots

    path = LocalPaths.RAW_ODDS_SNAPSHOT_PATH
    snaps = load_snapshots(path) if os.path.exists(path) else []
    print(f"\n■ odds_snapshots（{path}）")
    print(f"  pickle スナップショット数: {len(snaps)}")
    print(f"  DB raw_odds_snapshots 行数: {_db_count()}")

    if not snaps:
        print("\n→ スナップショットが空です。締切前オッズは未取得。")
        print("  過去レースの締切前オッズは netkeiba から遡って取得できません"
              "（ライブ odds_watch を将来に向けて回した分だけ蓄積されます）。")
        print("=" * 64)
        return

    phases = Counter(s.phase for s in snaps)
    print("\n■ フェーズ分布（締切までの残り時間帯）")
    for ph, c in phases.most_common():
        tag = "  ← 締切前" if ph in _PRECLOSE else ("  ← 確定オッズ代理" if ph == "t0" else "")
        print(f"  {ph:<14} {c} 件{tag}")

    races_all = {str(s.race_id) for s in snaps}
    races_preclose = {str(s.race_id) for s in snaps if s.phase in _PRECLOSE}
    print("\n■ レース数")
    print(f"  スナップショットのある全レース: {len(races_all)}")
    print(f"  締切前フェーズを持つレース  : {len(races_preclose)}")

    # featured（バックテスト対象）との重なり
    try:
        from app._model_eval import load_featured_data

        featured = load_featured_data()
        if featured is not None and not featured.empty:
            feat_races = {str(r) for r in featured.index.unique()}
            print("\n■ featured（バックテスト対象）との重なり")
            print(f"  featured レース数: {len(feat_races)}")
            print(f"  うち締切前オッズあり: {len(feat_races & races_preclose)} レース")
    except Exception as e:  # noqa: BLE001
        print(f"  featured 読込失敗: {e}")

    print("\n判定:")
    if len(races_preclose) == 0:
        print("  締切前フェーズのスナップショットが 0 → 締切前オッズでの再評価は不可。")
    else:
        print(f"  締切前オッズが {len(races_preclose)} レース分あり。featured との重なり次第で"
              "その範囲のみ締切前オッズ評価が可能。")
    print("=" * 64)


if __name__ == "__main__":
    main()
