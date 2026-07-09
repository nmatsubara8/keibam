"""JRDB↔netkeiba キー橋渡しの検証。JRDBファイル(KYI/SED/SKB)を読み、変換した
race_id/horse_id が、取得済み featured にどれだけ一致するかを実測する。

これが繋がれば「JRDBの馬(特記=前走不利/出遅れ・基準オッズ)」を我々の featured の
馬に貼れる＝取り込みが成立する。繋がらなければ world_id/採番の対応を再検討する。

使い方:
  python scripts/jrdb_bridge_check.py /path/to/KYI150712.txt
  python scripts/jrdb_bridge_check.py /path/to/SED080913.txt
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._keys import ketto_to_horse_id, race_key_to_race_id  # noqa: E402


def _records(path: str):
    raw = Path(path).read_bytes()
    sep = b"\r\n" if b"\r\n" in raw else b"\n"
    return [r for r in raw.split(sep) if r.strip()]


def main() -> int:
    if len(sys.argv) < 2:
        print("使い方: python scripts/jrdb_bridge_check.py <JRDBファイル(KYI/SED/SKB)>")
        return 1
    path = sys.argv[1]
    if not Path(path).exists():
        print(f"ファイルが見つかりません: {path}")
        print("→ '/path/to/...' はプレースホルダです。実際のJRDBファイルのパスに置き換えてください。")
        print("  例: python scripts/jrdb_bridge_check.py ~/jrdb/KYI150712.txt")
        print("  ファイル位置が不明なら: find ~ -iname 'KYI*.txt' -o -iname 'SED*.txt' 2>/dev/null | head")
        return 1
    recs = _records(path)
    if not recs:
        print(f"空ファイル: {path}")
        return 1

    # 全レコードで key(0:8)→race_id, 血統登録(10:18)→horse_id を変換
    race_ids, horse_ids = [], []
    for r in recs:
        key = r[0:8].decode("cp932", "replace")
        ketto = r[10:18].decode("cp932", "replace")
        rid = race_key_to_race_id(key)
        hid = ketto_to_horse_id(ketto)
        if rid:
            race_ids.append(rid)
        if hid:
            horse_ids.append(hid)
    uniq_races = set(race_ids)
    uniq_horses = set(horse_ids)
    print(f"JRDB {Path(path).name}: {len(recs)} レコード / "
          f"race_id {len(uniq_races)}種 / horse_id {len(uniq_horses)}種")
    print("  変換例:", sorted(uniq_races)[:3], "/", sorted(uniq_horses)[:3])

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません（変換のみ実施）")
        return 0

    feat_races = set(featured.index.astype(str))
    feat_horses = set(featured["horse_id"].astype(str)) if "horse_id" in featured.columns else set()

    r_hit = len(uniq_races & feat_races)
    h_hit = len(uniq_horses & feat_horses)
    print("\n[橋渡し一致率]")
    print(f"  race_id : {r_hit}/{len(uniq_races)} = "
          f"{r_hit/len(uniq_races):.1%} が featured に存在")
    if feat_horses:
        print(f"  horse_id: {h_hit}/{len(uniq_horses)} = "
              f"{h_hit/len(uniq_horses):.1%} が featured に存在")
    print("\n判定:")
    if uniq_races and r_hit / len(uniq_races) > 0.8:
        print("  ★ race_id が高率で一致 → レースキー橋渡しは成立。")
    else:
        print("  △ race_id 一致が低い → サンプル日が featured 期間外か、採番規則の再検討が必要。")
    if feat_horses and uniq_horses and h_hit / len(uniq_horses) > 0.8:
        print("  ★ horse_id が高率で一致 → 血統登録番号橋渡しは成立。特記/基準オッズを貼れる。")
    elif feat_horses:
        print("  △ horse_id 一致が低い → horse_id 採番の対応を確認（feat['horse_id'].head() を共有ください）。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
