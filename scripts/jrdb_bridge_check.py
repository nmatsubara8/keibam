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
        print("  △ horse_id 一致が低い（featured は代理キーの可能性）。")
        _diagnose_horse_id(recs, featured)

    # 本命の結合キー: (race_id, 馬番)。race_id が一致していればこれで JRDB→featured を貼れる。
    _check_race_uma_join(recs, featured)
    return 0


def _check_race_uma_join(recs, featured) -> None:
    """(race_id, 馬番) 結合の一致率を測る。これが高ければ horse_id 不要で橋渡し成立。"""
    import pandas as pd

    jr = set()
    for r in recs:
        rid = race_key_to_race_id(r[0:8].decode("cp932", "replace"))
        uma = r[8:10].decode("cp932", "replace").strip()
        if rid and uma.isdigit():
            jr.add((rid, int(uma)))
    if "馬番" not in featured.columns:
        print("\n[(race_id,馬番)結合] featured に '馬番' 列が無く判定不可")
        return
    idx = featured.index.astype(str)
    um = pd.to_numeric(featured["馬番"], errors="coerce")
    fr = {(rid, int(u)) for rid, u in zip(idx, um, strict=False) if u == u}
    hit = len(jr & fr)
    print(f"\n[(race_id, 馬番) 結合の一致率]  {hit}/{len(jr)} = "
          f"{hit/len(jr):.1%}" if jr else "  判定不可")
    if jr and hit / len(jr) > 0.8:
        print("  ★★ (race_id, 馬番) で JRDB→featured を結合可能 → 橋渡し成立。horse_id は不要。")
        print("     JRDB内は血統登録番号で馬を時系列連結 → 前走特記(不利/出遅れ)を featured の馬に貼れる。")
    else:
        print("  △ (race_id,馬番) 一致も低い → 馬番の型/レースキー範囲を要確認。")


def _diagnose_horse_id(recs, featured) -> None:
    """featured の horse_id 形式を突き止めるため、複数の変換仮説の一致率を出す。"""
    fh = featured["horse_id"].astype(str).str.strip()
    print("\n[featured horse_id サンプル]")
    print("  例:", fh.head(6).tolist())
    print("  dtype:", featured["horse_id"].dtype,
          " 桁分布:", fh.str.len().value_counts().head().to_dict())
    feat_horses = set(fh)

    # JRDB 血統登録番号（生 8桁）
    raw = set()
    for r in recs:
        k = r[10:18].decode("cp932", "replace").strip()
        if k.isdigit() and len(k) == 8:
            raw.add(k)

    def century(k):
        yy = int(k[0:2])
        return (1900 + yy if yy >= 86 else 2000 + yy)

    hyps = {
        "世紀+血統登録(10桁)": lambda k: f"{century(k)}{k[2:]}",
        "生血統登録(8桁)": lambda k: k,
        "int化(先頭0除去)": lambda k: str(int(k)),
        "世紀+登録 int化": lambda k: str(int(f"{century(k)}{k[2:]}")),
    }
    print("\n[horse_id 変換仮説の一致率]")
    best = None
    for name, fn in hyps.items():
        conv = {fn(k) for k in raw}
        hit = len(conv & feat_horses)
        rate = hit / len(conv) if conv else 0.0
        print(f"  {name:<22}: {hit}/{len(conv)} = {rate:.1%}")
        if best is None or rate > best[1]:
            best = (name, rate)
    if best and best[1] > 0.8:
        print(f"  ★ 仮説「{best[0]}」が一致 → この変換で橋渡し成立。_keys.py を合わせます。")
    else:
        print("  △ どの仮説も低い → featured の horse_id は JRA血統登録と別採番"
              "（netkeiba独自ID等）。馬名+生年での突合表が必要。上の『例』を共有ください。")


if __name__ == "__main__":
    sys.exit(main())
