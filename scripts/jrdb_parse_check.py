"""JRDB 実ファイルをパースして優先フィールドを目視確認する。

抽出が正しいか（基準オッズが妥当なオッズか、特記に 387=不利 等が入るか）を実データで検証。

使い方:
  python scripts/jrdb_parse_check.py KYI /mnt/c/Users/.../KYI150712.txt
  python scripts/jrdb_parse_check.py SED /mnt/c/Users/.../SED080913.txt
  python scripts/jrdb_parse_check.py SKB /mnt/c/Users/.../SKB020908.txt
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._parser import parse  # noqa: E402


def main() -> int:
    if len(sys.argv) < 3:
        print("使い方: python scripts/jrdb_parse_check.py <KYI|SED|SKB> <ファイル>")
        return 1
    rt, path = sys.argv[1].upper(), sys.argv[2]
    if not Path(path).exists():
        print(f"ファイルが見つかりません: {path}（/mnt/c/... 形式で）")
        return 1
    df = parse(path, rt)
    print(f"{rt}: {len(df)} レコード / race_id {df['race_id'].nunique()}種")

    if rt == "KYI":
        show = ["race_id", "umaban", "ketto", "bamei", "idm", "kijun_odds", "kijun_ninki"]
        print(df[show].head(8).to_string(index=False))
        print(f"\n基準オッズ 統計: min={df['kijun_odds'].min()} "
              f"median={df['kijun_odds'].median()} max={df['kijun_odds'].max()}")
        print(f"IDM 統計: min={df['idm'].min()} median={df['idm'].median()} max={df['idm'].max()}")
    elif rt == "SED":
        show = ["race_id", "umaban", "ymd", "chakujun", "deokure", "ichidori", "furi"]
        print(df[show].head(8).to_string(index=False))
        print(f"\n出遅>0 の頭数: {(df['deokure'] > 0).sum()} / 不利>0: {(df['furi'] > 0).sum()}"
              f"  ← これが前走特記(数値評価)の素")
    elif rt == "SKB":
        tk = [c for c in df.columns if c.startswith("tokki")]
        print(df[["race_id", "umaban", "ketto", *tk]].head(8).to_string(index=False))
        allcodes = Counter()
        for c in tk:
            allcodes.update(x for x in df[c] if x)
        print("\n特記コード頻度 上位20（387=不利, 195/出遅系 等が出れば抽出成功）:")
        for code, n in allcodes.most_common(20):
            print(f"  {code}: {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
