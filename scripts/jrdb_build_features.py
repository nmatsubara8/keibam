"""JRDBファイル群(KYI/SED/SKB)を読み、featured にJRDB特徴量を付与して保存する。

フォルダ内のファイルを接頭辞(KYI/SED/SKB)で自動分類。連続期間ぶんを入れるほど
前走特記のカバレッジが上がる。出力は元featured＋JRDB列の pickle。

使い方:
  python scripts/jrdb_build_features.py --jrdb-dir /mnt/c/Users/.../jrdb \
      --out data/featured_jrdb.pkl
JRDB列だけのサイドカーが欲しい場合は --sidecar も付ける。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.jrdb._augment import attach, build_history, build_kyi  # noqa: E402


def _classify(jrdb_dir: str) -> dict[str, list[str]]:
    out = {"KYI": [], "SED": [], "SKB": []}
    for p in sorted(Path(jrdb_dir).glob("*")):
        name = p.name.upper()
        for pref in out:
            if name.startswith(pref):
                out[pref].append(str(p))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="featured に JRDB 特徴量を付与")
    ap.add_argument("--jrdb-dir", required=True, help="KYI/SED/SKB ファイルのフォルダ")
    ap.add_argument("--out", default="data/featured_jrdb.pkl", help="出力 pickle")
    ap.add_argument("--sidecar", action="store_true", help="JRDB列だけのサイドカーも保存")
    args = ap.parse_args()

    files = _classify(args.jrdb_dir)
    print(f"検出: KYI {len(files['KYI'])} / SED {len(files['SED'])} / SKB {len(files['SKB'])} ファイル")
    if not any(files.values()):
        print("JRDBファイルが見つかりません（接頭辞 KYI/SED/SKB）。")
        return 1

    from app._model_eval import load_featured_data
    featured = load_featured_data()
    if featured is None or featured.empty:
        print("featured_data がありません")
        return 1

    print("KYI 解析（基準オッズ/IDM）...")
    kyi = build_kyi(files["KYI"])
    print("SED/SKB 解析（前走特記の履歴）...")
    history = build_history(files["SED"], files["SKB"])
    print(f"  KYI {len(kyi):,}行 / 履歴 {len(history):,}行")

    print("featured へ付与（(race_id,馬番)結合＋前走チェーン）...")
    out = attach(featured, kyi, history)

    jr_cols = ["jrdb_idm", "jrdb_kijun_odds", "jrdb_kijun_gap", "prev_deokure", "prev_trouble"]
    print("\n[JRDB列カバレッジ]")
    for c in jr_cols:
        if c in out.columns:
            cov = out[c].notna().mean()
            print(f"  {c:<18}: {cov:.2%} 非欠損")

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_pickle(args.out)
    print(f"\n保存: {args.out}（{len(out):,}行）")
    if args.sidecar:
        side = out[[c for c in jr_cols if c in out.columns]]
        sp = str(Path(args.out).with_suffix(".sidecar.pkl"))
        side.to_pickle(sp)
        print(f"サイドカー: {sp}")
    print("\n次: この featured_jrdb.pkl を load して manji_walk_forward で "
          "kijun_gap/prev_trouble/prev_deokure を含む因子で前進検証。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
