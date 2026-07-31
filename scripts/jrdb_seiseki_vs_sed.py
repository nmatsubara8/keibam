"""配布「成績IDM」(idmse) と 既存 `SED[idm]` の値照合（タスク#5）。

「成績IDM は既に SED から取込済みか、丸め/補正差か、独立系列か」を実データで確定する。
新規アップロード不要: SED は既存の取込慣習（--jrdb-dir に KYI/SED/SKB を置く）に乗せる。

使い方（ユーザー環境＝SED 実データがある側で実行）:
  # 成績IDM は先に取り込んで pkl 化しておく（scripts/jrdb_target_ingest.py）
  python scripts/jrdb_target_ingest.py --src /mnt/c/Users/Ayaka/Downloads --out-dir data/jrdb_target
  # SED と突き合わせ
  python scripts/jrdb_seiseki_vs_sed.py --jrdb-dir /mnt/c/Users/Ayaka/Downloads

--idmse で idmse の pkl か zip 置き場を明示可（既定 data/jrdb_target/jrdb_target_idmse.pkl）。
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _load_idmse(idmse_arg: str):
    import pandas as pd

    from src.jrdb._extract import read_jrdb_bytes
    from src.jrdb._target import classify, parse_target_bytes

    p = Path(idmse_arg)
    if p.is_file() and p.suffix == ".pkl":
        return pd.read_pickle(p)
    # zip/dir から parse
    zips = [p] if p.is_file() else sorted(p.glob("idmse_*.zip"))
    frames = []
    for z in zips:
        if classify(z.name) != "idmse":
            continue
        entries = [(z.name, n, d) for n, d in read_jrdb_bytes(str(z))]
        frames.append(parse_target_bytes("idmse", entries))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def main() -> int:
    import pandas as pd

    from src.jrdb._extract import extract_dir
    from src.jrdb._parser import parse
    from src.jrdb._target import compare_seiseki_vs_sed

    ap = argparse.ArgumentParser(description="成績IDM(idmse) × SED[idm] 値照合")
    ap.add_argument("--jrdb-dir", required=True, help="SED(.txt/.zip/.lzh) を置いたフォルダ")
    ap.add_argument("--extract-to", default="data/jrdb_txt", help="アーカイブ展開先")
    ap.add_argument("--idmse", default="data/jrdb_target/jrdb_target_idmse.pkl",
                    help="idmse の pkl か zip/ディレクトリ")
    ap.add_argument("--out", default="data/jrdb_target/seiseki_vs_sed.json", help="結果 JSON")
    args = ap.parse_args()

    by_type = extract_dir(args.jrdb_dir, args.extract_to)
    sed_files = by_type.get("SED", [])
    if not sed_files:
        print(f"SED ファイルが見つかりません: {args.jrdb_dir}", file=sys.stderr)
        return 1
    sed = pd.concat([parse(p, "SED") for p in sed_files], ignore_index=True)
    print(f"SED {len(sed):,}行 / {len(sed_files)} ファイル")

    idmse = _load_idmse(args.idmse)
    if idmse.empty:
        print(f"idmse が空です: {args.idmse}（先に jrdb_target_ingest を実行）", file=sys.stderr)
        return 1
    print(f"idmse {len(idmse):,}行")

    rep = compare_seiseki_vs_sed(sed, idmse)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(rep, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== 成績IDM(idmse) × SED[idm] 照合 ===")
    print(f"  重なりキー {rep['n_overlap_keys']:,} / 両値あり {rep['n_both_present']:,}")
    print(f"  一致率      {rep['exact_match_rate']}")
    print(f"  最大絶対差  {rep['max_abs_diff']}  平均絶対差 {rep['mean_abs_diff']}")
    print(f"  相関        {rep['corr']}  スケール比中央値 {rep['scale_ratio_median']}")
    print(f"  SED範囲 {rep['sed_range']} / idmse範囲 {rep['idmse_range']}")
    print(f"  SEDのみ {rep['n_sed_only']:,} / idmseのみ {rep['n_idmse_only']:,}")
    # 判定の目安（最終判断はユーザー）
    emr = rep["exact_match_rate"]
    if emr is not None and emr >= 0.99:
        print("  → ほぼ完全一致: idmse は不要。SED[idm] から履歴集約(mean/max/trend, shift付)で足りる。")
    elif rep["corr"] is not None and rep["corr"] >= 0.98:
        print("  → 高相関だが不一致: 丸め/補正/対象差の可能性。差分の意味を確認。")
    else:
        print("  → 低一致: 別系列の可能性。独立系列として評価対象に。")
    print(f"\n書き出し: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
