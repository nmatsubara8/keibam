"""騎手＋厩舎ランク → featured に rank_bonus 列を付与（③ 単一スナップ全期間・リーク承知）。

jocrank/tnrank の pkl（scripts/jrdb_target_ingest.py 出力）を読み、crosswalk で JRDB コード→
netkeiba jockey_id/trainer_id に橋渡しして、featured に rank_bonus = z(騎手rank)+z(厩舎rank) を付ける。
物理シムは field_from_featured(..., rank_gain=X) でこれを ability に加点する。

⚠ 単一スナップショットを全レースに同一適用＝過去に未来ランクが混入する leak。過去 ROI をどれだけ
動かせるかの探索用（sim_walk_forward.py --rank-gain で sweep）。live(as-of)には transfer しない。

使い方:
  python scripts/build_rank_bonus.py \
    --jocrank data/jrdb_target/jrdb_target_jocrank.pkl \
    --tnrank  data/jrdb_target/jrdb_target_tnrank.pkl \
    --featured data/featured_jrdb.pkl --out data/featured_rankbonus.pkl
  # → sim_walk_forward.py が読む featured にこれを使い、--rank-gain を振る
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def _code_to_id(name: str) -> dict:
    """crosswalk（jockey/trainer）→ {jrdb_code: netkeiba_id}。読めなければ空 dict（=名前不使用時は0埋め）。"""
    try:
        from src.jrdb._crosswalk import read_crosswalk
    except Exception:
        return {}
    code_col, id_col = ("kishu_code", "jockey_id") if name == "jockey" else ("chokyo_code", "trainer_id")
    try:
        xw = read_crosswalk(name)
    except Exception as e:  # noqa: BLE001
        print(f"[warn] crosswalk '{name}' を読めません（{e}）→ 結合できず rank_bonus=0 になります",
              file=sys.stderr)
        return {}
    if xw is None or xw.empty or code_col not in xw or id_col not in xw:
        return {}
    return {str(c): str(i) for c, i in zip(xw[code_col], xw[id_col]) if str(i) not in ("", "nan")}


def main() -> int:
    import pandas as pd

    from src.simulation._rank_bonus import attach_rank_bonus, build_rank_z

    ap = argparse.ArgumentParser(description="騎手＋厩舎ランク → featured の rank_bonus 列")
    ap.add_argument("--jocrank", default="data/jrdb_target/jrdb_target_jocrank.pkl")
    ap.add_argument("--tnrank", default="data/jrdb_target/jrdb_target_tnrank.pkl")
    ap.add_argument("--featured", required=True, help="rank_bonus を付ける featured pkl")
    ap.add_argument("--out", required=True, help="出力 featured pkl")
    args = ap.parse_args()

    joc = pd.read_pickle(args.jocrank) if Path(args.jocrank).exists() else pd.DataFrame()
    tn = pd.read_pickle(args.tnrank) if Path(args.tnrank).exists() else pd.DataFrame()
    jockey_z = build_rank_z(joc, code_to_id=_code_to_id("jockey"))
    trainer_z = build_rank_z(tn, code_to_id=_code_to_id("trainer"))
    print(f"ランク z: 騎手 {len(jockey_z)} 人 / 厩舎 {len(trainer_z)} 厩舎")

    featured = pd.read_pickle(args.featured)
    out = attach_rank_bonus(featured, jockey_z, trainer_z)
    nz = int((out["rank_bonus"] != 0).sum())
    print(f"featured {len(out):,} 行 / rank_bonus 非0 {nz:,}（{nz / max(1, len(out)):.1%}）"
          f" 範囲 [{out['rank_bonus'].min():.2f}, {out['rank_bonus'].max():.2f}]")
    if nz == 0:
        print("[warn] rank_bonus が全て0です。crosswalk が読めず jockey_id/trainer_id と突合できていない"
              "可能性があります。", file=sys.stderr)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_pickle(args.out)
    print(f"書き出し: {args.out}\n次: python sim_walk_forward.py --rank-gain <値> "
          f"（featured にこの出力を使う）で回収率が動くか sweep")
    return 0


if __name__ == "__main__":
    sys.exit(main())
