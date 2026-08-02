"""run_residual_head の純関数テスト（レース内z-score・レコード構築）。"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd

_MOD = Path(__file__).resolve().parents[2] / "scripts" / "run_residual_head.py"
_spec = importlib.util.spec_from_file_location("run_residual_head", _MOD)
rrh = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rrh)


def test_zscore_within_race():
    z = rrh.zscore_within_race([1.0, 2.0, 3.0])
    assert abs(z.mean()) < 1e-9 and abs(z.std() - 1.0) < 1e-9
    assert list(rrh.zscore_within_race([5.0])) == [0.0]        # 1頭→0
    assert list(rrh.zscore_within_race([2.0, 2.0])) == [0.0, 0.0]  # std0→0


def test_build_residual_records_jra_and_zscore():
    # JRA(場05) 1レース3頭 + NAR(場44) 1レース3頭。jra_only で NAR 除外。
    rows = []
    for u, o, rk, a in [(1, 3.0, 2, 1.0), (2, 2.0, 1, 3.0), (3, 9.0, 3, 5.0)]:
        rows.append(("202305010101", u, o, rk, a))
    for u, o, rk, a in [(1, 3.0, 1, 1.0), (2, 2.0, 2, 2.0), (3, 9.0, 3, 3.0)]:
        rows.append(("202544010101", u, o, rk, a))
    df = pd.DataFrame([(u, o, rk, a) for _, u, o, rk, a in rows],
                      columns=["馬番", "単勝", "着順", "myfeat"],
                      index=pd.Index([r for r, *_ in rows], name="race_id"))
    recs, cols = rrh.build_residual_records(df, ["myfeat"], jra_only=True)
    assert cols == ["myfeat"]
    assert len(recs) == 1 and recs[0]["race_id"] == "202305010101"   # NAR 除外
    assert recs[0]["winner"] == 2
    # レース内 z-score: myfeat [1,3,5] → 平均0
    zs = [recs[0]["feats"][u]["myfeat"] for u in (1, 2, 3)]
    assert abs(sum(zs)) < 1e-9
