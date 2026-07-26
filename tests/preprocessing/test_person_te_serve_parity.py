"""ライブ推論 person_te の train/serve 一致（skew ゼロ）テスト。

学習時 `build_person_form_features`（全履歴に対する expanding）で得られる、ある「対象レース」行の
person_te 値と、ライブ推論 `person_te_for_upcoming`（履歴＋出馬表を結合して as-of 計算）で得られる
同じ馬の値が一致することを確認する。一致すれば train/serve skew が無い。
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.preprocessing._target_encoding import (
    build_person_form_features,
    person_te_for_upcoming,
)


def _history_with_target_race():
    """複数の過去レース＋「対象レース(target)」を含む results 履歴を作る。

    対象レースは最新日付。train ではこの行も含めて featured を作る（対象行の encoding は
    厳密過去なので自分と同日を除いた過去のみで決まる）。serve ではこの対象レースを「出馬表」
    として扱い、履歴（対象日より前）から再計算する。両者は一致するべき。
    """
    rng = np.random.RandomState(3)
    rows = []
    d0 = pd.Timestamp("2024-01-01")
    jockeys = ["J1", "J2", "J3"]
    trainers = ["T1", "T2"]
    for i in range(240):  # 過去レース群
        rows.append({
            "race_id": f"P{i // 8:04d}",
            "horse_id": f"h{i:04d}",
            "jockey_id": jockeys[i % 3],
            "trainer_id": trainers[i % 2],
            "owner_id": f"o{i % 5}",
            "着順": int(rng.randint(1, 13)),
            "race_type": "芝" if i % 2 == 0 else "ダート",
            "開催": (i % 10) + 1,
            "date": d0 + pd.Timedelta(days=i % 60),
        })
    # 対象レース（最新日・8頭）。J1/T1 などが騎乗。
    tdate = d0 + pd.Timedelta(days=120)
    target_rows = []
    for u in range(8):
        target_rows.append({
            "race_id": "TARGET",
            "horse_id": f"t{u}",
            "jockey_id": jockeys[u % 3],
            "trainer_id": trainers[u % 2],
            "owner_id": f"o{u % 5}",
            "着順": int(rng.randint(1, 9)),   # train には着順があるが encoding には自分は入らない
            "race_type": "芝",
            "開催": 5,
            "date": tdate,
        })
    hist = pd.DataFrame(rows).set_index("race_id")
    target = pd.DataFrame(target_rows).set_index("race_id")
    return hist, target, tdate


def test_serve_person_te_matches_train():
    hist, target, tdate = _history_with_target_race()

    # --- train 相当: 全履歴（過去＋対象レース）で build_person_form_features ---
    full = pd.concat([hist, target])
    train_feats = build_person_form_features(full, alpha=20.0)
    train_target = train_feats.loc["TARGET"].reset_index(drop=True)

    # --- serve 相当: 履歴（過去のみ）＋出馬表(target,着順を伏せる) で as-of 再計算 ---
    upcoming = target.drop(columns=["着順"]).copy()  # 出馬表は着順未確定
    serve_target = person_te_for_upcoming(
        hist, upcoming, race_date=tdate, alpha=20.0
    ).reset_index(drop=True)

    # 同じ列・同じ値（対象レース各馬）であること
    assert list(train_target.columns) == list(serve_target.columns)
    for col in train_target.columns:
        a = train_target[col].to_numpy(dtype=float)
        b = serve_target[col].to_numpy(dtype=float)
        assert np.allclose(a, b, equal_nan=True), f"{col}: train={a} serve={b}"


def test_serve_person_te_index_aligned_to_upcoming():
    hist, target, tdate = _history_with_target_race()
    upcoming = target.drop(columns=["着順"])
    out = person_te_for_upcoming(hist, upcoming, race_date=tdate, alpha=20.0)
    assert len(out) == len(upcoming)
    assert (out.index == upcoming.index).all()
