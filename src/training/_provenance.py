"""学習データの由来(provenance)を artifact に刻む純関数群。

「この artifact は本当に JRA 限定データで学習されたか」を後から確実に回答できるようにするための
研究基盤。要点は **入力フラグでなく実データから nar_rows を計算して併記** すること
（フラグが立っていてもフィルタ実装/適用順が壊れていれば無意味なため）。

_keiba_ai._set_feature_contract から呼び、model.training_provenance_ に保存する。純粋計算のみ
（git commit の取得だけ best-effort で subprocess）。
"""
from __future__ import annotations

from collections import Counter
from typing import Optional


def _git_commit() -> Optional[str]:
    """現在の git commit（best-effort・取得不能なら None）。"""
    try:
        import subprocess
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, timeout=5)
        return out.stdout.strip() or None
    except Exception:  # noqa: BLE001
        return None


def build_training_provenance(index, *, jra_only=None, training_period=None,
                              feature_contract_version=None) -> dict:
    """学習 index(race_id) から由来メタを作る。nar_rows は**実データ由来**（フラグ非依存）。

    返す dict:
      train_rows/train_races、nar_rows/nar_fraction（場コード30+＝地方を実測）、
      jra_only_effective（nar_rows==0＝データが実際にJRA限定か）、jra_only_flag（入力の記録のみ）、
      place_code_counts、training_period、feature_contract_version、git_commit。
    """
    import pandas as pd

    from src.constants._model_category import central_index_mask
    rid = pd.Series(pd.Index(index).astype(str)).str.replace(r"\.0$", "", regex=True)
    n = len(rid)
    mask = central_index_mask(rid) if n else []
    nar = int((~pd.Series(mask)).sum()) if n else 0
    places = rid.str[4:6]
    return {
        "train_rows": int(n),
        "train_races": int(rid.nunique()),
        "nar_rows": nar,
        "nar_fraction": float(nar / n) if n else 0.0,
        "jra_only_effective": bool(nar == 0 and n > 0),  # 実データから確定
        "jra_only_flag": jra_only,                       # 入力フラグ（記録のみ・保証は effective）
        "place_code_counts": dict(Counter(places).most_common()) if n else {},
        "training_period": training_period,
        "feature_contract_version": feature_contract_version,
        "git_commit": _git_commit(),
    }


def assert_jra_only(index) -> int:
    """JRA限定を実データで強制（fail-closed）。NAR 行が残っていれば RuntimeError。返す nar_rows(=0)。

    --jra-only 指定時に「フラグは立っているが実データに NAR が残る」フィルタ破損を検出する。
    """
    import pandas as pd

    from src.constants._model_category import central_index_mask
    rid = pd.Index(index).astype(str)
    nar = int((~pd.Series(central_index_mask(rid))).sum()) if len(rid) else 0
    if nar != 0:
        raise RuntimeError(
            f"JRA-only 学習のはずが NAR(地方) を {nar} 行含む＝フィルタ実装/適用順の破損。"
            "central_index_mask の適用箇所を確認せよ。")
    return nar
