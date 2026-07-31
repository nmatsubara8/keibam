"""検証・学習・評価の各結果に付ける再現メタデータ（EvalStamp）。

prod_p vs 簡易 harness の混乱（例: 本セッションの 0.835 疑義＝特徴量471 vs 570・分割差の産物）を
構造的に防ぐため、「どのモデル / どの特徴スキーマ / どの期間 / 何を落として / オッズ込みか /
seed / 分割法」を 1 つの dict に固めて結果へ添付する。純関数中心・pandas 非依存。

使い方:
  from src.pipeline._eval_stamp import make_stamp, format_stamp
  stamp = make_stamp(feature_names=trt_cols, training_dates=df.loc[tr, "date"],
                     eval_dates=df.loc[te, "date"], drop_columns=DROP,
                     odds_included=keep_odds, seed=0, split_method=f"year<{cutoff}")
  print(format_stamp(stamp))          # 人間可読の1行
  result_json["stamp"] = stamp        # JSON へ添付
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Sequence
from dataclasses import asdict, dataclass


def feature_schema_hash(names: Sequence[str]) -> str:
    """順序付き列名から安定ハッシュ（sha1 先頭12桁）。列順の違いも別ハッシュになる。

    同じ特徴集合でも列順が違えば別 hash＝学習/推論の列順パリティ崩れを検知できる。
    """
    joined = "\n".join(str(n) for n in names)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()[:12]


def date_range(values: Iterable) -> tuple[str, str] | None:
    """日付列（ISO 文字列 or Timestamp の反復可能）から (min, max) を YYYY-MM-DD で返す。

    None/空/NaT/nan は除外。空なら None。文字列 ISO 日付・pandas Timestamp のどちらでも動く
    （どちらも辞書順＝時系列順、str()[:10] で日付部分になる）。
    """
    bad = {"", "NaT", "nan", "None"}
    vals = [v for v in values if v is not None and str(v) not in bad]
    if not vals:
        return None
    return (str(min(vals))[:10], str(max(vals))[:10])


@dataclass(frozen=True)
class EvalStamp:
    model_version: str | None = None
    feature_schema_hash: str | None = None
    n_features: int | None = None
    training_period: tuple[str, str] | None = None
    eval_period: tuple[str, str] | None = None
    drop_columns: tuple[str, ...] = ()
    odds_included: bool | None = None
    seed: int | None = None
    split_method: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def make_stamp(
    *,
    model_version: str | None = None,
    feature_names: Iterable[str] | None = None,
    training_dates: Iterable | None = None,
    eval_dates: Iterable | None = None,
    drop_columns: Iterable[str] = (),
    odds_included: bool | None = None,
    seed: int | None = None,
    split_method: str | None = None,
) -> dict:
    """材料から EvalStamp dict を作る。feature_names→(hash, n)、dates→(min,max)。"""
    names = list(feature_names) if feature_names is not None else None
    return EvalStamp(
        model_version=model_version,
        feature_schema_hash=feature_schema_hash(names) if names is not None else None,
        n_features=len(names) if names is not None else None,
        training_period=date_range(training_dates) if training_dates is not None else None,
        eval_period=date_range(eval_dates) if eval_dates is not None else None,
        drop_columns=tuple(str(c) for c in drop_columns),
        odds_included=odds_included,
        seed=seed,
        split_method=split_method,
    ).to_dict()


def format_stamp(stamp: dict) -> str:
    """スタンプを人間可読の1行に（ログ/print 用）。"""
    def _rng(r):
        return f"{r[0]}..{r[1]}" if r else "None"

    return (
        "[検証メタ] "
        f"model={stamp.get('model_version')} "
        f"schema={stamp.get('feature_schema_hash')}({stamp.get('n_features')}列) "
        f"train={_rng(stamp.get('training_period'))} "
        f"eval={_rng(stamp.get('eval_period'))} "
        f"odds={'込' if stamp.get('odds_included') else '除外'} "
        f"seed={stamp.get('seed')} split={stamp.get('split_method')} "
        f"drop={len(stamp.get('drop_columns', ()))}列"
    )
