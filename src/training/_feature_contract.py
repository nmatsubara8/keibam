"""学習/推論の特徴量契約（列名・列順・dtype）を一元化する（#24）。

base/stacking モデルは位置ベース（`.values`）で学習・推論するため、学習時と推論時で
列の追加/削除/並び替えがあっても検出されず、**静かに誤予測**する危険がある。
本モジュールは「学習時に確定した特徴量名・順序（と任意で dtype）」を単一の契約として持ち、
推論前に入力 DataFrame をその契約へ厳密に整列（不足=エラー / 余分=drop / 順序=固定）する。

使い方（概念）:
    contract = FeatureContract.from_frame(X_train)          # 学習時に確定・モデルと一緒に保存
    X_aligned = contract.align(X_infer)                     # 推論前に整列（不一致は fail-fast）
    model.predict_proba(X_aligned)                          # 位置ベースでも順序が保証される

純ロジック（pandas のみ・重い依存なし）でユニットテスト可能に保つ。
"""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


class FeatureContractError(ValueError):
    """特徴量契約の不一致（不足列・dtype 不一致等）。学習=推論の乖離を fail-fast で止める。"""


@dataclass(frozen=True)
class FeatureContract:
    """学習時に確定した特徴量の列名・順序（と任意で dtype 文字列）の契約。"""

    names: tuple[str, ...]
    dtypes: tuple[str, ...] | None = None  # names と同順の dtype 文字列（None なら dtype 非検査）

    @classmethod
    def from_frame(cls, df: pd.DataFrame, *, with_dtypes: bool = True) -> "FeatureContract":
        """学習入力 DataFrame から契約を作る（列名・順序・dtype をそのまま採録）。"""
        names = tuple(str(c) for c in df.columns)
        dtypes = tuple(str(df[c].dtype) for c in df.columns) if with_dtypes else None
        return cls(names=names, dtypes=dtypes)

    def to_dict(self) -> dict:
        """JSON/pickle 保存用の素の dict。"""
        return {"names": list(self.names),
                "dtypes": list(self.dtypes) if self.dtypes is not None else None}

    @classmethod
    def from_dict(cls, d: dict) -> "FeatureContract":
        dt = d.get("dtypes")
        return cls(names=tuple(d["names"]), dtypes=tuple(dt) if dt is not None else None)

    def align(self, df: pd.DataFrame, *, allow_extra: bool = True,
              check_dtypes: bool = False, coerce_dtypes: bool = False) -> pd.DataFrame:
        """入力 DataFrame を契約の列・順序へ整列して返す（不一致は FeatureContractError）。

        - 不足列があれば **エラー**（silent 誤予測の主因を fail-fast）。
        - 余分列は allow_extra=True なら drop、False ならエラー。
        - 列順は契約 names に固定（位置ベース predict でも安全）。
        - check_dtypes=True で dtype 不一致を検出、coerce_dtypes=True なら契約 dtype へ変換。
        """
        if not isinstance(df, pd.DataFrame):
            raise FeatureContractError("align には DataFrame 入力が必要です（列名で整列するため）。")
        cols = set(map(str, df.columns))
        want = set(self.names)
        missing = [c for c in self.names if c not in cols]
        if missing:
            raise FeatureContractError(
                f"推論入力に学習時の特徴量が {len(missing)} 列不足しています: {missing[:20]}"
                f"{' …' if len(missing) > 20 else ''}。学習=推論の列不一致（列名変更/未生成）。"
            )
        extra = [str(c) for c in df.columns if str(c) not in want]
        if extra and not allow_extra:
            raise FeatureContractError(
                f"推論入力に契約外の列が {len(extra)} 列あります: {extra[:20]}"
                f"{' …' if len(extra) > 20 else ''}（allow_extra=False）。"
            )
        out = df.loc[:, list(self.names)].copy()  # 順序固定＋余分列 drop（不足は上で排除済み）
        if (check_dtypes or coerce_dtypes) and self.dtypes is not None:
            self._apply_dtypes(out, check=check_dtypes, coerce=coerce_dtypes)
        return out

    def _apply_dtypes(self, out: pd.DataFrame, *, check: bool, coerce: bool) -> None:
        mismatches = []
        for name, want_dt in zip(self.names, self.dtypes, strict=True):
            cur = str(out[name].dtype)
            if cur == want_dt:
                continue
            if coerce:
                try:
                    out[name] = out[name].astype(want_dt)
                except (ValueError, TypeError) as e:
                    raise FeatureContractError(
                        f"列 {name} を dtype {want_dt} へ変換できません（現 {cur}）: {e}"
                    ) from e
            elif check:
                mismatches.append((name, cur, want_dt))
        if mismatches:
            raise FeatureContractError(
                "dtype 不一致（列, 現, 期待）: "
                + ", ".join(f"({n},{c},{w})" for n, c, w in mismatches[:20])
                + (" …" if len(mismatches) > 20 else "")
            )
