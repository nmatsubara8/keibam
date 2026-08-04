"""騎手＋厩舎ランクを物理シムの ability 加減点 rank_bonus にする（① live 専用プライア・保守的固定）。

■ 運用方針（①・確定）
騎手/厩舎ランク（jocrank/tnrank）は日付なしの「現時点スナップ」。これを **これから走るレース**
の予測に使うのは as-of 情報＝リークなし・妥当なプライア。だが **過去レース** に同じスナップを
当てると「未来の昇格・成績を含む現在ランク」が混入する＝リークでバックテスト/較正を汚染する。
したがって:
  ・live（今後のレース予測）だけで rank_bonus を適用する。
  ・バックテスト/較正/スイープでは rank_gain=0（適用しない）を厳守する。
  ・加減点スケールは **過去データで最適化せず、保守的な固定値 RANK_GAIN_LIVE に据える**
    （過去 ROI へのフィッティング＝自己欺瞞を避ける）。live 経路は build_live_field を使う。

設計:
  rank_bonus = zscore(騎手rank) + zscore(厩舎rank)          （騎手＋厩舎の合算）
  物理シムで  ability += RANK_GAIN_LIVE · rank_bonus         （live のみ・固定スケール）

歴史的経緯: 当初は ③（単一スナップ全期間・リーク承知で過去 ROI を sweep）を検討したが、過去
データへの最適化はリーク値で live に transfer しないため、① へ切替。sim_rank_gain_sweep.py は
「rank が過去 ROI を動かすか」を診るリサーチ専用（本番の調整機構ではない）。
"""
from __future__ import annotations

import pandas as pd

# live 専用プライアの保守的固定スケール（過去データで調整しない）。
# rank_bonus = zscore(騎手) + zscore(厩舎) は概ね標準偏差 ~1.4。RANK_GAIN_LIVE=0.05 なら
# 騎手＋厩舎とも +1σ（rank_bonus≈2）で ability を +0.10 程度動かす＝能力スプレッド(≈0.2)の半分。
# 過学習を避けるための控えめな一定値。強めたいときも 0.10 を上限の目安にする。
RANK_GAIN_LIVE = 0.05


def build_rank_z(rank_df, *, code_col: str = "person_code", rank_col: str = "rank",
                 code_to_id: dict | None = None) -> dict:
    """rank_df(person_code, rank) → {id: zscore(rank)}。

    code_to_id（{jrdb_code: netkeiba_id}）があれば netkeiba id をキーに、無ければ person_code を
    そのままキーにする（テスト/直結用）。同一 code の重複は last。空/全欠損は空 dict。
    """
    if rank_df is None or len(rank_df) == 0 or code_col not in rank_df or rank_col not in rank_df:
        return {}
    d = rank_df[[code_col, rank_col]].copy()
    d[rank_col] = pd.to_numeric(d[rank_col], errors="coerce")
    d = d.dropna(subset=[code_col, rank_col]).drop_duplicates(code_col, keep="last")
    if d.empty:
        return {}
    mu = float(d[rank_col].mean())
    sd = float(d[rank_col].std(ddof=0))
    sd = sd if sd > 0 else 1.0
    out: dict = {}
    for code, r in zip(d[code_col].astype(str), d[rank_col], strict=False):
        nid = (code_to_id or {}).get(code)
        key = str(nid) if nid is not None else code
        out[key] = (float(r) - mu) / sd
    return out


def attach_rank_bonus(featured, jockey_z: dict, trainer_z: dict, *,
                      jockey_col: str = "jockey_id", trainer_col: str = "trainer_id",
                      out_col: str = "rank_bonus") -> pd.DataFrame:
    """featured に rank_bonus = jockey_z[jockey_id] + trainer_z[trainer_id]（欠損 0）列を付けて返す。

    単一スナップの z を全レースに同一適用する（③・leak 承知）。id は文字列化して突合。
    """
    f = featured.copy()
    if jockey_col in f.columns and jockey_z:
        jb = f[jockey_col].astype(str).map(jockey_z).astype(float).fillna(0.0)
    else:
        jb = pd.Series(0.0, index=f.index)
    if trainer_col in f.columns and trainer_z:
        tb = f[trainer_col].astype(str).map(trainer_z).astype(float).fillna(0.0)
    else:
        tb = pd.Series(0.0, index=f.index)
    f[out_col] = (jb + tb).to_numpy()
    return f


def assert_live_only(rank_gain, *, context: str = "backtest") -> bool:
    """バックテスト/較正で rank_gain!=0 が渡されたら①方針違反として大きく警告する。

    True=違反あり（呼び出し側で継続可否を判断）。① は rank_bonus を live 予測のみに使う規約で、
    過去レースに当てるとリークして評価を汚染するため。既定の backtest 経路は rank_gain=0。
    """
    import sys
    try:
        g = float(rank_gain)
    except (TypeError, ValueError):
        return False
    if abs(g) > 0:
        print(f"⚠⚠ [rank_bonus 方針違反] {context} で rank_gain={g}。① では rank_bonus は "
              "live 予測のみ——過去に当てるとリークで較正/ROI を汚染する。これはリサーチ専用の"
              "挙動であり本番評価には rank_gain=0 を使うこと。", file=sys.stderr)
        return True
    return False


def build_live_field(race_df, *, ability_spread: float = 0.20,
                     rank_gain: float | None = None, **field_kwargs):
    """今から走る1レースの RaceField を、騎手＋厩舎ランクの保守的固定プライア込みで作る（live専用）。

    ① の唯一の live 適用経路。rank_gain 既定は RANK_GAIN_LIVE（保守的固定・過去データで調整しない）。
    race_df に rank_bonus 列が無ければ加点は 0（field_from_featured 側で無効）。
    **バックテスト/較正には使わないこと**（それらは rank_gain=0 の field_from_featured を直接使う）。
    """
    from src.simulation._sim_params import field_from_featured
    g = RANK_GAIN_LIVE if rank_gain is None else float(rank_gain)
    return field_from_featured(race_df, ability_spread=ability_spread, rank_gain=g, **field_kwargs)
