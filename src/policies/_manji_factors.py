"""卍式・行動バイアス因子の抽出（JRDB専有指数を使わない範囲）。

各因子は「featured の1バッチ（index=race_id）」を受け取り、行ごとの**バケットラベル**
（文字列）を返す純関数。点数（±）は付けない——点数は Model 2（回収率較正 Layer A）が
学習期間の回収率から決める。ここは「どのバケットに属すか」だけを決定的に返す。

設計方針:
- 必要な生列が無い因子は、全行 "na"（＝中立・0点）を返して安全にスキップする
  （既存パイプラインの「列不在は自動スキップ」の作法に合わせる）。
- レース内相対（馬体重順位・斤量順位など）は index=race_id で groupby して計算する。
- 卍さんの「馬体重は今走でなく前走の順位」等の細部は、featured に前走列がある場合のみ
  近似し、無ければ今走で代用する（各因子の docstring に明記）。
"""
from __future__ import annotations

from typing import Any
from typing import Callable

import numpy as np
import pandas as pd

from src.constants._results_cols import ResultsCols

NA = "na"


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def _parse_body_weight(s: pd.Series) -> pd.Series:
    """'480(+4)' / '480' / '計不' → 先頭の体重(kg)。数値化不能は NaN。"""
    if s is None:
        return pd.Series(np.nan, index=[])
    return pd.to_numeric(s.astype(str).str.extract(r"(\d+)")[0], errors="coerce")


def _parse_sex(s: pd.Series) -> pd.Series:
    """'牡3'/'牝4'/'セ5' → '牡'/'牝'/'セ'。"""
    return s.astype(str).str.extract(r"([牡牝セ])")[0].fillna(NA)


def _parse_age(s: pd.Series) -> pd.Series:
    """'牡3' → 3（歳）。"""
    return pd.to_numeric(s.astype(str).str.extract(r"(\d+)")[0], errors="coerce")


# --- スキーマ耐性リゾルバ: 生netkeiba列でも engineered列でも同じ値を取り出す --------

def _col(df: pd.DataFrame, *names):
    """候補列名のうち最初に存在するものを返す（無ければ None）。"""
    for n in names:
        if n in df.columns:
            return n
    return None


def _onehot_cat(df: pd.DataFrame, prefix: str):
    """'性__牡','性__牝',... のような one-hot 群から元カテゴリ Series を復元。無ければ None。"""
    cols = [c for c in df.columns if c.startswith(prefix)]
    if not cols:
        return None
    sub = df[cols].to_numpy()
    labels = np.array([c[len(prefix):] for c in cols])
    idx = sub.argmax(axis=1)
    anyhot = sub.max(axis=1) > 0
    out = pd.Series(labels[idx], index=df.index, dtype=object)
    return out.where(anyhot, NA)


def _age_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "年齢", "馬齢", "age")
    if c is not None:
        return _num(df[c])
    if ResultsCols.SEX_AGE in df.columns:
        return _parse_age(df[ResultsCols.SEX_AGE])
    return pd.Series(np.nan, index=df.index)


def _sex_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "性別", "性", "sex", "性コード")
    if c is not None:
        return df[c].astype(str).str.extract(r"([牡牝セ])")[0].fillna(NA)
    oh = _onehot_cat(df, "性__")  # 性__牡/性__牝/性__セ → 牡/牝/セ
    if oh is not None:
        return oh
    if ResultsCols.SEX_AGE in df.columns:
        return _parse_sex(df[ResultsCols.SEX_AGE])
    return pd.Series(NA, index=df.index)


def _body_weight_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "体重", "馬体重", "body_weight")
    if c is None:
        return pd.Series(np.nan, index=df.index)
    s = df[c]
    if pd.api.types.is_numeric_dtype(s):
        return _num(s)
    return _parse_body_weight(s)  # "480(+4)" 形式


def _weight_diff_series(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "体重変化", "馬体重増減", "weight_diff", "体重増減")
    if c is not None:
        return _num(df[c])
    c2 = _col(df, "馬体重")
    if c2 is not None:
        m = df[c2].astype(str).str.extract(r"\(([-+]?\d+)\)")[0]
        return _num(m)
    return pd.Series(np.nan, index=df.index)


def _interval_series(df: pd.DataFrame) -> pd.Series:
    # 履歴から算出した mf_interval を優先（既存 interval 列がスパースな環境で dense に発火）。
    c = _col(df, "mf_interval", "interval", "days", "レース間隔", "休養日数")
    return _num(df[c]) if c is not None else pd.Series(np.nan, index=df.index)


def _race_type_series(df: pd.DataFrame):
    """芝ダ列（文字列 Series）。無ければ None。"""
    c = _col(df, "race_type", "芝ダ", "芝ダート", "track_type", "コース種別")
    if c is not None:
        return df[c].astype(str)
    return _onehot_cat(df, "race_type__")  # race_type__芝/ダート/障害


def _dist_change_series(df: pd.DataFrame) -> pd.Series:
    # 履歴から算出した mf_dist_change を優先（既存 dist_change 列がスパースでも dense に発火）。
    c = _col(df, "mf_dist_change", "dist_change", "距離変化", "距離差", "距離増減")
    return _num(df[c]) if c is not None else pd.Series(np.nan, index=df.index)


# --- 個別因子 --------------------------------------------------------------

def f_umaban_parity(df: pd.DataFrame) -> pd.Series:
    """馬番の奇偶（卍: 奇数/偶数で加減点）。"""
    if ResultsCols.UMABAN not in df.columns:
        return pd.Series(NA, index=df.index)
    u = _num(df[ResultsCols.UMABAN])
    return np.where(u.isna(), NA, np.where(u % 2 == 1, "odd", "even"))


def f_sex(df: pd.DataFrame) -> pd.Series:
    return _sex_series(df).to_numpy()


def f_age(df: pd.DataFrame) -> pd.Series:
    """馬齢帯: 2-3=young / 4-5=prime / 6+=old。"""
    a = _age_series(df)
    if a.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(a, [0, 3.5, 5.5, np.inf], labels=["young", "prime", "old"])
    return out.astype(object).fillna(NA).to_numpy()


def _season(df: pd.DataFrame) -> pd.Series:
    if "date" not in df.columns:
        return pd.Series(NA, index=df.index)
    m = pd.to_datetime(df["date"], errors="coerce").dt.month
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[m.isin([12, 1, 2])] = "winter"
    lab[m.isin([3, 4, 5])] = "spring"
    lab[m.isin([6, 7, 8])] = "summer"
    lab[m.isin([9, 10, 11])] = "autumn"
    return lab


def f_season(df: pd.DataFrame) -> pd.Series:
    return _season(df).to_numpy()


def f_season_sex(df: pd.DataFrame) -> pd.Series:
    """季節×性別（卍: 夏の牝馬に加点、など）。"""
    if "date" not in df.columns:
        return pd.Series(NA, index=df.index)
    sex = _sex_series(df)
    sea = _season(df)
    combo = sea.astype(str) + "_" + sex.astype(str)
    combo[(sea == NA) | (sex == NA)] = NA
    return combo.to_numpy()


def f_weight_rank(df: pd.DataFrame) -> pd.Series:
    """馬体重のレース内順位帯（light/mid/heavy）。全馬が重い/軽い開催の歪みを
    絶対値でなく順位で吸収する（卍の指摘）。"""
    w = _body_weight_series(df)
    if w.isna().all():
        return pd.Series(NA, index=df.index)
    r = w.groupby(df.index).rank(pct=True)
    out = pd.cut(r, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(w.notna(), NA).to_numpy()


def f_rotation(df: pd.DataFrame) -> pd.Series:
    """ローテ（前走からの間隔 interval[日]）: 連闘/中1-3週/中4週+/休養明け。"""
    iv = _interval_series(df)
    if iv.isna().all():
        return pd.Series(NA, index=df.index)
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[iv <= 8] = "rentai"        # 連闘（中0週）
    lab[(iv > 8) & (iv <= 27)] = "naka1_3"
    lab[(iv > 27) & (iv < 180)] = "naka4plus"
    lab[iv >= 180] = "kyuyoake"    # 休養明け
    return lab.to_numpy()


def f_dist_change(df: pd.DataFrame) -> pd.Series:
    """距離変更（今回−前走, dist_change[100m単位or m]）: 短縮/同/延長。"""
    dc = _dist_change_series(df)
    if dc.isna().all():
        return pd.Series(NA, index=df.index)
    lab = pd.Series(NA, index=df.index, dtype=object)
    lab[dc < 0] = "short"
    lab[dc == 0] = "same"
    lab[dc > 0] = "extend"
    return lab.to_numpy()


def f_kinryo_rank(df: pd.DataFrame) -> pd.Series:
    """斤量のレース内順位帯（light/mid/heavy）。"""
    if ResultsCols.KINRYO not in df.columns:
        return pd.Series(NA, index=df.index)
    k = _num(df[ResultsCols.KINRYO])
    r = k.groupby(df.index).rank(pct=True)
    out = pd.cut(r, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(k.notna(), NA).to_numpy()


def f_track_sex(df: pd.DataFrame) -> pd.Series:
    """トラック（芝/ダ）×性別（卍: トラックと性別から回収率を分析し加減）。"""
    rt = _race_type_series(df)
    sex = _sex_series(df)
    if rt is None or (sex == NA).all():
        return pd.Series(NA, index=df.index)
    combo = rt + "_" + sex.astype(str)
    combo[sex.to_numpy() == NA] = NA
    return combo.to_numpy()


def f_career(df: pd.DataFrame) -> pd.Series:
    """キャリア（出走回数帯）。featured に出走回数列がある場合のみ（無ければ na）。
    候補列名を順に探す。"""
    col = _col(df, "times", "n_races", "career", "出走回数", "race_count", "出走数")
    if col is None:
        return pd.Series(NA, index=df.index)
    c = _num(df[col])
    out = pd.cut(c, [-1, 0, 3, 8, np.inf], labels=["debut", "few", "mid", "many"])
    return out.astype(object).fillna(NA).to_numpy()


def f_popularity(df: pd.DataFrame) -> pd.Series:
    """人気帯（卍: 1番人気は過剰人気で減点/中穴に妙味）。人気列優先、無ければ単勝順位。"""
    if ResultsCols.POPULARITY in df.columns and _num(df[ResultsCols.POPULARITY]).notna().any():
        pop = _num(df[ResultsCols.POPULARITY])
    elif ResultsCols.TANSHO_ODDS in df.columns:
        pop = _num(df[ResultsCols.TANSHO_ODDS]).groupby(df.index).rank(method="min")
    else:
        return pd.Series(NA, index=df.index)
    # 13番人気以下（大穴）は単勝で人気バイアスにより過剰に買われ回収率が低い（名鑑）→ 分離。
    out = pd.cut(pop, [0, 1, 3, 8, 12, np.inf],
                 labels=["fav1", "fav2_3", "mid4_8", "long9_12", "long13plus"])
    return out.astype(object).fillna(NA).to_numpy()


def f_waku(df: pd.DataFrame) -> pd.Series:
    """枠順×芝ダ（卍/データ: 芝は内枠有利・ダは外枠有利）。"""
    rt = _race_type_series(df)
    if ResultsCols.WAKUBAN not in df.columns or rt is None:
        return pd.Series(NA, index=df.index)
    w = _num(df[ResultsCols.WAKUBAN])
    pos = pd.cut(w, [0, 3, 5, 8], labels=["inner", "mid", "outer"])
    combo = rt + "_" + pos.astype(object)
    combo = combo.where(w.notna() & pos.notna(), NA)
    return combo.to_numpy()


def f_body_weight(df: pd.DataFrame) -> pd.Series:
    """馬体重の絶対帯（卍/データ: 大型有利・軽量危険）。"""
    w = _body_weight_series(df)
    if w.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(w, [0, 440, 470, 500, np.inf], labels=["u440", "440_470", "470_500", "o500"])
    return out.astype(object).where(w.notna(), NA).to_numpy()


def f_weight_diff(df: pd.DataFrame) -> pd.Series:
    """前走比 馬体重増減の帯（卍: 大幅減は割引・大幅増は平場割引/重賞妙味）。"""
    d = _weight_diff_series(df)
    if d.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(d, [-1000, -12, -3, 3, 12, 1000],
                 labels=["big_minus", "minus", "flat", "plus", "big_plus"])
    return out.astype(object).where(d.notna(), NA).to_numpy()


def f_kinryo_per_weight(df: pd.DataFrame) -> pd.Series:
    """斤量/馬体重 の相対負担帯（卍: 軽量馬×重斤量は危険）。"""
    if "kinryo_per_weight" in df.columns:
        r = _num(df["kinryo_per_weight"])
    else:
        w = _body_weight_series(df)
        if ResultsCols.KINRYO not in df.columns or w.isna().all():
            return pd.Series(NA, index=df.index)
        r = _num(df[ResultsCols.KINRYO]) / w
    q = r.groupby(df.index).rank(pct=True)
    out = pd.cut(q, [0, 1 / 3, 2 / 3, 1.0], labels=["light", "mid", "heavy"], include_lowest=True)
    return out.astype(object).where(r.notna(), NA).to_numpy()


def f_age_rotation(df: pd.DataFrame) -> pd.Series:
    """馬齢×休養明け（卍/データ: 若馬の長期明けは加点・古馬の長期明けは減点）。"""
    a = _age_series(df)
    iv = _interval_series(df)
    if a.isna().all() or iv.isna().all():
        return pd.Series(NA, index=df.index)
    young = a <= 3
    layoff = iv >= 90
    lab = pd.Series("other", index=df.index, dtype=object)
    lab[young & layoff] = "young_layoff"
    lab[(~young) & layoff] = "old_layoff"
    lab[(a.isna()) | (iv.isna())] = NA
    return lab.to_numpy()


def f_dist_age(df: pd.DataFrame) -> pd.Series:
    """距離変更×馬齢（卍: 2歳の距離短縮/延長で回収率が変わる）。"""
    dc = _dist_change_series(df)
    a = _age_series(df)
    if dc.isna().all() or a.isna().all():
        return pd.Series(NA, index=df.index)
    grp = np.where(a <= 3, "young", "old")
    dirn = np.where(dc < 0, "short", np.where(dc > 0, "extend", "same"))
    combo = pd.Series([f"{g}_{d}" for g, d in zip(grp, dirn, strict=False)], index=df.index)
    combo = combo.where(dc.notna() & a.notna(), NA)
    return combo.to_numpy()


def f_prev_finish(df: pd.DataFrame) -> pd.Series:
    """前走着順帯（卍/データ: 前走6着や前走人気で凡走は過小評価=妙味、二桁着はカット）。

    featured に前走着順の列がある場合のみ（無ければ na）。候補列名を寛容に探す。
    """
    col = next((c for c in ("mf_prev_rank", "前走着順", "prev_rank", "前走_着順", "着順_1",
                            "rank_prev", "last_rank") if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    r = _num(df[col])
    out = pd.cut(r, [0, 1, 3, 5, 6, 10, 99],
                 labels=["p1", "p2_3", "p4_5", "p6", "p7_10", "p11plus"])
    return out.astype(object).where(r.notna(), NA).to_numpy()


def f_paddock(df: pd.DataFrame) -> pd.Series:
    """パドック評価（JRDB系: A/B/穴）。専門家の当日馬体評価＝卍の妙味源に近い。"""
    oh = _onehot_cat(df, "パドック評価__")
    if oh is None:
        return pd.Series(NA, index=df.index)
    return oh.to_numpy()


def f_ground(df: pd.DataFrame) -> pd.Series:
    """馬場状態（良/稍重/重/不良）。卍: 道悪適性の加減点の土台。"""
    oh = _onehot_cat(df, "ground_state1__")
    if oh is None:
        return pd.Series(NA, index=df.index)
    return oh.to_numpy()


def f_race_class(df: pd.DataFrame) -> pd.Series:
    """クラス（新馬/未勝利/1-3勝/OP/G）。卍: 昇降級・クラス別の回収率傾向。"""
    oh = _onehot_cat(df, "race_class__")
    if oh is None:
        return pd.Series(NA, index=df.index)
    return oh.to_numpy()


def f_leg_type(df: pd.DataFrame) -> pd.Series:
    """脚質（前/後）。卍/データ: 逃げ先行有利・展開。leg_type_binary から。"""
    if "leg_type_binary" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["leg_type_binary"])
    lab = np.where(v.isna(), NA, np.where(v >= 0.5, "back", "front"))
    return lab


def f_pace_pressure(df: pd.DataFrame) -> pd.Series:
    """展開（レース内の先行勢比率）。各馬の**過去走**脚質傾向 leg_type_binary から、
    そのレースの「逃げ・先行タイプ」の割合を出す＝想定ペースの代理。

    前進安全: 当該レースの結果（今走の通過順）ではなく、各馬が過去に示した脚質傾向の
    レース内構成を使う（発走前に既知）。先行勢が多い＝ハイペース想定＝差し有利、が定石。

    単独ではレース内で全馬同値＝順位に効かないが、**脚質とのクロス（展開×脚質）**で
    「先行馬×ハイペース＝不利／差し馬×ハイペース＝有利」を per-horse に表せる。
    バケット: few(緩)/mid/many(速)。レースの有効脚質が乏しければ na。
    """
    if "leg_type_binary" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["leg_type_binary"])
    front = (v < 0.5).astype(float)                 # 先行勢=1
    denom = v.notna().astype(float)
    idx = pd.Series(df.index, index=df.index)
    n_front = front.groupby(idx).transform("sum")
    n_valid = denom.groupby(idx).transform("sum")
    ratio = np.where(n_valid > 0, n_front / n_valid, np.nan)
    lab = np.where(np.isnan(ratio), NA,
                   np.where(ratio < 0.30, "few",
                            np.where(ratio < 0.55, "mid", "many")))
    return pd.Series(lab, index=df.index)


def f_kijun_gap(df: pd.DataFrame) -> pd.Series:
    """基準オッズ乖離（JRDBフェアバリュー / 市場単勝）＝Benter核。JRDB付与時のみ発火。

    gap<1: 基準<市場＝JRDBは市場より勝率高いと見る＝市場が過小評価＝買い妙味。
    gap>1: 基準>市場＝市場が過大評価。
    """
    if "jrdb_kijun_gap" not in df.columns:
        return pd.Series(NA, index=df.index)
    g = _num(df["jrdb_kijun_gap"])
    out = pd.cut(g, [0, 0.7, 0.9, 1.1, 1.5, np.inf],
                 labels=["under", "slight_under", "fair", "slight_over", "over"])
    return out.astype(object).where(g.notna(), NA).to_numpy()


def f_prev_trouble(df: pd.DataFrame) -> pd.Series:
    """前走で不利/道中外々等の特記（JRDB SKB特記）。卍の核＝過小評価の妙味。JRDB付与時のみ。"""
    if "prev_trouble" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["prev_trouble"])
    return np.where(v.isna(), NA, np.where(v >= 0.5, "trouble", "clean"))


def f_prev_deokure(df: pd.DataFrame) -> pd.Series:
    """前走で出遅れ（JRDB SED出遅）。着順が実力より悪く出た過小評価馬。JRDB付与時のみ。"""
    if "prev_deokure" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["prev_deokure"])
    return np.where(v.isna(), NA, np.where(v >= 0.5, "deokure", "normal"))


_SIRE_COLS = ("父", "種牡馬", "sire", "father", "父名", "種牡馬名", "peds_0")


def f_sire_line(df: pd.DataFrame) -> pd.Series:
    """種牡馬の大系統（JRDB 系統コード表による分類）。血統の「洗い替え」因子。

    featured に種牡馬名列があれば大系統に束ねる（例: ヘイロー系/ミスタープロスペクター系）。
    未分類・列不在は na（中立）。生の種牡馬名より疎性が低く、系統×条件の回収率を学習しやすい。
    """
    col = next((c for c in _SIRE_COLS if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    from src.features._sire_line import daikeito
    vals = df[col].map(daikeito)
    return vals.where(vals != "不明", NA).to_numpy()


# --- 人・種牡馬の妙味ファクター（卍流「妙味度」＝均等払戻補正回収率の源） ---------
# 卍流評価手法の核: 騎手・厩舎・種牡馬は「過小評価/過剰人気」がはっきり出る（妙味度名鑑）。
# 個別名を生バケットにし、点数（±）は事後較正（均等払戻回収率）が決める。高カーディナリティは
# posterior の min_n gate と普遍性フィルタで疎バケットが自動的に落ちる。条件別クロス
# （jockey*race_type / sire*dist_change 等）は既存の "A*B" 機構で自動利用できる。

def _name_bucket(df: pd.DataFrame, *names) -> pd.Series:
    """候補列の最初に在るものを生名バケットで返す（空文字・nan は na）。"""
    c = _col(df, *names)
    if c is None:
        return pd.Series(NA, index=df.index)
    v = df[c].astype(str).str.strip()
    bad = v.eq("") | v.str.lower().eq("nan") | v.eq("0")
    return v.where(~bad, NA)


def f_jockey(df: pd.DataFrame) -> pd.Series:
    """騎手 個別妙味（過小評価騎手を加点）。名鑑の騎手ランキング＝妙味度に対応。"""
    return _name_bucket(df, ResultsCols.JOCKEY, "騎手", "jockey", "jockey_id").to_numpy()


def f_trainer(df: pd.DataFrame) -> pd.Series:
    """厩舎（調教師）個別妙味。名鑑「厩舎はリーディング上位ほど妙味度が高い」。"""
    return _name_bucket(df, ResultsCols.TRAINER, "調教師", "厩舎", "trainer", "trainer_id").to_numpy()


def f_sire(df: pd.DataFrame) -> pd.Series:
    """種牡馬 個別妙味（大系統 sire_line と別に生名で。キズナ/ドレフォン等の過小評価を捕捉）。"""
    col = next((c for c in _SIRE_COLS if c in df.columns), None)
    if col is None:
        return pd.Series(NA, index=df.index)
    v = df[col].astype(str).str.strip()
    bad = v.eq("") | v.str.lower().eq("nan")
    return v.where(~bad, NA).to_numpy()


def f_race_type(df: pd.DataFrame) -> pd.Series:
    """芝/ダート/障害（単独因子。人×芝ダ・種牡馬×芝ダのクロス土台）。"""
    rt = _race_type_series(df)
    if rt is None:
        return pd.Series(NA, index=df.index)
    v = rt.astype(str)
    return v.where(v.isin(["芝", "ダート", "障害"]), NA).to_numpy()


def _distance_m(df: pd.DataFrame):
    """距離[m] を返す。course_len が 100m 単位バケット（例16=1600m）の場合はスケール。"""
    c = _col(df, "course_len", "距離", "course_len_m", "distance")
    if c is None:
        return None
    d = _num(df[c])
    med = d.median()
    if pd.notna(med) and med < 100:  # バケット表現（//100 済み）を m へ戻す
        d = d * 100.0
    return d


def f_dist_band(df: pd.DataFrame) -> pd.Series:
    """距離帯（名鑑の条件別: 短距離≤1600 / 中距離1700-2200 / 長距離≥2300）。"""
    d = _distance_m(df)
    if d is None or d.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(d, [0, 1600, 2250, np.inf], labels=["sprint_mile", "mid", "long"])
    return out.astype(object).where(d.notna(), NA).to_numpy()


def f_place(df: pd.DataFrame) -> pd.Series:
    """競馬場（race_id 先頭 5-6 桁の場コード）。人×場・種牡馬×場の条件別妙味の土台。"""
    idx = pd.Series(df.index.astype(str), index=df.index)
    code = idx.str[4:6]
    valid = code.str.fullmatch(r"\d{2}").fillna(False)
    return code.where(valid, NA).to_numpy()


# JRA 場コード→回り（名鑑の条件別「左回り/右回り」。場より汎化が効く高妙味軸）。
# 左: 04新潟 05東京 07中京 / 右: 01札幌 02函館 03福島 06中山 08京都 09阪神 10小倉。
_TURN_LEFT = {"04", "05", "07"}
_TURN_RIGHT = {"01", "02", "03", "06", "08", "09", "10"}


def f_turn(df: pd.DataFrame) -> pd.Series:
    """左回り/右回り（JRA 場コードから）。人×回り・種牡馬×回りは名鑑の最上位妙味パターン。"""
    idx = pd.Series(df.index.astype(str), index=df.index)
    code = idx.str[4:6]

    def _m(c):
        return "left" if c in _TURN_LEFT else ("right" if c in _TURN_RIGHT else NA)

    return code.map(_m).to_numpy()


# --- 生月・遠征・馬種・2世代血統 ---------------------------------------------

def _birth_month(df: pd.DataFrame) -> pd.Series:
    c = _col(df, "生月", "誕生月", "birth_month")
    if c is not None:
        return _num(df[c])
    c2 = _col(df, "生年月日", "誕生日", "birth_date", "birthday")
    if c2 is not None:
        return pd.to_datetime(df[c2], errors="coerce").dt.month
    return pd.Series(np.nan, index=df.index)


def f_birth_month_2yo(df: pd.DataFrame) -> pd.Series:
    """2歳馬の生まれ月（早生まれ=成長が早く有利）。1-3=early / 4-5=mid / 6月以降=late。

    2歳以外・生月不明は na（成長差が効くのは2歳戦が中心のため2歳限定）。
    """
    a = _age_series(df)
    m = _birth_month(df)
    if m.isna().all():
        return pd.Series(NA, index=df.index)
    lab = pd.Series(NA, index=df.index, dtype=object)
    is2 = (a == 2)
    lab[is2 & m.between(1, 3)] = "early"
    lab[is2 & m.between(4, 5)] = "mid"
    lab[is2 & (m >= 6)] = "late"
    return lab.to_numpy()


def f_foreign_bred(df: pd.DataFrame) -> pd.Series:
    """馬種類: 外国産(マル外)か内国産か。馬区分/フラグ/産地から判定。"""
    c = _col(df, "外国産", "foreign", "マル外")
    if c is not None:
        v = _num(df[c])
        if v.notna().any():
            return np.where(v.isna(), NA, np.where(v >= 0.5, "foreign", "domestic"))
    c3 = _col(df, "馬区分", "馬種類")
    if c3 is not None:
        s = df[c3].astype(str)
        known = (s != "") & (s.str.lower() != "nan")
        fore = s.str.contains("外", na=False)  # (外)/(父外)/マル外 等
        return np.where(~known, NA, np.where(fore, "foreign", "domestic"))
    c2 = _col(df, "産地", "origin", "country")
    if c2 is not None:
        s = df[c2].astype(str)
        known = (s != "") & (s.str.lower() != "nan")
        fore = s.str.contains(
            "アメリカ|アイルランド|イギリス|英|米|フランス|仏|ドイツ|独|豪|オーストラリア|"
            "カナダ|ニュージーランド|USA|IRE|GB|FR|AUS", na=False)
        return np.where(~known, NA, np.where(fore, "foreign", "domestic"))
    return pd.Series(NA, index=df.index)


_KANTO_CODES = {"03", "04", "05", "06"}   # 福島 新潟 東京 中山
_KANSAI_CODES = {"07", "08", "09", "10"}  # 中京 京都 阪神 小倉


def _prev_region(df: pd.DataFrame):
    """前走の地区（kanto/kansai/overseas/na）。前走場所列が無ければ None。

    factor_store が履歴から算出した mf_prev_place（前走の場コード）を優先。
    """
    c = _col(df, "mf_prev_place", "前走場コード", "前走場所", "前走_開催", "前走場", "prev_place")
    if c is None:
        return None
    # astype(str) は object 内の np.nan を文字列化せず残すため fillna("") で空へ寄せる。
    s = df[c].fillna("").astype(str).str.strip()
    code = s.str.extract(r"(\d{2})")[0]
    # 既知の前走場所は既定 "other"（＝その地区への遠征ではない=no）。空/nan のみ na。
    reg = pd.Series("other", index=df.index, dtype=object)
    reg[s.eq("") | s.str.lower().eq("nan")] = NA
    reg[code.isin(_KANTO_CODES)] = "kanto"
    reg[code.isin(_KANSAI_CODES)] = "kansai"
    reg[s.str.contains("東京|中山|福島|新潟", na=False)] = "kanto"
    reg[s.str.contains("中京|京都|阪神|小倉", na=False)] = "kansai"
    reg[s.str.contains("海外|香港|ドバイ|フランス|イギリス|米|豪|UAE|サウジ", na=False)] = "overseas"
    return reg


def _prev_flag(df: pd.DataFrame, region: str, *flag_cols):
    """前走が指定地区への遠征か（yes/no/na）。明示フラグ列を優先、無ければ前走地区から。"""
    c = _col(df, *flag_cols)
    if c is not None:
        v = _num(df[c])
        if v.notna().any():
            return np.where(v.isna(), NA, np.where(v >= 0.5, "yes", "no"))
    reg = _prev_region(df)
    if reg is None:
        return None
    return np.where(reg.to_numpy() == NA, NA, np.where(reg.to_numpy() == region, "yes", "no"))


def f_prev_kanto(df: pd.DataFrame) -> pd.Series:
    """前回 関東遠征の有無（yes/no）。前走関東遠征フラグ or 前走地区から。無ければ na。"""
    r = _prev_flag(df, "kanto", "前走関東遠征", "prev_kanto", "前走_関東遠征")
    return r if r is not None else pd.Series(NA, index=df.index).to_numpy()


def f_prev_kansai(df: pd.DataFrame) -> pd.Series:
    """前回 関西遠征の有無（yes/no）。無ければ na。"""
    r = _prev_flag(df, "kansai", "前走関西遠征", "prev_kansai", "前走_関西遠征")
    return r if r is not None else pd.Series(NA, index=df.index).to_numpy()


def f_prev_overseas(df: pd.DataFrame) -> pd.Series:
    """前回 海外遠征の有無（yes/no）。無ければ na。"""
    r = _prev_flag(df, "overseas", "前走海外遠征", "prev_overseas", "前走海外", "前走_海外遠征")
    return r if r is not None else pd.Series(NA, index=df.index).to_numpy()


def f_pedigree_2gen(df: pd.DataFrame) -> pd.Series:
    """2世代血統: 父系×母父系（脚質/距離適性の代理）。父・母父の大系統を結合。

    父(peds_0)と母父(peds_32='母父')の JRDB 大系統を "父系|母父系" で結合。どちらか
    不明・列不在は na。生の血統名より疎性が低く、系統×条件の回収率を学習しやすい。
    """
    sire_c = next((c for c in _SIRE_COLS if c in df.columns), None)
    bms_c = _col(df, "母父", "母の父", "母父名", "broodmare_sire", "bms", "damsire", "peds_32")
    if sire_c is None or bms_c is None:
        return pd.Series(NA, index=df.index)
    from src.features._sire_line import daikeito
    ps = df[sire_c].map(daikeito).astype(str)
    ms = df[bms_c].map(daikeito).astype(str)
    combo = ps + "|" + ms
    bad = (ps == "不明") | (ms == "不明")
    return combo.where(~bad, NA).to_numpy()


# --- 馬×時刻の履歴依拠ファクター（近走 / 通算） -----------------------------
# これらは「馬の過去走」を要する＝1行だけでは計算できないため、事前に
# _manji_factor_store が forward-only（当該走を含めない）で数値列 mf_* を付ける。
# 各関数はその数値列が有れば帯に切り、無ければ na（中立）を返す（既存 f_career と同作法）。

def _bucket_num(df: pd.DataFrame, col: str, bins: list, labels: list) -> pd.Series:
    """数値列 col を bins/labels で帯化。列不在・欠損は na。"""
    if col not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df[col])
    if v.isna().all():
        return pd.Series(NA, index=df.index)
    out = pd.cut(v, bins, labels=labels)
    return out.astype(object).where(v.notna(), NA)


def f_recent3_form(df: pd.DataFrame) -> pd.Series:
    """直近3走の平均着順帯（good/mid/poor）。前走までの実績＝過小/過大評価の妙味源。"""
    return _bucket_num(df, "mf_recent3_avg_rank", [0, 3.0, 6.0, np.inf],
                       ["good", "mid", "poor"]).to_numpy()


def f_recent5_form(df: pd.DataFrame) -> pd.Series:
    """直近5走の平均着順帯（good/mid/poor）。3走より緩やかな近況。"""
    return _bucket_num(df, "mf_recent5_avg_rank", [0, 3.0, 6.0, np.inf],
                       ["good", "mid", "poor"]).to_numpy()


def f_recent3_recovery(df: pd.DataFrame) -> pd.Series:
    """直近3走の単勝回収帯（under/fair/over）。市場評価に対する近走の割安/割高。"""
    return _bucket_num(df, "mf_recent3_recovery", [-1e-9, 0.7, 1.3, np.inf],
                       ["under", "fair", "over"]).to_numpy()


def f_recent5_recovery(df: pd.DataFrame) -> pd.Series:
    """直近5走の単勝回収帯（under/fair/over）。"""
    return _bucket_num(df, "mf_recent5_recovery", [-1e-9, 0.7, 1.3, np.inf],
                       ["under", "fair", "over"]).to_numpy()


def f_career_form(df: pd.DataFrame) -> pd.Series:
    """通算（キャリア全体・前走まで）の勝率帯（cold/mid/hot）。全過去依拠ファクター。"""
    return _bucket_num(df, "mf_career_winrate", [-1e-9, 0.08, 0.18, np.inf],
                       ["cold", "mid", "hot"]).to_numpy()


def f_career_recovery(df: pd.DataFrame) -> pd.Series:
    """通算（前走まで）の単勝回収帯（under/fair/over）。全過去依拠ファクター。"""
    return _bucket_num(df, "mf_career_recovery", [-1e-9, 0.7, 1.3, np.inf],
                       ["under", "fair", "over"]).to_numpy()


# --- 近走詳細（出遅れ/不利/着差/逆トラック・逆馬場）＝factor_store が付ける mf_* を帯化 -----

def f_recent_deokure(df: pd.DataFrame) -> pd.Series:
    """近走(過去5走)で出遅れたことがある馬（yes=過小評価の妙味／加点）。列不在は na。"""
    if "mf_recent_deokure" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["mf_recent_deokure"])
    return np.where(v.isna(), NA, np.where(v >= 0.5, "yes", "no"))


def f_recent_trouble(df: pd.DataFrame) -> pd.Series:
    """近走(過去5走)で道中不利を受けたレースがある馬（yes=妙味／加点）。列不在は na。"""
    if "mf_recent_trouble" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["mf_recent_trouble"])
    return np.where(v.isna(), NA, np.where(v >= 0.5, "yes", "no"))


def f_recent_close(df: pd.DataFrame) -> pd.Series:
    """近走の勝ち馬からの最小着差[秒]。≤0.2=within02 / ≤0.5=within05 / それ超=over。列不在は na。"""
    if "mf_recent_close" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["mf_recent_close"])
    out = pd.cut(v, [-1.0, 0.2, 0.5, np.inf], labels=["within02", "within05", "over"])
    return out.astype(object).where(v.notna(), NA).to_numpy()


def f_offsurface_form(df: pd.DataFrame) -> pd.Series:
    """今走と逆トラックの近走成績（今走芝×近走ダ好走=過大評価で減点、凡走=妙味で加点。ダ↔芝も同様）。

    offsurf_good=逆トラックで好走(最高着≤3)／offsurf_poor=凡走(≥6)。判定不能は na。
    """
    rt = _race_type_series(df)
    if rt is None:
        return pd.Series(NA, index=df.index)
    today = rt.astype(str).to_numpy()
    dv = _num(df["mf_recent_dirt_bestrank"]).to_numpy() if "mf_recent_dirt_bestrank" in df.columns \
        else np.full(len(df), np.nan)
    tv = _num(df["mf_recent_turf_bestrank"]).to_numpy() if "mf_recent_turf_bestrank" in df.columns \
        else np.full(len(df), np.nan)
    if np.isnan(dv).all() and np.isnan(tv).all():
        return pd.Series(NA, index=df.index)
    opp = np.where(today == "芝", dv, np.where(today == "ダート", tv, np.nan))
    return np.where(np.isnan(opp), NA,
                    np.where(opp <= 3, "offsurf_good", np.where(opp >= 6, "offsurf_poor", NA)))


def f_offground_form(df: pd.DataFrame) -> pd.Series:
    """今走良馬場のとき、近走の道悪(重/不良)成績（好走=過大評価で減点、凡走=妙味で加点）。

    offgrnd_good=道悪で好走(≤3)／offgrnd_poor=凡走(≥6)。今走が良でない/判定不能は na。
    """
    oh = _onehot_cat(df, "ground_state1__")
    if oh is None and "ground_state" in df.columns:
        oh = df["ground_state"].astype(str)
    if oh is None or "mf_recent_heavy_bestrank" not in df.columns:
        return pd.Series(NA, index=df.index)
    today = pd.Series(oh, index=df.index).astype(str).to_numpy()
    hv = _num(df["mf_recent_heavy_bestrank"]).to_numpy()
    good_today = (today == "良")
    return np.where(~good_today | np.isnan(hv), NA,
                    np.where(hv <= 3, "offgrnd_good", np.where(hv >= 6, "offgrnd_poor", NA)))


def f_head2head(df: pd.DataFrame) -> pd.Series:
    """同一レース対戦履歴（factor_store の with_h2h=True で mf_h2h_score が付く）。

    net>0=今走の相手に過去負けていた側（過小評価＝妙味, underdog）／net<0=過去勝っていた側
    （割引, favorite）／0=互角。過去対戦なし・列不在は na。
    """
    if "mf_h2h_score" not in df.columns:
        return pd.Series(NA, index=df.index)
    v = _num(df["mf_h2h_score"])
    return np.where(v.isna(), NA,
                    np.where(v > 0, "underdog", np.where(v < 0, "favorite", "even")))


# 因子レジストリ（名前 → 抽出関数）。Model 2 はこの名前で点数を較正する。
FACTORS: dict[str, Callable[[pd.DataFrame], Any]] = {
    "umaban_parity": f_umaban_parity,
    "sex": f_sex,
    "age": f_age,
    "season": f_season,
    "season_sex": f_season_sex,
    "weight_rank": f_weight_rank,
    "rotation": f_rotation,
    "dist_change": f_dist_change,
    "kinryo_rank": f_kinryo_rank,
    "track_sex": f_track_sex,
    "career": f_career,
    "sire_line": f_sire_line,
    "popularity": f_popularity,
    "waku": f_waku,
    "body_weight": f_body_weight,
    "weight_diff": f_weight_diff,
    "kinryo_per_weight": f_kinryo_per_weight,
    "age_rotation": f_age_rotation,
    "dist_age": f_dist_age,
    "prev_finish": f_prev_finish,
    "paddock": f_paddock,
    "ground": f_ground,
    "race_class": f_race_class,
    "leg_type": f_leg_type,
    "pace_pressure": f_pace_pressure,
    "kijun_gap": f_kijun_gap,
    "prev_trouble": f_prev_trouble,
    "prev_deokure": f_prev_deokure,
    # 人・種牡馬の妙味（生名バケット。条件別クロス jockey*race_type 等の土台）
    "jockey": f_jockey,
    "trainer": f_trainer,
    "sire": f_sire,
    "race_type": f_race_type,
    "dist_band": f_dist_band,
    "place": f_place,
    "turn": f_turn,
    # 生月(2歳)・遠征・馬種・2世代血統
    "birth_month_2yo": f_birth_month_2yo,
    "foreign_bred": f_foreign_bred,
    "prev_kanto": f_prev_kanto,
    "prev_kansai": f_prev_kansai,
    "prev_overseas": f_prev_overseas,
    "pedigree_2gen": f_pedigree_2gen,
    # 馬×時刻の履歴依拠（_manji_factor_store が付ける mf_* 列を帯化。列不在なら na）
    "recent3_form": f_recent3_form,
    "recent5_form": f_recent5_form,
    "recent3_recovery": f_recent3_recovery,
    "recent5_recovery": f_recent5_recovery,
    "career_form": f_career_form,
    "career_recovery": f_career_recovery,
    # 近走詳細（factor_store の mf_* を帯化。元列が無ければ na）
    "recent_deokure": f_recent_deokure,
    "recent_trouble": f_recent_trouble,
    "recent_close": f_recent_close,
    "offsurface_form": f_offsurface_form,
    "offground_form": f_offground_form,
    "head2head": f_head2head,  # 同一レース対戦履歴（factor_store with_h2h=True で発火）
}


CROSS_SEP = "*"  # クロス因子名の区切り: "season_sex*ground" など


def factor_series(df: pd.DataFrame, name: str) -> pd.Series:
    """単独因子 or クロス因子（"A*B"）のバケット Series を返す。

    クロスは各構成因子のバケットを結合（どちらか na なら na）。任意次数（A*B*C）に対応。
    これにより加算モデルでは表せない相互作用を、1つの合成バケット因子として扱える。
    """
    if CROSS_SEP not in name:
        return pd.Series(FACTORS[name](df), index=df.index).astype(object).fillna(NA)
    parts = name.split(CROSS_SEP)
    combo = None
    na_mask = None
    for p in parts:
        s = pd.Series(FACTORS[p](df), index=df.index).astype(object).fillna(NA)
        m = (s == NA)
        combo = s.astype(str) if combo is None else combo + "|" + s.astype(str)
        na_mask = m if na_mask is None else (na_mask | m)
    assert combo is not None and na_mask is not None  # parts は必ず1要素以上（ループが回る）
    return combo.where(~na_mask, NA)


def buckets(df: pd.DataFrame, names: list[str] | None = None) -> pd.DataFrame:
    """指定因子（既定=全単独因子）のバケットラベル列を持つ DataFrame。クロス名 "A*B" も可。"""
    names = names or list(FACTORS)
    out = {name: factor_series(df, name) for name in names}
    return pd.DataFrame(out, index=df.index)
