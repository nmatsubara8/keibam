from abc import ABCMeta
from abc import abstractmethod
from typing import Callable

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols
from src.constants._results_cols import TARGET_LEAK_COLS
from src.constants._results_cols import ResultsCols

# const
_SCORE = "score"
# 期待値計算で用いる列名（ExpectedValueBetPolicy / OddsProvider と整合）
PROB = "prob"
CURRENT_ODDS = "current_odds"

# predict_proba に渡す前に除外する非特徴量列（目的変数・日付・オッズ・ID）。
# 学習時の _DROP_FOR_TRAIN（rank/date/horse_id/単勝/着順/通過）と列数を揃える必要がある。
# horse_id を除外し損ねると特徴量が 1 列多くなり LightGBM が
# "number of features ... is not the same as ... training data" で失敗する。
# CORNER('通過') は post-race のコーナー通過順（リーク列）。_DROP_FOR_TRAIN と対で除外する。
# rank_win 等の目的変数リーク列は TARGET_LEAK_COLS（_results_cols）を単一定義元として参照。
_DROP_FOR_PREDICT = [
    "horse_id", ResultsCols.TANSHO_ODDS, "rank", *TARGET_LEAK_COLS, "date",
    ResultsCols.RANK, HorseResultsCols.CORNER,
]

# score_policy が参照する非特徴量列（枠番・馬番 + 除外列）。モデルの特徴量ではないが
# 推論時に X へ残す必要がある列の単一の定義元（KeibaAI.calc_score が参照する）。
META_COLS = [ResultsCols.UMABAN, ResultsCols.WAKUBAN, *_DROP_FOR_PREDICT]


def _coerce_for_predict(frame: pd.DataFrame) -> pd.DataFrame:
    """特徴量フレームを LightGBM が扱える数値に正規化する。

    pandas の nullable 拡張dtype（Int64/Float64/boolean）や object 列に pd.NA(NAType) が
    あると、LightGBM/numpy が ``float() argument must be ... not 'NAType'`` で落ちる。
    該当列だけ ``to_numeric``→float64（pd.NA→np.nan）に変換し、欠損は LightGBM が
    ネイティブに扱える np.nan へ統一する。通常の float64/int64 列は触らない。
    """
    bad_cols = [
        c for c in frame.columns
        if pd.api.types.is_extension_array_dtype(frame[c].dtype) or frame[c].dtype == object
    ]
    if not bad_cols:
        return frame
    out = frame.copy()
    for c in bad_cols:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    return out


# common funcs
def _calc(model, X: pd.DataFrame) -> pd.DataFrame:
    score_table = X[ResultsCols.UMABAN].to_frame().copy()
    score_table[ResultsCols.WAKUBAN] = X[ResultsCols.WAKUBAN]
    # race_idごとにWAKUBANの組み合わせをカウント
    # race_idごとにUMABANとWAKUBANの個数を取得
    race_key = "race_id" if "race_id" in X.columns else X.index
    umaban_count_per_race = X.groupby(race_key)[ResultsCols.UMABAN].nunique()
    wakuban_count_per_race = X.groupby(race_key)[ResultsCols.WAKUBAN].nunique()

    # UMABANの個数がWAKUBANの個数よりも多い場合にwakuban_flagを設定
    wakuban_flag = (umaban_count_per_race > wakuban_count_per_race).astype(int)
    wakuban_flag.name = "wakuban_flag"
    X_pred = _coerce_for_predict(X.drop(_DROP_FOR_PREDICT, axis=1, errors="ignore"))
    try:
        score = model.predict_proba(X_pred)[:, 1]
    except Exception:
        # 列数不一致の場合のみフォールバック。モデルが Column_N の generic 名で
        # 保存されていると名前マッチが効かないため、最初に X_pred の先頭 N 列で再試行。
        feat_names = getattr(model, "feature_name_", None) or getattr(model, "feature_names_in_", None)
        if feat_names is None and hasattr(model, "booster_"):
            feat_names = model.booster_.feature_name()
        n = len(list(feat_names)) if feat_names is not None else X_pred.shape[1]
        common = [c for c in (feat_names or []) if c in X_pred.columns]
        if len(common) == n:
            X_pred = X_pred[common]
        elif X_pred.shape[1] >= n:
            X_pred = X_pred.iloc[:, :n]
        else:
            X_pred = X_pred.reindex(columns=list(feat_names or []), fill_value=0)
        score = model.predict_proba(X_pred)[:, 1]
    score_table[_SCORE] = score
    # race_idに対応するwakuban_flagを結合
    if "race_id" in score_table.columns:
        score_table = pd.merge(score_table, wakuban_flag, left_on="race_id", right_index=True)
    else:
        score_table = score_table.join(wakuban_flag.rename("wakuban_flag"), how="left")
        score_table["wakuban_flag"] = score_table["wakuban_flag"].fillna(0).astype(int)

    return score_table


def _apply_scaler(score: pd.Series, scaler: Callable[[pd.Series], pd.Series]) -> pd.Series:
    """race_id(index level0)ごとに scaler を適用。transform で**入力の並び順を保持**する。

    apply(group_keys=False) は group キー順に並べ替えるため、重複ラベル index(=同一レースの
    複数馬)へ列代入すると reindex 不能でクラッシュする。transform は元の位置に整列した Series を
    返すので、呼び出し側は .to_numpy() で位置代入でき、重複 index でも安全。
    """
    return score.groupby(level=0, group_keys=False).transform(scaler)


# scalers
def _scaler_standard(x):
    return (x - x.mean()) / x.std(ddof=0)


def _scaler_relative_proba(x):
    return x / x.sum()


# policies
class AbstractScorePolicy(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError


class BasicScorePolicy(AbstractScorePolicy):
    """
    LightGBMの出力をそのままscoreとして計算。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        return _calc(model, X)


class StdScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価する。「レース内偏差値」のようなもの。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化（重複 index 安全のため位置代入）
        score_table[_SCORE] = _apply_scaler(score_table[_SCORE], _scaler_standard).to_numpy()
        return score_table


class MinMaxScorePolicy(AbstractScorePolicy):
    """
    レース内で標準化して、相対評価した後、全体を0~1にスケーリング。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを標準化
        score = _apply_scaler(score_table[_SCORE], _scaler_standard)
        # データ全体で0~1にスケーリング（重複 index 安全のため位置代入）
        min_ = score.min()
        score_table[_SCORE] = ((score - min_) / (score.max() - min_)).to_numpy()
        return score_table


class RelativeProbaScorePolicy(AbstractScorePolicy):
    """
    レース内での相対確率。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        score_table = _calc(model, X)
        # レース内でスコアを相対確率化（重複 index 安全のため位置代入）
        score_table[_SCORE] = _apply_scaler(score_table[_SCORE], _scaler_relative_proba).to_numpy()
        return score_table


class ExpectedValueScorePolicy(AbstractScorePolicy):
    """期待値ベース馬券選定（ExpectedValueBetPolicy）向けのテーブルを作る。

    較正済みモデルの勝率と単勝オッズを保持した DataFrame を返す。
    列: [馬番, prob(較正勝率), current_odds(単勝オッズ)]、index は race_id。
    既存のスコア系ポリシーと異なり「スコア」ではなく確率とオッズを供給する。
    """

    @staticmethod
    def calc(model, X: pd.DataFrame) -> pd.DataFrame:
        prob = model.predict_proba(
            _coerce_for_predict(X.drop(_DROP_FOR_PREDICT, axis=1, errors="ignore"))
        )[:, 1]
        table = X[[ResultsCols.UMABAN]].copy()
        table[PROB] = prob
        table[CURRENT_ODDS] = X[ResultsCols.TANSHO_ODDS].astype(float)
        return table
