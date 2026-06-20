import re

import pandas as pd

from src.constants._horse_results_cols import HorseResultsCols as Cols
from src.constants._master import Master
from src.constants._units import COURSE_LEN_BUCKET_METERS
from src.preprocessing._abstract_data_processor import AbstractDataProcessor


def parse_corner(x, n):
    """通過順文字列（例 "3-3-2-1"）から n 番目のコーナー位置を取り出す。

    n=1 は最初、n=4 は最終コーナー。文字列以外（NaN 等）や数字を含まない値
    （空文字・"-"・DB 復元時の空通過順）は欠損（pd.NA）として扱い、
    ``int(re.findall(...)[0])`` が IndexError で落ちるのを防ぐ。
    """
    if not isinstance(x, str):
        return x
    nums = re.findall(r"\d+", x)
    if not nums:
        return pd.NA
    if n == 4:
        return int(nums[-1])
    if n == 1:
        return int(nums[0])
    return pd.NA


class HorseResultsProcessor(AbstractDataProcessor):
    def __init__(self, filepath):
        """
        初期処理
        """
        super().__init__(filepath)

    def _preprocess(self):
        """
        前処理
        """
        df = self.raw_data

        # 着順に数字以外の文字列が含まれているものは、欠損値（NaN）に置き換える
        # サイト上のテーブルに存在する列名は、HorseResultsColsクラスで定数化している。
        df[Cols.RANK] = pd.to_numeric(df[Cols.RANK], errors="coerce")
        # 着順が欠損値（NaN）となったものを取り除く
        df.dropna(subset=[Cols.RANK], inplace=True)
        df[Cols.RANK] = df[Cols.RANK].astype(int)

        # 日付をdatetime型に設定
        df["date"] = pd.to_datetime(df[Cols.DATE])

        # 賞金のNaNを0で埋める
        # df[Cols.PRIZE].fillna(0, inplace=True)
        df[Cols.PRIZE] = df[Cols.PRIZE].fillna(0)

        # 1着の着差を0にする（xが0より小さい場合は、0、xが0以上の場合、xを返す）
        df[Cols.RANK_DIFF] = pd.to_numeric(df[Cols.RANK_DIFF], errors="coerce").fillna(0)
        df[Cols.RANK_DIFF] = df[Cols.RANK_DIFF].map(lambda x: 0 if x < 0 else x)

        # 上がり3F（終盤の脚力）を数値化（多窓集計の対象にするため）。"34.5" 等→float。
        df[Cols.NOBORI] = pd.to_numeric(df[Cols.NOBORI], errors="coerce")

        # レース展開データ（n=1: 最初のコーナー位置, n=4: 最終コーナー位置）
        df["first_corner"] = df[Cols.CORNER].map(lambda x: parse_corner(x, 1))
        df["final_corner"] = df[Cols.CORNER].map(lambda x: parse_corner(x, 4))

        df["final_to_rank"] = df["final_corner"] - df[Cols.RANK]
        df["first_to_rank"] = df["first_corner"] - df[Cols.RANK]
        df["first_to_final"] = df["first_corner"] - df["final_corner"]

        # 開催場所（数字以外の文字列を抽出）中央開催・地方開催・海外開催以外をその他（'99'）とする
        df[Cols.PLACE] = df[Cols.PLACE].str.extract(r"(\D+)")[0].map(Master.PLACE_DICT).fillna("99")

        # race_type（数字以外の文字列を抽出）
        df["race_type"] = df[Cols.RACE_TYPE_COURSE_LEN].str.extract(r"(\D+)")[0].map(Master.RACE_TYPE_DICT)
        # 距離は10の位を切り捨てる（数字の文字列を抽出）
        df["course_len"] = (
            df[Cols.RACE_TYPE_COURSE_LEN].str.extract(r"(\d+)").astype(float) // COURSE_LEN_BUCKET_METERS
        )

        # タイムの値を秒単位に変換
        # 準備
        baseformat = "%M:%S.%f"
        basetime = pd.to_datetime("00:00.0", format=baseformat)

        def to_datetime(x):
            return pd.to_datetime(df[Cols.TIME], format=x, errors="coerce")

        # 秒単位へのフォーマット変換処理
        datetime_s = to_datetime(baseformat)
        # 「x:xx.x」フォーマット以外、許容するフォーマットを定義
        formats_additional = ["%M.%S.%f", "%M:%S:%f"]
        for format_ in formats_additional:
            # 秒単位へのフォーマット変換処理
            datetime_s = datetime_s.fillna(to_datetime(format_))
        # フォーマット例外は欠損値になる
        df["time_seconds"] = (datetime_s - basetime).dt.total_seconds()

        # §2l スピード指数（タイム偏差）: (競馬場×種別×距離×馬場) ごとの基準タイムから
        # 何σ速かったか。faster=正。生タイムは馬場/距離差で比較不能なため標準化して相対化する。
        # 基準は母集団統計（着順という結果は使わない）。当該走のタイムのみで算出されリーク無し。
        speed_keys = [Cols.PLACE, "race_type", "course_len", Cols.GROUND_STATE]
        grp = df.groupby(speed_keys)["time_seconds"]
        base_mean = grp.transform("mean")
        base_std = grp.transform("std")
        df["speed_figure"] = (base_mean - df["time_seconds"]) / (base_std + 1e-8)

        # インデックス名を与える
        # df.index.name = "horse_id"
        df.set_index("horse_id", inplace=True)

        # カラム抽出
        df = self._select_columns(df)

        return df

    def _select_columns(self, raw):
        """
        カラム抽出
        """
        df = raw.copy()[
            [
                Cols.DATE,  # 日付
                Cols.PLACE,  # 開催
                Cols.WEATHER,  # 天気
                Cols.R,  # R
                Cols.RACE_NAME,  # レース名
                # 映像
                Cols.N_HORSES,  # 頭数
                Cols.WAKUBAN,  # 枠番
                Cols.UMABAN,  # 馬番
                Cols.TANSHO_ODDS,  # オッズ
                Cols.POPULARITY,  # 人気
                Cols.RANK,  # 着順
                Cols.JOCKEY,  # 騎手
                Cols.KINRYO,  # 斤量
                # Cols.RACE_TYPE_COURSE_LEN, # 距離
                Cols.GROUND_STATE,  # 馬場
                # 馬場指数
                # Cols.TIME, # タイム
                Cols.RANK_DIFF,  # 着差
                # ﾀｲﾑ指数
                Cols.CORNER,  # 通過
                Cols.PACE,  # ペース
                Cols.NOBORI,  # 上り
                Cols.WEIGHT_AND_DIFF,  # 馬体重
                # 厩舎ｺﾒﾝﾄ
                # 備考
                # 勝ち馬(2着馬)
                Cols.PRIZE,  # 賞金
                "date",
                "first_corner",
                "final_corner",
                "final_to_rank",
                "first_to_rank",
                "first_to_final",
                "race_type",
                "course_len",
                "time_seconds",
                "speed_figure",
            ]
        ]

        return df
