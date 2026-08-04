import dataclasses

@dataclasses.dataclass(frozen=True)
class ResultsCols:
    """
    サイト上のテーブル列名を、定数として持っておく。
    """
    RANK: str = '着順'
    WAKUBAN: str = '枠番'
    UMABAN: str = '馬番'
    HORSE_NAME: str = '馬名'
    SEX_AGE: str = '性齢'
    KINRYO: str = '斤量'
    JOCKEY: str = '騎手'
    TIME: str = 'タイム'
    RANK_DIFF: str = '着差'
    # 通過
    # 上がり
    TANSHO_ODDS: str = '単勝'
    POPULARITY: str = '人気'
    WEIGHT_AND_DIFF: str = '馬体重'
    TRAINER: str = '調教師'


# 目的変数リーク列（着順から作られ「結果そのもの」を符号化する列。特徴量に紛れると漏洩）。
# 学習・推論の DROP リスト（_data_splitter._DROP_FOR_TRAIN/_DROP_FOR_TEST /
# _score_policy._DROP_FOR_PREDICT）が必ず除外する単一の定義元。新たな漏洩列が判明したら
# ここに追加すれば全箇所へ反映される。
#   - rank_win: 着順==1 の勝ち馬フラグ（二値目的変数）。特徴量に混入すると held-out 単独
#     AUC=1.0 になる完全リーク。現状すでに各 DROP リストで除外済みだが、定義を一元化して
#     将来のリーク列追加を一箇所で完結させる。
TARGET_LEAK_COLS = ["rank_win"]
