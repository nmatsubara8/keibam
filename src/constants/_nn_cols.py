"""NN (base②) 入力に関する列名定数。

Entity Embedding 対象 ID 列と、NN DataFrame 生成時に除外するメタ列を定義する。
変更時の影響範囲をこのファイルに限定する。
"""

# Entity Embedding 対象列（StandardScaler 対象外; Embedding 層が処理）。
# peds_0 = 父 (sire), peds_2 = 母父 (BMS / maternal grandfather)
NN_ENTITY_COLS: list = [
    "horse_id",
    "jockey_id",
    "trainer_id",
    "owner_id",
    "breeder_id",
    "peds_0",
    "peds_2",
]

# featured_data から NN 用 DataFrame を選択する際に除外するメタ列
# rank / date は学習ターゲット・分割キー, 単勝はリーク源
NN_DROP_COLS: list = ["rank", "date", "単勝"]
